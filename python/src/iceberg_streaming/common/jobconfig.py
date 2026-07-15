"""Shared configuration and :class:`~pyspark.sql.SparkSession` factory for every PySpark example.

Python counterpart of ``com.aws.emr.common.JobConfig``. All examples take order-independent
``key=value`` arguments; every key is optional with a sensible default. Besides the run environment
(local, local on S3/S3 Tables, EMR), the arguments parameterise the **Iceberg table layout**
(copy-on-write vs merge-on-read, format-version 2 vs 3, parquet/ORC/Avro files, object-storage
layout), the **source payload format** (protobuf, Avro or JSON) and the **write behaviour** (dedup
strategy, compaction strategy, trigger). One job module therefore covers what used to be many
near-identical modules.

Arguments::

    runtime=local|emr             where Spark runs (default: local)
    catalog=local|glue|s3tables   Iceberg catalog / storage (default: local)
    warehouse=<path|s3 uri|arn>   catalog warehouse (default 'warehouse' for local; s3://... for
                                  glue; the table bucket ARN for s3tables)
    checkpoint=<path|s3 uri>      streaming checkpoint base dir (default: tmp/); every job derives
                                  a per-query path under it
    bootstrap=<host:port,...>     Kafka bootstrap servers (default: localhost:9092)

    -- table layout knobs (consumed by create_table_ddl) --
    table=<name>                  target table name (job-specific default)
    mode=cow|mor                  copy-on-write or merge-on-read row-level operations
    fv=2|3                        Iceberg format-version (default 3; v3 => deletion vectors)
    fileformat=parquet|orc|avro   data/delete file format (default parquet)
    objectstorage=true|false      Iceberg object-storage layout for S3 (default false)
    fanout=true|false             Spark fanout writers (default true)
    manifestmerge=true|false      Iceberg manifest merge-on-commit (default true)

    -- streaming behaviour knobs --
    source=proto|avro|json        Kafka payload format for the telemetry jobs (default proto)
    topic=<name>                  Kafka topic (default: telemetry-<source>; CDC topics are fixed)
    dedup=none|batch|merge|watermark  dedup strategy (job-specific default; legacy true/false OK)
    compaction=none|inline|scheduled  compaction strategy (default none; legacy true/false OK)
    trigger=<seconds>|availablenow    micro-batch trigger (job-specific default); availablenow
                                      drains the topic and stops - a streaming job run as a batch
    watermark=<duration>          event-time watermark delay for dedup=watermark ('120 seconds')
    startingOffsets=latest|earliest|{json}  Kafka start offsets on a fresh checkpoint
    maxOffsetsPerTrigger=<n>      cap records per micro-batch (default unset -> drain all)
    failOnDataLoss=true|false     Kafka failOnDataLoss (default: Kafka default true)

    -- misc --
    descriptor=<path>             protobuf descriptor (default ../src/main/protobuf/VehicleTelemetry.desc)
    avro=<path>                   Avro .avsc schema (default ../src/main/avro/VehicleTelemetry.avsc)
    shuffle=<n>                   spark.sql.shuffle.partitions initial value; AQE coalesces
                                  (default: 200 local / 800 cloud)
    region=<aws-region>           Glue Schema Registry region (default: eu-west-1)

Tables default to Iceberg format-version 3, switchable per run with ``fv=2`` so v2 positional
deletes and v3 deletion vectors can be A/B tested with the same module.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Sequence

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("iceberg_streaming.jobconfig")

#: Default Iceberg table format version used by every example.
FORMAT_VERSION = "3"

#: Database / namespace used by every example.
DATABASE = "bigdata"

# Spark version and Scala binary used to resolve the connector / runtime packages for local runs.
_SPARK_VERSION = "4.0.2"
_SCALA_BINARY = "2.13"
_ICEBERG_VERSION = "1.11.0"
_S3TABLES_VERSION = "0.1.8"


class Runtime(str, Enum):
    LOCAL = "local"
    EMR = "emr"


class Catalog(str, Enum):
    LOCAL = "local"
    GLUE = "glue"
    S3TABLES = "s3tables"


class Mode(str, Enum):
    """Row-level operation mode of the target table."""

    COW = "cow"
    MOR = "mor"


class FileFormat(str, Enum):
    """Iceberg data/delete file format of the target table."""

    PARQUET = "parquet"
    ORC = "orc"
    AVRO = "avro"


class Source(str, Enum):
    """Payload format of the Kafka topic consumed by the telemetry jobs."""

    PROTO = "proto"
    AVRO = "avro"
    JSON = "json"


class Dedup(str, Enum):
    """Deduplication strategy.

    * ``NONE`` - append everything as-is.
    * ``BATCH`` - drop exact duplicates of the event identity inside each micro-batch (one cheap
      shuffle, no target scan). Duplicates split across micro-batches survive.
    * ``MERGE`` - ``BATCH`` plus a MERGE INTO against the recent target partitions, so
      re-deliveries arriving in a later micro-batch are suppressed too (bounded replay
      suppression, not a global upsert).
    * ``WATERMARK`` - event-time watermark + ``dropDuplicatesWithinWatermark`` (native-writer job
      only; events older than the watermark are dropped entirely - see the job docs).
    """

    NONE = "none"
    BATCH = "batch"
    MERGE = "merge"
    WATERMARK = "watermark"


class Compaction(str, Enum):
    """Compaction strategy: none, inline every N batches, or a scheduled background thread."""

    NONE = "none"
    INLINE = "inline"
    SCHEDULED = "scheduled"


@dataclass(frozen=True)
class JobConfig:
    """Immutable, parsed job configuration and Spark session factory."""

    runtime: Runtime
    catalog: Catalog
    warehouse: str
    checkpoint_location: str
    bootstrap_servers: str
    shuffle_partitions: int
    # All parsed key=value arguments; example-specific options are read through the typed accessors
    # instead of re-parsing argv.
    raw_args: dict[str, str]

    # ------------------------------------------------------------------ parsing

    @staticmethod
    def from_args(args: Sequence[str] | None) -> "JobConfig":
        """Parse ``key=value`` arguments (typically ``sys.argv[1:]``) into a :class:`JobConfig`."""
        kv: dict[str, str] = {}
        for arg in args or []:
            if not arg or "=" not in arg:
                if arg:
                    log.warning("Ignoring argument '%s' - expected key=value form.\n%s", arg, usage())
                continue
            key, _, value = arg.partition("=")
            kv[key.strip().lower()] = value.strip()

        runtime = Runtime.EMR if kv.get("runtime", "local").lower() == "emr" else Runtime.LOCAL
        catalog = _parse_catalog(kv.get("catalog", "local"))

        warehouse = kv.get("warehouse", "warehouse" if catalog is Catalog.LOCAL else None)
        if catalog is not Catalog.LOCAL and not warehouse:
            raise ValueError(
                f"catalog={catalog.value} requires a warehouse= argument (an s3:// URI for glue or a "
                f"table bucket ARN for s3tables).\n{usage()}"
            )

        default_shuffle = 200 if runtime is Runtime.LOCAL else 800

        cfg = JobConfig(
            runtime=runtime,
            catalog=catalog,
            warehouse=warehouse,
            checkpoint_location=kv.get("checkpoint", "tmp/"),
            bootstrap_servers=kv.get("bootstrap", "localhost:9092"),
            shuffle_partitions=int(kv.get("shuffle", str(default_shuffle))),
            raw_args=dict(kv),
        )
        cfg._log()
        return cfg

    # ------------------------------------------------------------------ typed accessors

    def arg(self, key: str, default: str | None = None) -> str | None:
        """Raw value of a ``key=value`` argument (case-insensitive), or ``default`` if not supplied."""
        value = self.raw_args.get(key.lower())
        return default if value is None or value == "" else value

    def arg_bool(self, key: str, default: bool) -> bool:
        value = self.arg(key)
        return default if value is None else _parse_bool(value)

    def table(self, default: str | None = None) -> str | None:
        """Target table name (``table=``)."""
        return self.arg("table", default)

    def format_version(self, default: str = FORMAT_VERSION) -> str:
        """Iceberg format version (``fv=2|3``)."""
        fv = self.arg("fv", default)
        if fv not in ("2", "3"):
            raise ValueError(f"fv must be 2 or 3, got: {fv}")
        return fv

    def mode(self, default: Mode) -> Mode:
        """Row-level operation mode (``mode=cow|mor``, per-job default)."""
        value = self.arg("mode")
        if value is None:
            return default
        v = value.lower()
        if v in ("cow", "copy-on-write"):
            return Mode.COW
        if v in ("mor", "merge-on-read"):
            return Mode.MOR
        raise ValueError(f"mode must be cow or mor, got: {value}")

    def file_format(self, default: FileFormat = FileFormat.PARQUET) -> FileFormat:
        """Iceberg data/delete file format (``fileformat=parquet|orc|avro``)."""
        value = self.arg("fileformat")
        if value is None:
            return default
        try:
            return FileFormat(value.lower())
        except ValueError:
            raise ValueError(f"fileformat must be parquet, orc or avro, got: {value}") from None

    def object_storage(self, default: bool = False) -> bool:
        """Iceberg object-storage layout toggle (``objectstorage=true|false``)."""
        return self.arg_bool("objectstorage", default)

    def source(self) -> Source:
        """Kafka payload format of the telemetry topics (``source=proto|avro|json``)."""
        value = self.arg("source", "proto")
        try:
            return Source(value.lower())
        except ValueError:
            raise ValueError(f"source must be proto, avro or json, got: {value}") from None

    def topic(self) -> str:
        """Kafka topic (``topic=``, default ``telemetry-<source>``)."""
        return self.arg("topic", f"telemetry-{self.source().value}")

    def dedup(self, default: Dedup) -> Dedup:
        """Dedup strategy (``dedup=none|batch|merge|watermark``; legacy true/false accepted)."""
        value = self.arg("dedup")
        if value is None:
            return default
        v = value.lower()
        if v in ("none", "false"):
            return Dedup.NONE
        if v == "batch":
            return Dedup.BATCH
        if v in ("merge", "true"):
            return Dedup.MERGE
        if v == "watermark":
            return Dedup.WATERMARK
        raise ValueError(f"dedup must be none, batch, merge or watermark, got: {value}")

    def compaction_mode(self, default: Compaction) -> Compaction:
        """Compaction strategy (``compaction=none|inline|scheduled``; legacy true/false accepted)."""
        value = self.arg("compaction")
        if value is None:
            return default
        v = value.lower()
        if v in ("none", "false"):
            return Compaction.NONE
        if v in ("inline", "true"):
            return Compaction.INLINE
        if v == "scheduled":
            return Compaction.SCHEDULED
        raise ValueError(f"compaction must be none, inline or scheduled, got: {value}")

    def trigger_kwargs(self, default_seconds: int) -> dict:
        """Keyword arguments for ``DataStreamWriter.trigger(**...)``.

        ``trigger=<seconds>`` gives a fixed processing-time trigger; ``trigger=availablenow`` turns
        the streaming job into a catch-up/backfill batch (drain everything available, then stop).
        """
        value = self.arg("trigger")
        if value is None:
            return {"processingTime": f"{default_seconds} seconds"}
        if value.lower() == "availablenow":
            return {"availableNow": True}
        try:
            return {"processingTime": f"{int(value)} seconds"}
        except ValueError:
            raise ValueError(f"trigger must be a number of seconds or 'availablenow', got: {value}") from None

    def watermark_delay(self) -> str:
        """Event-time watermark delay for ``dedup=watermark`` (``watermark=``, default 120s)."""
        return self.arg("watermark", "120 seconds")

    def fanout(self, default: bool) -> bool:
        """Spark fanout-writer toggle (``fanout=true|false``)."""
        return self.arg_bool("fanout", default)

    def manifest_merge(self, default: bool) -> bool:
        """Iceberg manifest merge-on-commit toggle (``manifestmerge=true|false``)."""
        return self.arg_bool("manifestmerge", default)

    def starting_offsets(self) -> str:
        """Kafka ``startingOffsets`` for a fresh checkpoint (``startingoffsets=``, default latest)."""
        return self.arg("startingoffsets", "latest")

    def region(self) -> str:
        """AWS region for the Glue Schema Registry examples (``region=``, default eu-west-1)."""
        return self.arg("region", "eu-west-1")

    @property
    def proto_descriptor(self) -> str:
        """Protobuf descriptor file path (``descriptor=``)."""
        return self.arg("descriptor", "../src/main/protobuf/VehicleTelemetry.desc")

    @property
    def avro_schema_file(self) -> str:
        """Avro schema (.avsc) file path (``avro=``)."""
        return self.arg("avro", "../src/main/avro/VehicleTelemetry.avsc")

    # ------------------------------------------------------------------ table DDL builder

    def table_properties_map(
        self, default_mode: Mode, overrides: Mapping[str, str] | None = None
    ) -> dict[str, str]:
        """Build the ``TBLPROPERTIES`` map for the selected table knobs.

        Single place the property recipe lives, so every example creates tables the same way.
        Format-specific compression properties are only emitted for the format in use (no parquet
        tuning on an ORC table); merge-on-read hash-distributes the row-level operation writes;
        metadata janitor properties keep the ``metadata.json`` log bounded on long-running streaming
        writers; commit retries are generous because streaming writes, MERGEs and compaction all
        race for optimistic commits.
        """
        mode = self.mode(default_mode)
        fmt = self.file_format().value

        p: dict[str, str] = {
            "table_type": "ICEBERG",
            "format-version": self.format_version(),
            "write.format.default": fmt,
            "write.delete.format.default": fmt,
        }
        if fmt == "parquet":
            p["write.parquet.compression-codec"] = "zstd"
            p["write.parquet.compression-level"] = "7"
            p["write.parquet.row-group-size-bytes"] = "134217728"  # 128 MiB
            p["write.parquet.page-size-bytes"] = "1048576"  # 1 MiB
        elif fmt == "orc":
            p["write.orc.compression-codec"] = "zstd"
        elif fmt == "avro":
            p["write.avro.compression-codec"] = "zstd"

        row_level = "merge-on-read" if mode is Mode.MOR else "copy-on-write"
        p["write.delete.mode"] = row_level
        p["write.update.mode"] = row_level
        p["write.merge.mode"] = row_level
        if mode is Mode.MOR:
            p["write.distribution-mode"] = "hash"
            p["write.delete.distribution-mode"] = "hash"
            p["write.update.distribution-mode"] = "hash"
            p["write.merge.distribution-mode"] = "hash"
        if self.object_storage(False):
            p["write.object-storage.enabled"] = "true"
        p["write.spark.fanout.enabled"] = str(self.fanout(True)).lower()
        p["write.target-file-size-bytes"] = "536870912"  # 512 MiB
        # Keep the metadata.json log bounded on long-running streaming writers.
        p["write.metadata.delete-after-commit.enabled"] = "true"
        p["write.metadata.previous-versions-max"] = "100"
        # Streaming writes, MERGEs and compaction all commit optimistically against the same table.
        p["commit.retry.num-retries"] = "20"
        p["commit.retry.min-wait-ms"] = "250"
        p["commit.retry.max-wait-ms"] = "60000"
        p["commit.manifest-merge.enabled"] = str(self.manifest_merge(True)).lower()
        p["compatibility.snapshot-id-inheritance.enabled"] = "true"
        if overrides:
            p.update(overrides)
        return p

    def create_table_ddl(
        self,
        table: str,
        columns_ddl: str,
        partition_ddl: str,
        default_mode: Mode,
        overrides: Mapping[str, str] | None = None,
    ) -> str:
        """Render a full ``CREATE TABLE IF NOT EXISTS`` statement for the given columns and
        partition spec, with ``TBLPROPERTIES`` derived from the table knobs."""
        props = ",\n            ".join(
            f"'{k}'='{v}'" for k, v in self.table_properties_map(default_mode, overrides).items()
        )
        return (
            f"CREATE TABLE IF NOT EXISTS {table}\n"
            f"      ({columns_ddl})\n"
            f"      PARTITIONED BY ({partition_ddl})\n"
            f"      TBLPROPERTIES (\n            {props} )"
        )

    # ------------------------------------------------------------------ checkpoints

    def checkpoint_for(self, query_name: str) -> str:
        """Derive a per-query checkpoint path under :attr:`checkpoint_location`.

        Every streaming query needs its own checkpoint (it encodes the query's schema and offsets and
        cannot be shared between different queries), so streaming examples must call this instead of
        using :attr:`checkpoint_location` directly.
        """
        import re

        base = self.checkpoint_location.rstrip("/")
        safe = re.sub(r"[^a-zA-Z0-9_.-]", "_", query_name)
        return f"{base}/{safe}"

    # ------------------------------------------------------------------ session / kafka

    @property
    def catalog_name(self) -> str:
        """Spark catalog name backing :data:`DATABASE` for the selected catalog."""
        return {
            Catalog.LOCAL: "local",
            Catalog.GLUE: "glue_catalog",
            Catalog.S3TABLES: "s3tablesbucket",
        }[self.catalog]

    def build_session(self, app_name: str) -> SparkSession:
        """Build a configured :class:`SparkSession` for the selected runtime and catalog."""
        builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            # Adaptive Query Execution: set a deliberately high initial shuffle partition count and
            # let AQE coalesce the small post-shuffle partitions at runtime.
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", str(self.shuffle_partitions))
        )

        if self.runtime is Runtime.LOCAL:
            builder = builder.master("local[*]")
            # On a local run the connector and Iceberg runtime jars are not on the classpath, so we
            # let Spark resolve them via Ivy. On EMR they are provided by the runtime.
            builder = builder.config("spark.jars.packages", ",".join(self._packages()))

        cat = self.catalog_name
        if self.catalog is Catalog.LOCAL:
            builder = (
                builder.config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
                .config(f"spark.sql.catalog.{cat}.type", "hadoop")
                .config(f"spark.sql.catalog.{cat}.warehouse", self.warehouse)
            )
        elif self.catalog is Catalog.GLUE:
            builder = (
                builder.config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
                .config(f"spark.sql.catalog.{cat}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                .config(f"spark.sql.catalog.{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config(f"spark.sql.catalog.{cat}.warehouse", self.warehouse)
                .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.sql.iceberg.data-prefetch.enabled", "true")
            )
        elif self.catalog is Catalog.S3TABLES:
            builder = (
                builder.config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
                .config(f"spark.sql.catalog.{cat}.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")
                .config(f"spark.sql.catalog.{cat}.warehouse", self.warehouse)
                .config("spark.sql.iceberg.data-prefetch.enabled", "true")
            )

        builder = builder.config("spark.sql.defaultCatalog", cat)
        return builder.getOrCreate()

    def kafka_stream(self, spark, topic: str):
        """Throughput-tuned Kafka structured-streaming source for ``topic``.

        Centralised so every consumer job stays consistent. Sets ``minPartitions`` to the shuffle
        partition count, large fetch/poll sizes and a big socket receive buffer. No
        ``maxOffsetsPerTrigger`` is set by default (drain all available data each micro-batch);
        pass it as an argument for rate limiting.
        """
        reader = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", topic)
            # start offsets on a fresh checkpoint (default latest); set startingOffsets=earliest for
            # a deterministic replay of a pre-loaded topic.
            .option("startingOffsets", self.starting_offsets())
            .option("minPartitions", str(self.shuffle_partitions))
            .option("kafka.fetch.min.bytes", "1048576")  # 1 MiB
            .option("kafka.fetch.max.bytes", "104857600")  # 100 MiB per fetch
            .option("kafka.max.partition.fetch.bytes", "10485760")  # 10 MiB per partition
            .option("kafka.max.poll.records", "50000")
            .option("kafka.receive.buffer.bytes", "16777216")  # 16 MiB socket buffer
        )
        max_offsets = self.arg("maxoffsetspertrigger")
        if max_offsets is not None:
            reader = reader.option("maxOffsetsPerTrigger", max_offsets)
        fail_on_data_loss = self.arg("failondataloss")
        if fail_on_data_loss is not None:
            reader = reader.option("failOnDataLoss", fail_on_data_loss)
        return reader.load()

    def _packages(self) -> list[str]:
        packages = [
            f"org.apache.iceberg:iceberg-spark-runtime-4.0_{_SCALA_BINARY}:{_ICEBERG_VERSION}",
            f"org.apache.iceberg:iceberg-aws-bundle:{_ICEBERG_VERSION}",
            f"org.apache.spark:spark-sql-kafka-0-10_{_SCALA_BINARY}:{_SPARK_VERSION}",
            f"org.apache.spark:spark-protobuf_{_SCALA_BINARY}:{_SPARK_VERSION}",
            f"org.apache.spark:spark-avro_{_SCALA_BINARY}:{_SPARK_VERSION}",
        ]
        if self.catalog is Catalog.S3TABLES:
            packages.append(
                f"software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:{_S3TABLES_VERSION}"
            )
        return packages

    def _log(self) -> None:
        log.warning(
            "JobConfig -> runtime=%s, catalog=%s (spark catalog '%s'), warehouse=%s, checkpoint=%s, "
            "bootstrap=%s, shuffle.partitions=%s, extra args=%s",
            self.runtime.value,
            self.catalog.value,
            self.catalog_name,
            self.warehouse,
            self.checkpoint_location,
            self.bootstrap_servers,
            self.shuffle_partitions,
            self.raw_args,
        )


def _parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_catalog(value: str) -> Catalog:
    v = value.lower()
    if v in {"local"}:
        return Catalog.LOCAL
    if v in {"glue", "glue_catalog"}:
        return Catalog.GLUE
    if v in {"s3tables", "s3table", "s3tablesbucket"}:
        return Catalog.S3TABLES
    raise ValueError(f"Unknown catalog '{value}'.\n{usage()}")


def usage() -> str:
    return (
        "Usage: [key=value ...]\n"
        "  runtime=local|emr             where Spark runs (default: local)\n"
        "  catalog=local|glue|s3tables   Iceberg catalog / storage (default: local)\n"
        "  warehouse=<path|s3 uri|arn>   catalog warehouse (default 'warehouse' for local;\n"
        "                                s3://... for glue; table bucket ARN for s3tables)\n"
        "  checkpoint=<path|s3 uri>      streaming checkpoint base dir (default: tmp/)\n"
        "  bootstrap=<host:port,...>     Kafka bootstrap servers (default: localhost:9092)\n"
        "  table=<name>                  target table name (job-specific default)\n"
        "  mode=cow|mor                  copy-on-write | merge-on-read (job-specific default)\n"
        "  fv=2|3                        Iceberg format-version (default 3)\n"
        "  fileformat=parquet|orc|avro   Iceberg data/delete file format (default parquet)\n"
        "  objectstorage=true|false      Iceberg object-storage layout (default false)\n"
        "  fanout=true|false             Spark fanout writers (default true)\n"
        "  manifestmerge=true|false      Iceberg manifest merge-on-commit (default true)\n"
        "  source=proto|avro|json        Kafka payload format (default proto)\n"
        "  topic=<name>                  Kafka topic (default: telemetry-<source>)\n"
        "  dedup=none|batch|merge|watermark  dedup strategy (job-specific default)\n"
        "  compaction=none|inline|scheduled  compaction strategy (default none)\n"
        "  trigger=<seconds>|availablenow    micro-batch trigger (job-specific default)\n"
        "  watermark=<duration>          watermark delay for dedup=watermark (default '120 seconds')\n"
        "  startingOffsets=latest|earliest|{json}  Kafka start offsets on a fresh checkpoint\n"
        "  maxOffsetsPerTrigger=<n>      cap records per micro-batch (default: unset -> drain all)\n"
        "  failOnDataLoss=true|false     Kafka failOnDataLoss (default: Kafka default true)\n"
        "  descriptor=<path>             protobuf descriptor (default ../src/main/protobuf/VehicleTelemetry.desc)\n"
        "  avro=<path>                   Avro .avsc schema (default ../src/main/avro/VehicleTelemetry.avsc)\n"
        "  shuffle=<n>                   spark.sql.shuffle.partitions (default: 200 local / 800 cloud)\n"
        "  region=<aws-region>           Glue Schema Registry region (default eu-west-1)"
    )

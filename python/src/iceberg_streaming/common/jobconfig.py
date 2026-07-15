"""Shared configuration and :class:`~pyspark.sql.SparkSession` factory for every PySpark example.

This is the Python counterpart of ``com.aws.emr.common.JobConfig`` in the Java project. It gives
all of the examples a single, consistent, ``key=value`` argument scheme that supports the three run
scenarios we care about:

1. **Local** -- Spark runs in ``local[*]`` against a Hadoop file based Iceberg catalog under
   ``./warehouse``. This is the default when no arguments are supplied.
2. **Local on top of Amazon S3 / S3 Tables** -- Spark still runs in ``local[*]`` (great for
   debugging) but reads/writes Iceberg data in Amazon S3 through the AWS Glue Data Catalog
   (``catalog=glue``) or in an Amazon S3 Tables bucket (``catalog=s3tables``).
3. **Amazon EMR on S3 / S3 Tables** -- the same catalogs as above but with ``runtime=emr`` so the
   master is inferred from the cluster instead of being forced to ``local[*]``.

Arguments (all optional, order independent)::

    runtime=local|emr           where Spark runs (default: local)
    catalog=local|glue|s3tables Iceberg catalog / storage (default: local)
    warehouse=<path|s3 uri|arn> catalog warehouse (default 'warehouse' for local; s3://... for glue;
                                the table bucket ARN for s3tables)
    checkpoint=<path|s3 uri>    structured streaming checkpoint dir (default: tmp/)
    bootstrap=<host:port,...>   Kafka bootstrap servers (default: localhost:9092)
    descriptor=<path>           protobuf descriptor file (default: Employee.desc)
    avro=<path>                 Avro .avsc schema file (default: ../src/main/avro/Employee.avsc)
    dedup=true|false            enable deduplication (default: false)
    compaction=true|false       enable periodic/async compaction (default: false)
    shuffle=<n>                 spark.sql.shuffle.partitions initial value, AQE coalesces (default: 200 local / 800 cloud)
    region=<aws-region>         Glue Schema Registry region (default: eu-west-1)

Every table created by the examples is an **Apache Iceberg format-version 3 (v3)** table. v3 became
production ready with Apache Iceberg 1.11.0 and brings deletion vectors, row lineage, the VARIANT
type, default column values and more.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Sequence

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("iceberg_streaming.jobconfig")

#: Iceberg table format version used by every example.
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


@dataclass(frozen=True)
class JobConfig:
    """Immutable, parsed job configuration and Spark session factory."""

    runtime: Runtime
    catalog: Catalog
    warehouse: str
    checkpoint_location: str
    bootstrap_servers: str
    proto_descriptor: str
    avro_schema_file: str
    remove_duplicates: bool
    compaction: bool
    shuffle_partitions: int
    region: str
    # All parsed key=value arguments, so example-specific options (table, fv, fanout, manifestmerge,
    # the streaming knobs, ...) are read through the typed accessors instead of re-parsing argv.
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
            proto_descriptor=kv.get("descriptor", "Employee.desc"),
            avro_schema_file=kv.get("avro", "../src/main/avro/Employee.avsc"),
            remove_duplicates=_parse_bool(kv.get("dedup", "false")),
            compaction=_parse_bool(kv.get("compaction", "false")),
            shuffle_partitions=int(kv.get("shuffle", str(default_shuffle))),
            region=kv.get("region", "eu-west-1"),
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
        """Target table for parameterised examples (``table=``)."""
        return self.arg("table", default)

    def format_version(self, default: str = FORMAT_VERSION) -> str:
        """Iceberg format version for parameterised examples (``fv=2|3``)."""
        return self.arg("fv", default)

    def fanout(self, default: bool) -> bool:
        """Spark fanout-writer toggle (``fanout=true|false``)."""
        return self.arg_bool("fanout", default)

    def manifest_merge(self, default: bool) -> bool:
        """Iceberg manifest merge-on-commit toggle (``manifestmerge=true|false``)."""
        return self.arg_bool("manifestmerge", default)

    def starting_offsets(self) -> str:
        """Kafka ``startingOffsets`` for a fresh checkpoint (``startingoffsets=``, default latest)."""
        return self.arg("startingoffsets", "latest")

    def checkpoint_for(self, query_name: str) -> str:
        """Derive a per-query checkpoint path under :attr:`checkpoint_location`.

        Every streaming query needs its own checkpoint (it encodes the query's schema and offsets and
        cannot be shared between different queries), so streaming examples should call this instead of
        using :attr:`checkpoint_location` directly.
        """
        import re

        base = self.checkpoint_location.rstrip("/")
        safe = re.sub(r"[^a-zA-Z0-9_.-]", "_", query_name)
        return f"{base}/{safe}"

    # ------------------------------------------------------------------ helpers

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
            # let AQE coalesce the small post-shuffle partitions at runtime, so we no longer have to
            # hand-tune spark.sql.shuffle.partitions per job / cluster size.
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
        partition count (more read parallelism than Kafka partitions; AQE coalesces downstream),
        large fetch/poll sizes and a big socket receive buffer so each poll pulls big batches. No
        ``maxOffsetsPerTrigger`` is set, so each micro-batch drains all available data (max
        throughput); add it per job for rate limiting.
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
        # Optional rate limit (unset by default -> drain all available data each micro-batch).
        max_offsets = self.arg("maxoffsetspertrigger")
        if max_offsets is not None:
            reader = reader.option("maxOffsetsPerTrigger", max_offsets)
        # failOnDataLoss defaults to Kafka's own default (true); set failOnDataLoss=false on a demo
        # topic with short retention so an aged-out offset does not kill the query.
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
            "bootstrap=%s, descriptor=%s, avro=%s, dedup=%s, compaction=%s, shuffle.partitions=%s. "
            "All tables are created as Iceberg format-version %s (v3).",
            self.runtime.value,
            self.catalog.value,
            self.catalog_name,
            self.warehouse,
            self.checkpoint_location,
            self.bootstrap_servers,
            self.proto_descriptor,
            self.avro_schema_file,
            self.remove_duplicates,
            self.compaction,
            self.shuffle_partitions,
            FORMAT_VERSION,
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
        "  runtime=local|emr           where Spark runs (default: local)\n"
        "  catalog=local|glue|s3tables Iceberg catalog / storage (default: local)\n"
        "  warehouse=<path|s3 uri|arn> catalog warehouse (default 'warehouse' for local;\n"
        "                              s3://... for glue; table bucket ARN for s3tables)\n"
        "  checkpoint=<path|s3 uri>    streaming checkpoint dir (default: tmp/)\n"
        "  bootstrap=<host:port,...>   Kafka bootstrap servers (default: localhost:9092)\n"
        "  descriptor=<path>           protobuf descriptor file (default: Employee.desc)\n"
        "  avro=<path>                 Avro .avsc schema file (default: ../src/main/avro/Employee.avsc)\n"
        "  dedup=true|false            enable deduplication (default: false)\n"
        "  compaction=true|false       enable periodic compaction (default: false)\n"
        "  shuffle=<n>                 spark.sql.shuffle.partitions initial value, AQE coalesces (default: 200 local / 800 cloud)\n"
        "  region=<aws-region>         Glue Schema Registry region (default: eu-west-1)\n"
        "  startingOffsets=latest|earliest|{json}  Kafka start offsets on a fresh checkpoint (default: latest)\n"
        "  maxOffsetsPerTrigger=<n>    cap records per micro-batch (default: unset -> drain all)\n"
        "  failOnDataLoss=true|false   Kafka failOnDataLoss (default: Kafka default true)\n"
        "  table=<name>                target table for parameterised examples (job-specific default)\n"
        "  fv=2|3                      Iceberg format-version for parameterised examples (job-specific default)\n"
        "  fanout=true|false           Spark fanout writers (job-specific default)\n"
        "  manifestmerge=true|false    Iceberg manifest merge-on-commit (job-specific default)"
    )

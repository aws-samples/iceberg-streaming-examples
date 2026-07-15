# PySpark Iceberg streaming examples

PySpark counterpart of the Java examples in the parent project. Most jobs are replicated in Python
(see [Notes / differences from the Java project](#notes--differences-from-the-java-project) for the
intentional exceptions), use the same unified `key=value` arguments, and create **Apache Iceberg
format-version 3 (v3)** tables by default. Targets **Apache Spark 4.0.2**, **Iceberg 1.11.0** and is
managed with [`uv`](https://docs.astral.sh/uv/).

## Requirements

* [`uv`](https://docs.astral.sh/uv/) (it manages the Python 3.12 toolchain and the virtualenv)
* Java 17 (Spark 4.0 runs on the JVM; PySpark launches it for you)
* A running Kafka broker for the streaming jobs (use the `docker-compose.yml` in the parent folder)

## Setup

```bash
cd python
uv sync            # creates .venv with Python 3.12, PySpark 4.0.2 and the rest
```

The Spark connector and Iceberg runtime jars (Kafka, Avro, Protobuf, `iceberg-spark-runtime-4.0`,
`iceberg-aws-bundle`, and the S3 Tables catalog) are **not** vendored. For local runs
`iceberg_streaming.common.jobconfig` adds them through `spark.jars.packages`, so Spark resolves them
from Maven Central via Ivy on first launch (this needs network access the first time). On EMR they
are provided by the runtime.

## Unified arguments

Exactly the same scheme as the Java project (see `iceberg_streaming/common/jobconfig.py`):

```
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
shuffle=<n>                 spark.sql.shuffle.partitions initial value; AQE coalesces (default: 200 local / 800 cloud)
region=<aws-region>         Glue Schema Registry region (default: eu-west-1)
```

### The three run scenarios

```bash
# 1) pure local dev (hadoop catalog under ./warehouse, kafka on localhost:9092)
uv run iot-custom-ingest

# 2) local Spark, data in Amazon S3 via the Glue Data Catalog
uv run iot-custom-ingest catalog=glue warehouse=s3://your-bucket/warehouse \
    checkpoint=s3://your-bucket/checkpoint bootstrap=broker:9092 dedup=true

# 2b) local Spark, Amazon S3 Tables managed catalog (needs valid AWS credentials)
uv run iot-custom-ingest catalog=s3tables \
    warehouse=arn:aws:s3tables:eu-west-1:111122223333:bucket/my-table-bucket \
    checkpoint=s3://your-bucket/checkpoint bootstrap=broker:9092

# 3) on EMR (runtime=emr => master inferred from the cluster)
spark-submit --py-files ... iceberg_streaming/iot/spark_custom_iceberg_ingest.py \
    runtime=emr catalog=glue warehouse=s3://your-bucket/warehouse checkpoint=s3://... bootstrap=...
```

## Console entry points

`uv run <name>` (defined in `pyproject.toml`), or `uv run python -m iceberg_streaming.<pkg>.<module>`:

| Entry point | Module | Java equivalent |
|---|---|---|
| `iot-custom-ingest` | `iceberg_streaming.iot.spark_custom_iceberg_ingest` | `SparkCustomIcebergIngest` |
| `iot-mor` | `iceberg_streaming.iot.spark_custom_iceberg_ingest_mor` | `SparkCustomIcebergIngestMoR` |
| `iot-s3buckets-avro` | `iceberg_streaming.iot.s3buckets_avro` | `...MoRS3BucketsAvro` |
| `iot-s3buckets-orc` | `iceberg_streaming.iot.s3buckets_orc` | `...MoRS3BucketsORC` |
| `iot-s3buckets-auto-avro` | `iceberg_streaming.iot.s3buckets_auto_avro` | `...MoRS3BucketsAutoAvro` |
| `iot-s3buckets-auto-orc` | `iceberg_streaming.iot.s3buckets_auto_orc` | `...MoRS3BucketsAutoORC` |
| `iot-proto-hex` | `iceberg_streaming.iot.spark_custom_iceberg_ingest_proto_hex` | `SparkCustomIcebergIngestProtoHex` |
| `iceberg-utils` | `iceberg_streaming.iot.spark_iceberg_utils` | `SparkIcebergUtils` |
| `proto-native` | `iceberg_streaming.proto.spark_native_iceberg_ingest_proto` | `SparkNativeIcebergIngestProto` |
| `proto-udf` | `iceberg_streaming.proto.spark_proto_udf` | `SparkProtoUDF` |
| `avro-native` | `iceberg_streaming.avro.spark_native_iceberg_ingest_avro` | `SparkNativeIcebergIngestAvro` |
| `avro-parquet-mor` | `iceberg_streaming.avro.spark_avro_ingest_mor_parquet` | `SparkCustomIcebergIngestMoRAvroParquet` |
| `cdc-log-change` | `iceberg_streaming.cdc.spark_log_change` | `SparkLogChange` |
| `cdc-mirror` | `iceberg_streaming.cdc.spark_cdc_mirror` | `SparkCDCMirror` |
| `cdc-incremental` | `iceberg_streaming.cdc.spark_incremental_pipeline` | `SparkIncrementalPipeline` |
| `cdc-streaming-mirror` | `iceberg_streaming.cdc.spark_streaming_cdc_mirror` | `SparkStreamingCDCMirror` |
| `iceberg-maintenance` | `iceberg_streaming.maintenance.iceberg_maintenance` | `IcebergMaintenance` |
| `scenario` | `iceberg_streaming.scenarios.runner` | (deterministic end-to-end harness; Python-only) |
| `proto-producer` | `iceberg_streaming.kafka.proto_producer` | `ProtoProducer` |
| `avro-producer` | `iceberg_streaming.kafka.avro_producer` | `AvroProducer` |
| `json-producer` | `iceberg_streaming.kafka.json_producer` | `JsonProducerSchemaRegistry` (native JSON) |
| `cdc-simulator` | `iceberg_streaming.kafka.kafka_cdc_simulator` | `KafkaCDCSimulator` |
| `proto-consumer` | `iceberg_streaming.kafka.proto_consumer` | `ProtoConsumer` |
| `avro-consumer` | `iceberg_streaming.kafka.avro_consumer` | `AvroConsumer` |

## Scenario harness (`scenario`)

A deterministic, self-checking end-to-end harness lives in `iceberg_streaming/scenarios/`. It seeds a
fixed, `seed`-controlled CDC dataset, runs it through the **same shared guarded MERGE** the jobs use
(`iceberg_streaming.cdc._sql.mirror_merge`) in bounded micro-batches, then asserts the final Iceberg
table state against a pure-Python oracle and reports metadata (snapshots, data files, delete files).
It runs fully locally (Hadoop catalog under a throwaway temp dir; no AWS).

```bash
uv run scenario cdc-out-of-order          # stale updates across batches must not overwrite newer rows
uv run scenario cdc-ordered               # in-order I/U/D lifecycle, terminal deletes
uv run scenario all                       # run every scenario (memory source)
uv run scenario cdc-ordered source=kafka bootstrap=localhost:9092   # real Kafka + Trigger.AvailableNow
uv run scenario mor-v2 && uv run scenario mor-v3   # identical final state, different delete encoding
```

The exit code is non-zero if any scenario's final state does not match the oracle, so it doubles as a
local smoke test. Sources: `memory` (default, no broker needed) or `kafka` (needs the
`docker-compose.yml` broker). Options: `seed=`, `keys=`, `events=`, `fv=`, `keep=true`.

| Scenario | What it proves |
|---|---|
| `append-only` | duplicate re-sends are deduped; every key present once |
| `cdc-ordered` | in-order updates + terminal deletes land correctly (~30% keys deleted) |
| `cdc-out-of-order` | **the guard fix**: shuffled, multi-batch stale updates never overwrite newer rows |
| `resurrection-demo` | reproduces + asserts the documented physical-delete "resurrection" limitation |
| `mor-v2` / `mor-v3` | v2 and v3 produce an identical logical result; the report surfaces per-table snapshot / data-file / delete-file counts (the v2 positional-delete vs v3 deletion-vector gap grows under sustained streaming churn, not a single bounded run) |

Only the pure oracle model (`events.py`) is unit-tested in CI (`test_scenarios_model.py`); the
Spark+Kafka execution is a local lab tool (it needs the Spark/Iceberg runtime and, on first run,
network access for Ivy to fetch the jars).

## Protobuf bindings

The protobuf producer/consumer use generated bindings. Regenerate them (needs the `dev` group,
installed by `uv sync`) with:

```bash
./scripts/gen_proto.sh
```

This produces `iceberg_streaming/proto_gen/*_pb2.py` from `../src/main/protobuf/Employee.proto`.

## Notes / differences from the Java project

* The **Glue Schema Registry** producers/consumers (`*SchemaRegistry`), including the Spark GSR
  consumer (`SparkProtoRegistry`), are **not** replicated in Python: there is no first-class Python
  Glue Schema Registry serde, and the GSR deserializer is JVM-only so it cannot run inside PySpark
  Python workers. Use the Java jobs for the Glue Schema Registry scenarios.
* The JSON example is provided as a plain native JSON producer/consumer (the Java one used the Glue
  registry).
* The Java **S3 Tables v2/v3 benchmark** classes (`SparkS3TablesMergeV2`, `SparkS3TablesMergeV3`,
  `SparkS3TablesTwoQuerySpj`, `SparkCDCReadBenchmark`) are benchmark-only and are **not** ported.
* `cdc-streaming-mirror` is ported and parameterised (`table`, `fv`, `fanout`, `manifestmerge`) to
  match the Java job, and both share the same deterministic, guarded MERGE (see the root README
  "CDC correctness assumptions").
* `cdc-incremental` advances its source watermark in a **separate** `ALTER TABLE` commit rather than
  atomically inside the MERGE commit (the Java `CommitMetadata` thread-local is JVM-only). If the job
  dies between the two, the range is simply reprocessed on the next run. See the module docstring.

## Troubleshooting

* **`download failed: org.apache.kafka#kafka-clients;<v>!kafka-clients.jar` (or a similar Ivy error)
  on the first local run.** Spark resolves `spark.jars.packages` through Ivy, whose resolver chain
  includes your local Maven cache (`~/.m2`). If that cache holds a *pom-only* entry for a jar (for
  example left behind by a previous Maven build that overrode the version), Ivy claims the module
  from `~/.m2` and then fails to find the jar without falling back to Maven Central. Fix it by
  fetching the missing jar into `~/.m2`, e.g.
  `mvn dependency:get -Dartifact=org.apache.kafka:kafka-clients:<v>` (or delete the offending
  `~/.m2/repository/.../<v>/` directory so it is re-downloaded cleanly).
* The protobuf Spark jobs need the descriptor file. It defaults to `Employee.desc` in the current
  directory; pass `descriptor=../src/main/protobuf/Employee.desc` (generated in the parent project)
  or copy it into `python/`.

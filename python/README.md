# PySpark Iceberg streaming examples

PySpark counterpart of the Java examples in the parent project: EV vehicle telemetry (IoT) and a
bank-account CDC feed, ingested from Kafka into Apache Iceberg with Structured Streaming. The jobs
use the same unified `key=value` arguments as the Java classes, so one module covers a whole matrix
of table layouts and behaviours instead of one file per variant. Targets **Apache Spark 4.0.2**,
**Iceberg 1.11.0** and is managed with [`uv`](https://docs.astral.sh/uv/).

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
runtime=local|emr             where Spark runs (default: local)
catalog=local|glue|s3tables   Iceberg catalog / storage (default: local)
warehouse=<path|s3 uri|arn>   catalog warehouse (default 'warehouse' for local; s3://... for glue;
                              the table bucket ARN for s3tables)
checkpoint=<path|s3 uri>      streaming checkpoint base dir (default: tmp/); per-query paths derived
bootstrap=<host:port,...>     Kafka bootstrap servers (default: localhost:9092)

table=<name>                  target table name (job-specific default)
mode=cow|mor                  copy-on-write | merge-on-read row-level operations
fv=2|3                        Iceberg format-version (default 3; v3 => deletion vectors)
fileformat=parquet|orc|avro   Iceberg data/delete file format (default parquet)
objectstorage=true|false      Iceberg object-storage layout for S3 (default false)
fanout=true|false             Spark fanout writers (default true)
manifestmerge=true|false      Iceberg manifest merge-on-commit (default true)

source=proto|avro|json        Kafka payload format for the telemetry jobs (default proto)
topic=<name>                  Kafka topic (default: telemetry-<source>; CDC topics are fixed)
dedup=none|batch|merge|watermark  dedup strategy (job-specific default)
compaction=none|inline|scheduled  compaction strategy (default none)
trigger=<seconds>|availablenow    micro-batch trigger; availablenow = drain the topic and stop
watermark=<duration>          watermark delay for dedup=watermark (default '120 seconds')
startingOffsets=latest|earliest|{json}  Kafka start offsets on a fresh checkpoint
maxOffsetsPerTrigger=<n>      cap records per micro-batch (default: unset -> drain all)
failOnDataLoss=true|false     Kafka failOnDataLoss (default: Kafka default true)

descriptor=<path>             protobuf descriptor (default ../src/main/protobuf/VehicleTelemetry.desc)
avro=<path>                   Avro .avsc schema (default ../src/main/avro/VehicleTelemetry.avsc)
shuffle=<n>                   spark.sql.shuffle.partitions initial value; AQE coalesces
region=<aws-region>           Glue Schema Registry region (default: eu-west-1)
```

### The three run scenarios

```bash
# 1) pure local dev (hadoop catalog under ./warehouse, kafka on localhost:9092)
uv run iot-custom-ingest dedup=merge

# 2) local Spark, data in Amazon S3 via the Glue Data Catalog
uv run iot-custom-ingest catalog=glue warehouse=s3://your-bucket/warehouse \
    checkpoint=s3://your-bucket/checkpoint bootstrap=broker:9092 dedup=merge

# 2b) local Spark, Amazon S3 Tables managed catalog (needs valid AWS credentials)
uv run iot-custom-ingest catalog=s3tables \
    warehouse=arn:aws:s3tables:eu-west-1:111122223333:bucket/my-table-bucket \
    checkpoint=s3://your-bucket/checkpoint bootstrap=broker:9092

# 3) on EMR (runtime=emr => master inferred from the cluster)
spark-submit --py-files ... iceberg_streaming/iot/spark_custom_iceberg_ingest.py \
    runtime=emr catalog=glue warehouse=s3://your-bucket/warehouse checkpoint=s3://... bootstrap=...
```

### Knob matrix examples

The same module covers what used to be many near-identical ones:

```bash
uv run iot-custom-ingest mode=mor fileformat=avro objectstorage=true   # was "s3buckets avro MoR"
uv run iot-custom-ingest mode=mor fileformat=orc                      # was the ORC MoR variant
uv run iot-custom-ingest source=avro                                  # was the Avro-source job
uv run iot-custom-ingest source=json dedup=batch                      # JSON + dead-letter table
uv run iot-custom-ingest dedup=merge compaction=inline                # in-job compaction demo
uv run iot-custom-ingest dedup=merge compaction=scheduled             # hourly background compaction
uv run iot-custom-ingest trigger=availablenow startingOffsets=earliest  # catch-up/backfill batch
uv run cdc-streaming-mirror table=accounts_mirror_v2 fv=2             # v2 vs v3 A/B, same module
```

## Console entry points

`uv run <name>` (defined in `pyproject.toml`), or `uv run python -m iceberg_streaming.<pkg>.<module>`:

| Entry point | Module | Java equivalent |
|---|---|---|
| `iot-native-ingest` | `iceberg_streaming.iot.spark_native_iceberg_ingest` | `SparkNativeIcebergIngest` |
| `iot-custom-ingest` | `iceberg_streaming.iot.spark_custom_iceberg_ingest` | `SparkCustomIcebergIngest` |
| `proto-udf` | `iceberg_streaming.iot.spark_proto_udf` | `SparkProtoUDF` |
| `iceberg-utils` | `iceberg_streaming.iot.spark_iceberg_utils` | `SparkIcebergUtils` |
| `cdc-log-change` | `iceberg_streaming.cdc.spark_log_change` | `SparkLogChange` |
| `cdc-mirror` | `iceberg_streaming.cdc.spark_cdc_mirror` | `SparkCDCMirror` |
| `cdc-incremental` | `iceberg_streaming.cdc.spark_incremental_pipeline` | `SparkIncrementalPipeline` |
| `cdc-streaming-mirror` | `iceberg_streaming.cdc.spark_streaming_cdc_mirror` | `SparkStreamingCDCMirror` |
| `iceberg-maintenance` | `iceberg_streaming.maintenance.iceberg_maintenance` | `IcebergMaintenance` |
| `scenario` | `iceberg_streaming.scenarios.runner` | (deterministic end-to-end harness; Python-only) |
| `telemetry-producer` | `iceberg_streaming.kafka.telemetry_producer` | `TelemetryProducer` |
| `telemetry-consumer` | `iceberg_streaming.kafka.telemetry_consumer` | `TelemetryConsumer` |
| `cdc-simulator` | `iceberg_streaming.kafka.kafka_cdc_simulator` | `KafkaCDCSimulator` |

The telemetry producer/consumer take `format=proto|avro|json` plus `count=`, `rate=`, `vehicles=`,
`late=`, `duplicates=` and (JSON only) `corrupt=true` to feed the dead-letter example:

```bash
uv run telemetry-producer format=json corrupt=true count=100000
uv run telemetry-consumer format=json count=10
uv run cdc-simulator count=10000 accounts=100
```

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
smoke test (CI runs `scenario all` with the memory source). Options: `seed=`, `keys=`, `events=`,
`fv=`, `keep=true`.

| Scenario | What it proves |
|---|---|
| `append-only` | duplicate re-sends are deduped; every key present once |
| `cdc-ordered` | in-order updates + terminal deletes land correctly (~30% keys deleted) |
| `cdc-out-of-order` | **the guard fix**: shuffled, multi-batch stale updates never overwrite newer rows |
| `resurrection-demo` | reproduces + asserts the documented physical-delete "resurrection" limitation |
| `mor-v2` / `mor-v3` | v2 and v3 produce an identical logical result; the report surfaces per-table snapshot / data-file / delete-file counts |

## Protobuf bindings

The protobuf producer/consumer and the proto UDF job use generated bindings. Regenerate them (needs
the dev group, installed by `uv sync`) with:

```bash
./scripts/gen_proto.sh
```

This produces `iceberg_streaming/proto_gen/VehicleTelemetry_pb2.py` from
`../src/main/protobuf/VehicleTelemetry.proto`.

## Notes / differences from the Java project

* The **Glue Schema Registry** examples (`TelemetryRegistryProducer/Consumer`, `SparkProtoRegistry`)
  are **not** replicated in Python: there is no first-class Python Glue Schema Registry serde, and
  the GSR deserializer is JVM-only so it cannot run inside PySpark Python workers. Use the Java jobs
  for the Glue Schema Registry scenarios.
* The **Storage-Partitioned Join** jobs (`SparkIcebergIngestSpj`, `SparkS3TablesTwoQuerySpj`) and the
  **read benchmark** (`SparkCDCReadBenchmark`) are Java-only.
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
* The protobuf Spark jobs need the descriptor file. It defaults to
  `../src/main/protobuf/VehicleTelemetry.desc` (relative to `python/`); pass `descriptor=` to point
  elsewhere.

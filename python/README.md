# PySpark Iceberg streaming examples

PySpark counterpart of the Java examples in the parent project. Every job is replicated in Python,
uses the same unified `key=value` arguments, and creates **Apache Iceberg format-version 3 (v3)**
tables. Targets **Apache Spark 4.0.2**, **Iceberg 1.11.0** and is managed with
[`uv`](https://docs.astral.sh/uv/).

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
| `cdc-log-change` | `iceberg_streaming.cdc.spark_log_change` | `SparkLogChange` |
| `cdc-mirror` | `iceberg_streaming.cdc.spark_cdc_mirror` | `SparkCDCMirror` |
| `cdc-incremental` | `iceberg_streaming.cdc.spark_incremental_pipeline` | `SparkIncrementalPipeline` |
| `proto-producer` | `iceberg_streaming.kafka.proto_producer` | `ProtoProducer` |
| `avro-producer` | `iceberg_streaming.kafka.avro_producer` | `AvroProducer` |
| `json-producer` | `iceberg_streaming.kafka.json_producer` | `JsonProducerSchemaRegistry` (native JSON) |
| `cdc-simulator` | `iceberg_streaming.kafka.kafka_cdc_simulator` | `KafkaCDCSimulator` |
| `proto-consumer` | `iceberg_streaming.kafka.proto_consumer` | `ProtoConsumer` |
| `avro-consumer` | `iceberg_streaming.kafka.avro_consumer` | `AvroConsumer` |

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

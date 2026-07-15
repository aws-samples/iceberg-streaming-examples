"""Shared schema and Kafka-payload decoding for the EV vehicle telemetry examples.

Python counterpart of ``com.aws.emr.spark.iot.Telemetry``. Every IoT job consumes the same logical
``VehicleTelemetry`` record from Kafka in one of three payload formats (``source=proto|avro|json``);
this module centralises the decode so the jobs only differ in *how they write*, not in how they
parse.

Two Kafka lineage columns (``kafka_partition``, ``kafka_offset``) are carried into the table on
purpose: free to obtain, invaluable for debugging, and the deterministic tiebreaker of the dedup
logic (see :mod:`iceberg_streaming.iot._sql`).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructType,
)

from iceberg_streaming.common import JobConfig, Source

#: Default telemetry table name (override per run with ``table=``).
TABLE = "vehicle_telemetry"

#: Column list shared by every telemetry table the examples create.
COLUMNS_DDL = (
    "vehicle_id bigint,\n"
    "          event_time timestamp,\n"
    "          model string,\n"
    "          speed_kmh int,\n"
    "          soc_pct int,\n"
    "          odometer_km bigint,\n"
    "          charging boolean,\n"
    "          kafka_partition int,\n"
    "          kafka_offset bigint"
)

#: Partition spec: hourly event-time partitions plus a modest bucket count on the vehicle id.
PARTITION_DDL = "hours(event_time), bucket(16, vehicle_id)"

#: Schema of the JSON payload; ``event_time`` is epoch milliseconds.
JSON_SCHEMA = (
    StructType()
    .add("vehicle_id", LongType())
    .add("event_time", LongType())
    .add("model", StringType())
    .add("speed_kmh", IntegerType())
    .add("soc_pct", IntegerType())
    .add("odometer_km", LongType())
    .add("charging", BooleanType())
)


def decode(kafka: DataFrame, cfg: JobConfig) -> DataFrame:
    """Decode the raw Kafka stream into the uniform telemetry columns for the configured
    ``source=``. Corrupt JSON records are silently dropped here; use :func:`decode_json_with_raw`
    when you need them captured in a dead-letter table."""
    source = cfg.source()
    if source is Source.PROTO:
        return _decode_proto(kafka, cfg)
    if source is Source.AVRO:
        return _decode_avro(kafka, cfg)
    return decode_json_with_raw(kafka).filter(F.col("vehicle_id").isNotNull()).drop("raw_value")


def _telemetry_columns(event_time_col):
    return [
        F.col("t.vehicle_id"),
        event_time_col.alias("event_time"),
        F.col("t.model"),
        F.col("t.speed_kmh"),
        F.col("t.soc_pct"),
        F.col("t.odometer_km"),
        F.col("t.charging"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
    ]


def _decode_proto(kafka: DataFrame, cfg: JobConfig) -> DataFrame:
    from pyspark.sql.protobuf.functions import from_protobuf

    # spark-protobuf maps google.protobuf.Timestamp to a Spark timestamp natively.
    return kafka.select(
        from_protobuf(F.col("value"), "VehicleTelemetry", cfg.proto_descriptor).alias("t"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    ).select(_telemetry_columns(F.col("t.event_time")))


def _decode_avro(kafka: DataFrame, cfg: JobConfig) -> DataFrame:
    from pyspark.sql.avro.functions import from_avro

    with open(cfg.avro_schema_file, encoding="utf-8") as fh:
        json_format_schema = fh.read()
    return kafka.select(
        from_avro(F.col("value"), json_format_schema, {"mode": "PERMISSIVE"}).alias("t"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    ).select(_telemetry_columns(F.timestamp_millis(F.col("t.event_time"))))


def decode_json_with_raw(kafka: DataFrame) -> DataFrame:
    """Decode a JSON payload keeping the raw Kafka value alongside the parsed columns.

    A record that fails to parse surfaces with ``vehicle_id IS NULL`` and the original line still in
    ``raw_value`` - exactly what a dead-letter table needs (see ``spark_custom_iceberg_ingest``).
    """
    return (
        kafka.select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .withColumn("t", F.from_json(F.col("raw_value"), JSON_SCHEMA))
        .select(_telemetry_columns(F.timestamp_millis(F.col("t.event_time"))) + [F.col("raw_value")])
    )

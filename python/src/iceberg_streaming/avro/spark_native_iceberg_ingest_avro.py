"""Apache Avro -> Iceberg v3 using the native connector and native (toTable) writer.

PySpark counterpart of ``com.aws.emr.avro.kafka.SparkNativeIcebergIngestAvro``. The Avro schema is
read from the ``avro=`` argument (default ``../src/main/avro/Employee.avsc``). Optional dedup uses an
event-time watermark.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from iceberg_streaming.common import DATABASE, JobConfig

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS employee
          (employee_id bigint,
          age int,
          start_date timestamp,
          team string,
          role string,
          address string,
          name string
          )
          PARTITIONED BY (bucket(8, employee_id), hours(start_date), team)
          TBLPROPERTIES (
                    'table_type'='ICEBERG',
                    'format-version'='3',
                    'write.parquet.compression-level'='7',
                    'format'='parquet',
                    'commit.retry.num-retries'='10',
                    'commit.retry.min-wait-ms'='250',
                    'commit.retry.max-wait-ms'='60000',
                    'write.parquet.compression-codec'='zstd',
                    'compatibility.snapshot-id-inheritance.enabled'='true' )
"""


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])

    json_format_schema = Path(cfg.avro_schema_file).read_text(encoding="utf-8")

    spark = cfg.build_session("PySparkNativeAvro2Iceberg")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(_CREATE_TABLE)

    df = cfg.kafka_stream(spark, "avro-demo-topic-pure")

    output = (
        df.select(from_avro(F.col("value"), json_format_schema, {"mode": "PERMISSIVE"}).alias("Employee"))
        .select(F.col("Employee.*"))
        .select(
            F.col("employee_id"),
            F.col("age"),
            F.col("start_date").cast("timestamp"),
            F.col("team"),
            F.col("role"),
            F.col("address"),
            F.col("name"),
        )
    )

    if cfg.remove_duplicates:
        output = output.withWatermark("start_date", "120 seconds").dropDuplicatesWithinWatermark(
            ["start_date", "employee_id"]
        )

    query = (
        output.writeStream.queryName("streaming-avro-ingest")
        .format("iceberg")
        .trigger(processingTime="1 minute")
        .outputMode("append")
        .option("checkpointLocation", cfg.checkpoint_location)
        .option("fanout-enabled", "true")
        .toTable("employee")
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

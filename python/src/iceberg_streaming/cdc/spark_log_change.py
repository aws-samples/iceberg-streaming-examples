"""CDC (DMS-like CSV) -> Iceberg v3 changelog table.

PySpark counterpart of ``com.aws.emr.spark.cdc.SparkLogChange``. Consumes comma separated CDC
records from Kafka and appends them to the ``accounts_changelog`` table.
"""

from __future__ import annotations

import sys

from pyspark.sql import functions as F

from iceberg_streaming.common import DATABASE, JobConfig

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS accounts_changelog
          (
          operation string,
          account_id bigint,
          balance bigint,
          last_updated timestamp,
          seq bigint            -- source sequence (LSN surrogate) for deterministic ordering
          )
          PARTITIONED BY (days(last_updated), bucket(8, account_id))
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

# DMS-like CSV: operation,account_id,balance,last_updated(epoch millis),seq
_SCHEMA = ["operation", "account_id", "balance", "last_updated", "seq"]


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkCDCLogChangeWriter")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(_CREATE_TABLE)

    df = cfg.kafka_stream(spark, "streaming-cdc-log-ingest")

    output = df.selectExpr("CAST(value AS STRING) as value")
    split_col = F.split(F.col("value"), ",")
    for i, name in enumerate(_SCHEMA):
        output = output.withColumn(name, split_col.getItem(i))

    output = (
        output.drop("value")
        .withColumn("account_id", F.col("account_id").cast("integer"))
        .withColumn("balance", F.col("balance").cast("integer"))
        # spark sql does not support epoch millis, so divide by 1000 to get seconds
        .withColumn("last_updated", (F.col("last_updated") / 1000).cast("timestamp"))
        .withColumn("seq", F.col("seq").cast("long"))
    )
    output.printSchema()

    query = (
        output.writeStream.queryName("cdc")
        .format("iceberg")
        .trigger(processingTime="2 minutes")
        .outputMode("append")
        # per-query checkpoint so it never collides with the other streaming examples
        .option("checkpointLocation", cfg.checkpoint_for("cdc-log-change"))
        .toTable("accounts_changelog")
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

"""CDC changelog writer: DMS-like CSV feed -> ``accounts_changelog`` Iceberg table.

PySpark counterpart of ``com.aws.emr.spark.cdc.SparkLogChange``. Appends the raw, replayable change
history the mirror pipelines (``cdc-mirror``, ``cdc-incremental``) consume. ``balance`` is in minor
units (cents) and stays ``bigint`` end to end - money never touches a float.

Deduplication (``dedup=none|batch``): the producer's ``seq`` is unique per source change, so a
duplicate delivery repeats the same ``seq``. Duplicates are harmless to the mirror (its windowed
dedup picks one row per key anyway) but they pollute a table analysts query directly;
``dedup=batch`` switches to ``foreachBatch`` and drops repeated ``seq`` values inside each
micro-batch - one cheap shuffle, no state. Cross-batch duplicates still land; the guarded mirror
MERGE absorbs them.
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, functions as F

from iceberg_streaming.cdc import _sql
from iceberg_streaming.common import DATABASE, Dedup, JobConfig, Mode
from iceberg_streaming.common.observability import attach_progress_listener

log = logging.getLogger("iceberg_streaming.cdc.spark_log_change")

TOPIC = "streaming-cdc-log-ingest"
TABLE = "accounts_changelog"

# DMS-like CSV: operation,account_id,balance,last_updated(epoch millis),seq
_CSV_SCHEMA = ["operation", "account_id", "balance", "last_updated", "seq"]


def parse_cdc_csv(kafka: DataFrame) -> DataFrame:
    """Parse the raw Kafka CSV values into typed changelog columns (shared with the streaming mirror)."""
    parsed = kafka.selectExpr("CAST(value AS STRING) AS value")
    split_col = F.split(F.col("value"), ",")
    for i, name in enumerate(_CSV_SCHEMA):
        parsed = parsed.withColumn(name, split_col.getItem(i))
    return (
        parsed.drop("value")
        .withColumn("account_id", F.col("account_id").cast("bigint"))
        .withColumn("balance", F.col("balance").cast("bigint"))  # minor units; never a float
        .withColumn("last_updated", F.timestamp_millis(F.col("last_updated").cast("bigint")))
        .withColumn("seq", F.col("seq").cast("bigint"))
    )


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkCDCLogChangeWriter")

    dedup = cfg.dedup(Dedup.NONE)
    if dedup not in (Dedup.NONE, Dedup.BATCH):
        raise ValueError(
            "The changelog writer supports dedup=none or dedup=batch (it appends history; keyed"
            " dedup belongs to the mirror jobs)."
        )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(
        cfg.create_table_ddl(TABLE, _sql.CHANGELOG_COLUMNS_DDL, _sql.CHANGELOG_PARTITION_DDL, Mode.COW)
    )

    output = parse_cdc_csv(cfg.kafka_stream(spark, TOPIC))
    output.printSchema()

    table_fqn = f"{cfg.catalog_name}.{DATABASE}.{TABLE}"

    attach_progress_listener(spark)

    writer = (
        output.writeStream.queryName("cdc-log-change")
        .trigger(**cfg.trigger_kwargs(120))
        .outputMode("append")
        # per-query checkpoint so it never collides with the other streaming examples
        .option("checkpointLocation", cfg.checkpoint_for("cdc-log-change"))
    )

    if dedup is Dedup.BATCH:

        def process_batch(batch, batch_id: int) -> None:
            if batch.isEmpty():
                return
            # seq is unique per source change: repeated seq = duplicate delivery.
            batch.dropDuplicates(["seq"]).writeTo(table_fqn).append()

        query = writer.foreachBatch(process_batch).start()
    else:
        query = writer.format("iceberg").toTable(TABLE)

    query.awaitTermination()


if __name__ == "__main__":
    main()

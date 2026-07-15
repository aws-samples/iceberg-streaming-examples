"""Streaming ("continuous") CDC mirror MERGE pattern.

PySpark counterpart of ``com.aws.emr.spark.cdc.SparkStreamingCDCMirror``. Where
``spark_cdc_mirror`` runs a single batch pass over ``accounts_changelog`` and
``spark_incremental_pipeline`` processes one snapshot range per run, this job keeps a long-running
structured-streaming query that consumes the DMS-like CDC feed straight from Kafka (topic
``streaming-cdc-log-ingest``, same CSV format as ``spark_log_change``) and, on every micro-batch,
deduplicates to the latest change per ``account_id`` and MERGEs it directly into the target mirror
table. There is no intermediate changelog table.

**Why this is the deletion-vector workload.** The MERGE is keyed on ``account_id`` alone, so as the
same accounts change again and again the micro-batches overwhelmingly hit ``WHEN MATCHED`` -- every
update and delete rewrites an existing row. In merge-on-read that produces a steady stream of
row-level delete files on every commit, which is exactly where Iceberg v3 *deletion vectors* clearly
beat v2 *positional delete files*. An insert-only MERGE, by contrast, is pure appends and shows no
difference between v2 and v3.

**Comparing v2 and v3 under an identical workload** (parity with the Java job): the target table name
and Iceberg format version are parameterised, so the same module runs twice against the same feed::

    cdc-streaming-mirror table=accounts_mirror_v2 fv=2 checkpoint=<cp-v2>
    cdc-streaming-mirror table=accounts_mirror_v3 fv=3 checkpoint=<cp-v3>

Correctness (see :mod:`iceberg_streaming.cdc._sql`): the dedup orders by the source sequence ``seq``
and the MERGE guards updates/deletes with ``c.seq >= a.seq`` so stale, out-of-order changes can never
overwrite newer state.

Run environment and catalog are configured with :class:`iceberg_streaming.common.JobConfig`
``key=value`` arguments.
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import functions as F

from iceberg_streaming.cdc._sql import mirror_merge
from iceberg_streaming.common import DATABASE, JobConfig
from iceberg_streaming.common.observability import attach_progress_listener

log = logging.getLogger("iceberg_streaming.cdc.spark_streaming_cdc_mirror")

_TOPIC = "streaming-cdc-log-ingest"
# DMS-like CSV: operation,account_id,balance,last_updated(epoch millis),seq
_SCHEMA = ["operation", "account_id", "balance", "last_updated", "seq"]

_DEFAULT_TABLE = "accounts_mirror"


def _create_table_sql(fqn: str, format_version: str, manifest_merge: bool, fanout: bool) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {fqn}
              (account_id bigint,
              balance float,
              last_updated timestamp,
              seq bigint            -- last applied source sequence, for stale-change guards
              )
              PARTITIONED BY (bucket(64, account_id))
              TBLPROPERTIES (
                        'table_type'='ICEBERG',
                        'format-version'='{format_version}',
                        'format'='parquet',
                        'write.delete.mode'='merge-on-read',
                        'write.update.mode'='merge-on-read',
                        'write.merge.mode'='merge-on-read',
                        'write.merge.distribution-mode'='hash',
                        'write.parquet.compression-codec'='zstd',
                        'write.spark.fanout.enabled'='{str(fanout).lower()}',
                        'write.metadata.delete-after-commit.enabled'='true',
                        'write.metadata.previous-versions-max'='400',
                        'history.expire.min-snapshots-to-keep'='400',
                        'commit.retry.num-retries'='100',
                        'commit.retry.min-wait-ms'='250',
                        'commit.retry.max-wait-ms'='120000',
                        'commit.manifest-merge.enabled'='{str(manifest_merge).lower()}',
                        'compatibility.snapshot-id-inheritance.enabled'='true' )
    """


def _make_process_batch(merge_sql: str, table: str):
    def process_batch(batch_df, batch_id: int) -> None:
        session = batch_df.sparkSession
        log.warning("[cdc-mirror %s] batch %s", table, batch_id)
        if batch_df.isEmpty():
            return
        batch_df.createOrReplaceTempView("accounts_batch")
        session.sql(merge_sql)

    return process_batch


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkStreamingCDCMirrorMerge")

    table = cfg.table(_DEFAULT_TABLE)
    format_version = cfg.format_version("3")
    manifest_merge = cfg.manifest_merge(True)
    fanout = cfg.fanout(True)
    # foreachBatch's MERGE references the table by catalog.database.table (it does not rely on USE).
    fqn = f"{cfg.catalog_name}.{DATABASE}.{table}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(_create_table_sql(fqn, format_version, manifest_merge, fanout))
    # CREATE TABLE IF NOT EXISTS is a no-op on an existing table, so enforce the requested fanout on a
    # resumed run too (matches the Java job).
    spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ('write.spark.fanout.enabled'='{str(fanout).lower()}')")

    log.warning("Streaming CDC mirror -> table=%s (format-version %s, fanout=%s)", fqn, format_version, fanout)

    df = cfg.kafka_stream(spark, _TOPIC)

    output = df.selectExpr("CAST(value AS STRING) as value")
    split_col = F.split(F.col("value"), ",")
    for i, name in enumerate(_SCHEMA):
        output = output.withColumn(name, split_col.getItem(i))

    output = (
        output.drop("value")
        .withColumn("account_id", F.col("account_id").cast("bigint"))
        .withColumn("balance", F.col("balance").cast("float"))
        # spark sql does not support epoch millis, so divide by 1000 to get seconds
        .withColumn("last_updated", (F.col("last_updated") / 1000).cast("timestamp"))
        .withColumn("seq", F.col("seq").cast("long"))
    )

    merge_sql = mirror_merge(fqn, "accounts_batch")
    query = (
        output.writeStream.queryName(f"streaming-cdc-mirror-{table}")
        .outputMode("append")
        .foreachBatch(_make_process_batch(merge_sql, table))
        .trigger(processingTime="1 minute")
        .option("fanout-enabled", str(fanout).lower())
        # per-query checkpoint so v2/v3 runs (different table=) never share checkpoint state
        .option("checkpointLocation", cfg.checkpoint_for(f"streaming-cdc-mirror-{table}"))
        .start()
    )
    attach_progress_listener(spark)
    query.awaitTermination()


if __name__ == "__main__":
    main()

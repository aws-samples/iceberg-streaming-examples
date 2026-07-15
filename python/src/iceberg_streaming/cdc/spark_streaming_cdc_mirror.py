"""Streaming ("continuous") CDC mirror MERGE pattern.

PySpark counterpart of ``com.aws.emr.spark.cdc.SparkStreamingCDCMirror``. Where ``cdc-mirror`` runs
a single batch pass over ``accounts_changelog`` and ``cdc-incremental`` processes one snapshot range
per run, this job keeps a long-running structured-streaming query that consumes the DMS-like CDC
feed straight from Kafka (topic ``streaming-cdc-log-ingest``, same CSV format as ``cdc-log-change``)
and, on every micro-batch, deduplicates to the latest change per ``account_id`` and MERGEs it
directly into the mirror table. There is no intermediate changelog table.

**Why this is the deletion-vector workload.** The MERGE is keyed on ``account_id`` alone, so as the
same accounts change again and again the micro-batches overwhelmingly hit ``WHEN MATCHED`` - every
update and delete rewrites an existing row. In merge-on-read that produces a steady stream of
row-level delete files on every commit, which is exactly where Iceberg v3 *deletion vectors* clearly
beat v2 *positional delete files*.

**Comparing v2 and v3 under an identical workload** - the standard knobs, so the same module runs
twice against the same feed::

    cdc-streaming-mirror table=accounts_mirror_v2 fv=2 checkpoint=<cp-v2>
    cdc-streaming-mirror table=accounts_mirror_v3 fv=3 checkpoint=<cp-v3>

Correctness (see :mod:`iceberg_streaming.cdc._sql`): the dedup orders by the source sequence ``seq``
and the MERGE guards updates/deletes with ``c.seq >= a.seq`` so stale, out-of-order changes can never
overwrite newer state.
"""

from __future__ import annotations

import logging
import sys

from iceberg_streaming.cdc import _sql
from iceberg_streaming.cdc.spark_log_change import parse_cdc_csv
from iceberg_streaming.common import DATABASE, JobConfig, Mode
from iceberg_streaming.common.observability import attach_progress_listener

log = logging.getLogger("iceberg_streaming.cdc.spark_streaming_cdc_mirror")

_TOPIC = "streaming-cdc-log-ingest"
_DEFAULT_TABLE = "accounts_mirror"


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
    fanout = cfg.fanout(True)
    # foreachBatch's MERGE references the table by catalog.database.table (it does not rely on USE).
    fqn = f"{cfg.catalog_name}.{DATABASE}.{table}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    # Merge-on-read mirror; fv=/fileformat=/manifestmerge=/fanout= flow in from JobConfig. The
    # overrides keep a wide metadata/snapshot window on this long-running job and very generous
    # commit retries (the streaming writer races S3 Tables managed compaction).
    spark.sql(
        cfg.create_table_ddl(
            table,
            _sql.MIRROR_COLUMNS_DDL,
            "bucket(64, account_id)",
            Mode.MOR,
            {
                "write.metadata.previous-versions-max": "400",
                "history.expire.min-snapshots-to-keep": "400",
                "commit.retry.num-retries": "100",
                "commit.retry.max-wait-ms": "120000",
            },
        )
    )
    # CREATE TABLE IF NOT EXISTS is a no-op on an existing table, so enforce the requested fanout on a
    # resumed run too (matches the Java job).
    spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ('write.spark.fanout.enabled'='{str(fanout).lower()}')")

    log.warning(
        "Streaming CDC mirror -> table=%s (format-version %s, fanout=%s)",
        fqn, cfg.format_version(), fanout,
    )

    # DMS-like CSV CDC feed from Kafka; same typed parse as the changelog writer.
    output = parse_cdc_csv(cfg.kafka_stream(spark, _TOPIC))

    merge_sql = _sql.mirror_merge(fqn, "accounts_batch")

    # Log per-batch throughput/latency before starting so batch 0 is captured too.
    attach_progress_listener(spark)

    query = (
        output.writeStream.queryName(f"streaming-cdc-mirror-{table}")
        .outputMode("append")
        .foreachBatch(_make_process_batch(merge_sql, table))
        .trigger(**cfg.trigger_kwargs(60))
        .option("fanout-enabled", str(fanout).lower())
        # per-query checkpoint so v2/v3 runs (different table=) never share checkpoint state
        .option("checkpointLocation", cfg.checkpoint_for(f"streaming-cdc-mirror-{table}"))
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

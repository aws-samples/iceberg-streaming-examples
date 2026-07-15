"""EV telemetry -> Iceberg with a custom ``foreachBatch`` writer.

PySpark counterpart of ``com.aws.emr.spark.iot.SparkCustomIcebergIngest``: the flexible ingest path
where deduplication, dead-lettering and compaction happen per micro-batch. One module covers the
whole matrix through ``key=value`` knobs: payload format (``source=proto|avro|json``), table layout
(``mode=cow|mor``, ``fv=2|3``, ``fileformat=parquet|orc|avro``, ``objectstorage=``), trigger (fixed
interval or ``trigger=availablenow`` for a catch-up/backfill run) and the strategies below.

Deduplication (``dedup=none|batch|merge``): the event identity is ``(vehicle_id, event_time)`` - see
:mod:`iceberg_streaming.iot._sql` for why the dedup partitions by both columns and how ties break
deterministically on the Kafka offset. ``batch`` drops duplicate identities inside the micro-batch
(one cheap shuffle); ``merge`` additionally suppresses re-deliveries arriving in a later batch via a
MERGE scoped to the recent target partitions (bounded replay suppression, not a global upsert).

Compaction (``compaction=none|inline|scheduled``): both variants compact only the recently
**closed** hourly partitions - never the hot one - and never let a failed maintenance call kill the
ingest query. The recommended production baseline is the standalone ``iceberg-maintenance`` job.

JSON dead-letter: with ``source=json``, records that fail to parse are split into
``<table>_dead_letter`` (raw line + Kafka lineage + rejection time) instead of being dropped. Feed
it with the producer's ``corrupt=true`` knob.
"""

from __future__ import annotations

import logging
import sys
import threading
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from iceberg_streaming.common import DATABASE, Compaction, Dedup, JobConfig, Mode, Source
from iceberg_streaming.common.observability import attach_progress_listener
from iceberg_streaming.iot import _sql, _telemetry

log = logging.getLogger("iceberg_streaming.iot.spark_custom_iceberg_ingest")

_DEAD_LETTER_COLUMNS_DDL = (
    "raw_value string, kafka_partition int, kafka_offset bigint, rejected_at timestamp"
)


def _seconds_to_next_hour() -> float:
    """First run at five past the next full hour, so the previous hourly partition is closed."""
    now = datetime.now()
    nxt = (now + timedelta(hours=1)).replace(minute=5, second=0, microsecond=0)
    return max((nxt - now).total_seconds(), 0.0)


def _schedule_compaction(spark, rewrite_sql: str, manifests_sql: str) -> None:
    def compact() -> None:
        # A failed maintenance call must never stop future runs: guard, log, reschedule.
        try:
            log.warning("Scheduled compaction of closed hourly partitions")
            spark.sql(rewrite_sql).show()
            log.warning("Scheduled manifest rewrite")
            spark.sql(manifests_sql).show()
        except Exception:  # noqa: BLE001
            log.warning("Scheduled maintenance failed (will retry next hour)", exc_info=True)
        finally:
            timer = threading.Timer(60 * 60, compact)
            timer.daemon = True
            timer.start()

    first = threading.Timer(_seconds_to_next_hour(), compact)
    first.daemon = True
    first.start()


def _make_process_batch(
    *,
    json_source: bool,
    dedup: Dedup,
    compaction: Compaction,
    table_fqn: str,
    dead_letter_fqn: str,
    merge_sql: str,
    rewrite_sql: str,
    manifests_sql: str,
):
    def process_batch(batch, batch_id: int) -> None:
        session = batch.sparkSession
        log.warning("Writing batch %s", batch_id)
        # Skip empty micro-batches: no data to write and no reason to compact on an idle trigger.
        if batch.isEmpty():
            log.warning("Batch %s is empty, skipping", batch_id)
            return

        data = batch
        if json_source:
            # The batch is used twice (dead-letter split + ingest): cache it so Kafka is read once.
            batch.persist()
            bad = batch.filter(F.col("vehicle_id").isNull())
            (
                bad.select(
                    F.col("raw_value"),
                    F.col("kafka_partition"),
                    F.col("kafka_offset"),
                    F.current_timestamp().alias("rejected_at"),
                )
                .writeTo(dead_letter_fqn)
                .append()
            )
            data = batch.filter(F.col("vehicle_id").isNotNull()).drop("raw_value")
        try:
            if dedup is Dedup.BATCH:
                # Exact duplicates of the event identity collapse inside this batch; duplicates that
                # split across batches survive (use dedup=merge to also suppress those).
                data.dropDuplicates(["vehicle_id", "event_time"]).writeTo(table_fqn).append()
            elif dedup is Dedup.MERGE:
                data.createOrReplaceTempView("telemetry_batch")
                session.sql(merge_sql)
            else:
                data.writeTo(table_fqn).append()
        finally:
            if json_source:
                batch.unpersist()

        if compaction is Compaction.INLINE:
            # A failed maintenance call must never kill the ingest query: log and move on.
            try:
                if batch_id > 0 and batch_id % 10 == 0:
                    log.warning("Inline compaction of closed hourly partitions (batch %s)", batch_id)
                    session.sql(rewrite_sql).show()
                if batch_id > 0 and batch_id % 30 == 0:
                    log.warning("Inline manifest rewrite (batch %s)", batch_id)
                    session.sql(manifests_sql).show()
            except Exception:  # noqa: BLE001
                log.warning("Inline maintenance failed on batch %s (ingest continues)", batch_id, exc_info=True)

    return process_batch


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkTelemetryCustomIngest")

    table = cfg.table(_telemetry.TABLE)
    table_fqn = f"{cfg.catalog_name}.{DATABASE}.{table}"
    dead_letter_fqn = f"{table_fqn}_dead_letter"
    json_source = cfg.source() is Source.JSON
    dedup = cfg.dedup(Dedup.NONE)
    compaction = cfg.compaction_mode(Compaction.NONE)
    if dedup is Dedup.WATERMARK:
        raise ValueError("dedup=watermark belongs to iot-native-ingest; this job supports none|batch|merge.")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(cfg.create_table_ddl(table, _telemetry.COLUMNS_DDL, _telemetry.PARTITION_DDL, Mode.COW))
    if json_source:
        spark.sql(
            cfg.create_table_ddl(
                f"{table}_dead_letter", _DEAD_LETTER_COLUMNS_DDL, "days(rejected_at)", Mode.COW
            )
        )

    raw = cfg.kafka_stream(spark, cfg.topic())
    # For JSON the stream keeps the raw line alongside the parsed columns so each batch can split
    # failures into the dead-letter table. For proto/avro the decode is uniform.
    output = _telemetry.decode_json_with_raw(raw) if json_source else _telemetry.decode(raw, cfg)

    process_batch = _make_process_batch(
        json_source=json_source,
        dedup=dedup,
        compaction=compaction,
        table_fqn=table_fqn,
        dead_letter_fqn=dead_letter_fqn,
        merge_sql=_sql.replay_suppression_merge(table_fqn, "telemetry_batch"),
        rewrite_sql=_sql.rewrite_closed_hour_data_files(f"{DATABASE}.{table}"),
        manifests_sql=_sql.rewrite_manifests(f"{DATABASE}.{table}"),
    )

    # Attach the progress listener before the query starts so batch 0 is captured too.
    attach_progress_listener(spark)

    query = (
        output.writeStream.queryName(f"custom-ingest-{table}")
        .outputMode("append")
        .foreachBatch(process_batch)
        .trigger(**cfg.trigger_kwargs(60))
        .option("fanout-enabled", str(cfg.fanout(True)).lower())
        .option("checkpointLocation", cfg.checkpoint_for(f"custom-ingest-{table}"))
        .start()
    )

    if compaction is Compaction.SCHEDULED:
        _schedule_compaction(
            spark,
            _sql.rewrite_closed_hour_data_files(f"{DATABASE}.{table}"),
            _sql.rewrite_manifests(f"{DATABASE}.{table}"),
        )

    query.awaitTermination()


if __name__ == "__main__":
    main()

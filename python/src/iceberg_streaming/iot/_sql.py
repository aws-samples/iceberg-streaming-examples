"""Shared SQL templates for the telemetry ingest examples.

Python counterpart of ``com.aws.emr.spark.iot.TelemetrySql``, shared by the ingest jobs so the two
implementations cannot drift and the statements can be unit-tested without a Spark session.

Dedup semantics (bounded replay suppression): the event identity is ``(vehicle_id, event_time)`` - a
device re-sending a reading repeats both. The in-batch dedup therefore partitions by **both** columns
(partitioning by ``vehicle_id`` alone would collapse distinct readings of the same vehicle within the
batch and silently lose data) and breaks ties deterministically by the highest Kafka offset. The
MERGE ON clause restricts the *target* scan to the last two hours so Iceberg prunes to the recent
hourly partitions; a replay older than the window is re-inserted - this is bounded replay
suppression, not a global upsert (see the CDC mirror for keyed upserts).
"""

from __future__ import annotations


def replay_suppression_merge(target_table: str, source_view: str) -> str:
    """Deduplicate-then-MERGE for the telemetry append pipeline.

    :param target_table: target telemetry table (name or fully-qualified ``cat.db.table``)
    :param source_view: temp view holding the current micro-batch (must expose the telemetry
        columns including ``kafka_partition``/``kafka_offset``)
    """
    return f"""
        MERGE INTO {target_table} AS t
        USING (
              SELECT vehicle_id, event_time, model, speed_kmh, soc_pct, odometer_km, charging,
                     kafka_partition, kafka_offset
              FROM (
                  SELECT *, row_number() OVER (
                             PARTITION BY vehicle_id, event_time
                             ORDER BY kafka_offset DESC, kafka_partition DESC) AS row_num
                  FROM {source_view}
              )
              WHERE row_num = 1
        ) AS s
        ON t.vehicle_id = s.vehicle_id AND t.event_time = s.event_time
           AND t.event_time > current_timestamp() - INTERVAL 2 HOURS
        WHEN NOT MATCHED THEN INSERT *
    """


def rewrite_closed_hour_data_files(table: str) -> str:
    """Compact the recently **closed** hourly partitions - never the hot partition being written
    (compacting it maximises optimistic-commit conflicts with the streaming writer). Partial
    progress keeps a losing commit from throwing away the whole rewrite."""
    return f"""
        CALL system.rewrite_data_files(
          table => '{table}',
          strategy => 'sort',
          sort_order => 'event_time',
          where => 'event_time >= current_timestamp() - INTERVAL 3 HOURS
                    AND event_time < date_trunc(''hour'', current_timestamp())',
          options => map(
            'rewrite-job-order','bytes-asc',
            'target-file-size-bytes','536870912',
            'max-file-group-size-bytes','10737418240',
            'partial-progress.enabled', 'true',
            'partial-progress.max-commits', '10',
            'max-concurrent-file-group-rewrites', '1000'
          ))
    """


def rewrite_manifests(table: str) -> str:
    """Rebalance the manifest list (metadata only; still commits, so run it less often)."""
    return f"CALL system.rewrite_manifests(table => '{table}')"

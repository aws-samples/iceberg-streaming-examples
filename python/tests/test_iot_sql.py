"""Invariant tests for the shared telemetry ingest SQL (parity with the Java TelemetrySqlTest).

These guard the dedup-key fix the review called out: the in-batch dedup must partition by the full
event identity ``(vehicle_id, event_time)`` - partitioning by ``vehicle_id`` alone collapses distinct
readings and silently loses data - and ties must break deterministically on the Kafka offset.
Pure-Python: no Spark session.
"""

from __future__ import annotations

import re

from iceberg_streaming.iot._sql import (
    replay_suppression_merge,
    rewrite_closed_hour_data_files,
)

MERGE = re.sub(r"\s+", " ", replay_suppression_merge("bigdata.vehicle_telemetry", "telemetry_batch")).strip()
REWRITE = re.sub(r"\s+", " ", rewrite_closed_hour_data_files("bigdata.vehicle_telemetry")).strip()


def test_target_and_source_interpolated():
    assert "MERGE INTO bigdata.vehicle_telemetry AS t" in MERGE
    assert "FROM telemetry_batch" in MERGE


def test_dedup_partitions_by_the_full_event_identity():
    assert "PARTITION BY vehicle_id, event_time" in MERGE


def test_ties_break_deterministically_on_kafka_offset():
    assert "ORDER BY kafka_offset DESC" in MERGE
    assert "ORDER BY event_time" not in MERGE


def test_merge_is_insert_only_replay_suppression():
    assert "WHEN NOT MATCHED THEN INSERT *" in MERGE
    assert "WHEN MATCHED" not in MERGE


def test_on_clause_prunes_the_recent_target_window():
    assert "t.event_time > current_timestamp() - INTERVAL 2 HOURS" in MERGE
    assert "t.vehicle_id = s.vehicle_id AND t.event_time = s.event_time" in MERGE


def test_compaction_never_touches_the_hot_partition():
    assert "event_time < date_trunc(''hour'', current_timestamp())" in REWRITE
    assert "partial-progress.enabled" in REWRITE

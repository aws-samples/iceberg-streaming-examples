package com.aws.emr.spark.iot;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Invariant tests for the shared telemetry ingest SQL. These guard the dedup-key fix the review
 * called out: the in-batch dedup must partition by the full event identity
 * {@code (vehicle_id, event_time)} - partitioning by {@code vehicle_id} alone collapses distinct
 * readings and silently loses data - and ties must break deterministically on the Kafka offset.
 * They run without a Spark session.
 */
class TelemetrySqlTest {

  private final String merge =
      TelemetrySql.replaySuppressionMerge("bigdata.vehicle_telemetry", "telemetry_batch")
          .replaceAll("\\s+", " ");

  @Test
  void targetAndSourceAreInterpolated() {
    assertTrue(merge.contains("MERGE INTO bigdata.vehicle_telemetry AS t"));
    assertTrue(merge.contains("FROM telemetry_batch"));
  }

  @Test
  void dedupPartitionsByTheFullEventIdentity() {
    assertTrue(
        merge.contains("PARTITION BY vehicle_id, event_time"),
        "dedup must partition by (vehicle_id, event_time); vehicle_id alone drops distinct events");
  }

  @Test
  void tiesBreakDeterministicallyOnKafkaOffset() {
    assertTrue(merge.contains("ORDER BY kafka_offset DESC"));
    assertFalse(merge.contains("ORDER BY event_time"), "event_time ties would be nondeterministic");
  }

  @Test
  void mergeIsInsertOnlyReplaySuppression() {
    assertTrue(merge.contains("WHEN NOT MATCHED THEN INSERT *"));
    assertFalse(merge.contains("WHEN MATCHED"), "the append pipeline must never update rows");
  }

  @Test
  void onClausePrunesTheRecentTargetWindow() {
    assertTrue(merge.contains("t.event_time > current_timestamp() - INTERVAL 2 HOURS"));
    assertTrue(merge.contains("t.vehicle_id = s.vehicle_id AND t.event_time = s.event_time"));
  }

  @Test
  void compactionNeverTouchesTheHotPartition() {
    String rewrite =
        TelemetrySql.rewriteClosedHourDataFiles("bigdata.vehicle_telemetry").replaceAll("\\s+", " ");
    assertTrue(
        rewrite.contains("event_time < date_trunc(''hour'', current_timestamp())"),
        "must exclude the hour currently being written");
    assertTrue(rewrite.contains("partial-progress.enabled"));
  }
}

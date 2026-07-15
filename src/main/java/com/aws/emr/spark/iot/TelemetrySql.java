package com.aws.emr.spark.iot;

/**
 * Centralised SQL templates for the telemetry ingest examples, shared by the Java jobs and mirrored
 * in Python ({@code iceberg_streaming.iot._sql}) so the two implementations cannot drift and the
 * statements can be unit-tested without a Spark session.
 *
 * <h2>Dedup semantics (bounded replay suppression)</h2>
 *
 * <p>The event identity is {@code (vehicle_id, event_time)}: a device re-sending a reading repeats
 * both. The in-batch dedup therefore partitions by <b>both</b> columns - partitioning by
 * {@code vehicle_id} alone would collapse distinct readings of the same vehicle within the batch
 * and silently lose data. Ties (two non-identical rows claiming the same identity) are resolved
 * deterministically by the highest Kafka offset.
 *
 * <p>The MERGE ON clause additionally restricts the <em>target</em> scan to the last two hours so
 * Iceberg prunes to the recent hourly partitions instead of scanning the whole table; that window
 * covers the producer's 1-hour late events. A replay older than the window is re-inserted - this is
 * bounded replay suppression, not a global upsert (see the CDC mirror for keyed upserts).
 */
public final class TelemetrySql {

  private TelemetrySql() {}

  /**
   * Deduplicate-then-MERGE for the telemetry append pipeline: suppress re-deliveries of the same
   * {@code (vehicle_id, event_time)} event within the recent target window, insert everything else.
   *
   * @param targetTable the target telemetry table (name or fully-qualified {@code cat.db.table})
   * @param sourceView the temp view holding the current micro-batch (must expose the telemetry
   *     columns including {@code kafka_partition}/{@code kafka_offset})
   */
  public static String replaySuppressionMerge(String targetTable, String sourceView) {
    return String.format(
        """
        MERGE INTO %1$s AS t
        USING (
              SELECT vehicle_id, event_time, model, speed_kmh, soc_pct, odometer_km, charging,
                     kafka_partition, kafka_offset
              FROM (
                  SELECT *, row_number() OVER (
                             PARTITION BY vehicle_id, event_time
                             ORDER BY kafka_offset DESC, kafka_partition DESC) AS row_num
                  FROM %2$s
              )
              WHERE row_num = 1
        ) AS s
        ON t.vehicle_id = s.vehicle_id AND t.event_time = s.event_time
           AND t.event_time > current_timestamp() - INTERVAL 2 HOURS
        WHEN NOT MATCHED THEN INSERT *
        """,
        targetTable, sourceView);
  }

  /**
   * Compact the recently <b>closed</b> hourly partitions: everything newer than 3 hours ago but
   * strictly before the top of the current hour. The hot partition being written right now is
   * excluded on purpose - compacting it maximises optimistic-commit conflicts with the streaming
   * writer. Partial progress keeps a losing commit from throwing away the whole rewrite.
   */
  public static String rewriteClosedHourDataFiles(String table) {
    return String.format(
        """
        CALL system.rewrite_data_files(
          table => '%1$s',
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
        """,
        table);
  }

  /** Rebalance the manifest list (metadata only; still commits, so run it less often than data compaction). */
  public static String rewriteManifests(String table) {
    return String.format("CALL system.rewrite_manifests(table => '%s')", table);
  }
}

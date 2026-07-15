package com.aws.emr.spark.iot;

import com.aws.emr.common.JobConfig;
import com.aws.emr.common.StreamingProgressListener;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * Latest-vehicle-state upsert that makes the MERGE benefit from <b>Storage-Partitioned Joins
 * (SPJ)</b>: instead of appending every reading, this job maintains one row per vehicle
 * ({@code vehicle_state_spj}) - "where is every vehicle right now" - which makes the MERGE
 * update-heavy, exactly the join SPJ accelerates.
 *
 * <p>SPJ removes the join exchange only when <em>both</em> sides of the join expose the same storage
 * partitioning on the join key. A Kafka-derived micro-batch has no Iceberg bucketing to report, so a
 * direct {@code MERGE INTO target USING (kafka batch)} can never be storage-partitioned. The
 * workaround, per micro-batch:
 *
 * <ol>
 *   <li>Reduce the batch to the latest reading per {@code vehicle_id}. Note the dedup key: for this
 *       <em>upsert</em> the key is the vehicle alone (latest state wins), unlike the append pipeline
 *       where the identity is {@code (vehicle_id, event_time)}. Ties break on Kafka offset.
 *   <li>Overwrite a staging table bucketed by {@code bucket(16, vehicle_id)} - the exact transform
 *       of the target - so both sides report the same {@code KeyGroupedPartitioning}.
 *   <li>Upsert-MERGE staging into the target; with the SPJ planner flags below the join needs no
 *       exchange (check the SQL tab: there should be no Exchange on the join). The MATCHED branch is
 *       guarded by {@code s.event_time >= t.event_time} so a stale batch can never regress a
 *       vehicle's state.
 * </ol>
 *
 * <p>Both tables are partitioned <b>only</b> by the bucket transform (no hours/model dimensions) so
 * the reported partitioning lines up exactly on the join key. Table knobs ({@code fv=},
 * {@code fileformat=}, ...) come from {@link JobConfig}; the target defaults to merge-on-read.
 *
 * @author acmanjon@amazon.com
 */
public class SparkIcebergIngestSpj {

  private static final Logger log = LogManager.getLogger(SparkIcebergIngestSpj.class);

  private static final String TARGET = "vehicle_state_spj";
  private static final String STAGE = "vehicle_state_spj_stage";
  private static final String SPJ_PARTITION_DDL = "bucket(16, vehicle_id)";

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("TelemetryIcebergIngestSpj");

    // --- Storage-Partitioned Join planner flags --------------------------------------------------
    // v2 bucketing must be on, Iceberg must preserve its data grouping when planning the scan, and
    // SPJ is allowed even when only a subset of the clustering keys (the bucket) matches the join.
    spark.conf().set("spark.sql.sources.v2.bucketing.enabled", "true");
    spark.conf().set("spark.sql.iceberg.planning.preserve-data-grouping", "true");
    spark.conf().set("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true");
    spark.conf().set("spark.sql.requireAllClusterKeysForCoPartition", "false");
    spark.conf().set("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled", "true");

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);

    // Target: one row per vehicle, merge-on-read by default so the update-heavy MERGE writes
    // row-level deletes (deletion vectors on v3). merge distribution stays hash so the write is
    // clustered by bucket - SPJ removes the *join* exchange, the *write* still needs clustering.
    spark.sql(
        cfg.createTableDdl(
            TARGET, Telemetry.COLUMNS_DDL, SPJ_PARTITION_DDL, JobConfig.Mode.MOR, Map.of()));

    // Staging: refreshed every micro-batch, hash-distributed so files group by bucket.
    spark.sql(
        cfg.createTableDdl(
            STAGE,
            Telemetry.COLUMNS_DDL,
            SPJ_PARTITION_DDL,
            JobConfig.Mode.COW,
            Map.of(
                "write.distribution-mode", "hash",
                "write.metadata.previous-versions-max", "10")));

    Dataset<Row> output = Telemetry.decode(cfg.kafkaStream(spark, cfg.topic()), cfg);

    final String stageFqn = cfg.catalogName() + "." + JobConfig.DATABASE + "." + STAGE;
    final String targetFqn = cfg.catalogName() + "." + JobConfig.DATABASE + "." + TARGET;

    VoidFunction2<Dataset<Row>, Long> processBatch =
        (batch, batchId) -> {
          SparkSession session = batch.sparkSession();
          log.warn("Writing batch {}", batchId);
          if (batch.isEmpty()) {
            log.warn("Batch {} is empty, skipping", batchId);
            return;
          }

          // 1) Latest reading per vehicle (upsert key = vehicle_id; ties break on Kafka offset).
          batch.createOrReplaceTempView("telemetry_spj_batch");
          Dataset<Row> latest =
              session.sql(
                  """
                  SELECT vehicle_id, event_time, model, speed_kmh, soc_pct, odometer_km, charging,
                         kafka_partition, kafka_offset
                  FROM (
                      SELECT *, row_number() OVER (
                                 PARTITION BY vehicle_id
                                 ORDER BY event_time DESC, kafka_offset DESC) AS row_num
                      FROM telemetry_spj_batch
                  )
                  WHERE row_num = 1
                  """);

          // 2) Swap the staging table to this batch. overwritePartitions() replaces the buckets
          //    present in the batch; a batch normally spans all buckets, refreshing the whole stage.
          latest.writeTo(stageFqn).overwritePartitions();

          // 3) Bucket-aligned upsert MERGE: storage-partitioned join, no exchange. Guarded so a
          //    stale staging row never regresses newer state.
          session.sql(
              String.format(
                  """
                  MERGE INTO %1$s t
                  USING %2$s s
                  ON t.vehicle_id = s.vehicle_id
                  WHEN MATCHED AND s.event_time >= t.event_time THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *
                  """,
                  targetFqn, stageFqn));
        };

    StreamingProgressListener.attach(spark);

    StreamingQuery query =
        output
            .writeStream()
            .queryName("spj-ingest-" + TARGET)
            .outputMode("append")
            .foreachBatch(processBatch)
            .trigger(cfg.trigger(60))
            .option("checkpointLocation", cfg.checkpointFor("spj-ingest-" + TARGET))
            .start();

    query.awaitTermination();
  }
}

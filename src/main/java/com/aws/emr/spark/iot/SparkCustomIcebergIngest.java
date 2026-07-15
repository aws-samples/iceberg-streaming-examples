package com.aws.emr.spark.iot;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;

import com.aws.emr.common.JobConfig;
import com.aws.emr.common.StreamingProgressListener;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
 * EV telemetry -&gt; Iceberg with a custom {@code foreachBatch} writer: the flexible ingest path where
 * deduplication, dead-lettering and compaction happen per micro-batch.
 *
 * <p>One class covers the whole matrix through {@link JobConfig} {@code key=value} knobs: payload
 * format ({@code source=proto|avro|json}), table layout ({@code mode=cow|mor}, {@code fv=2|3},
 * {@code fileformat=parquet|orc|avro}, {@code objectstorage=}), trigger (fixed interval or
 * {@code trigger=availablenow} for a catch-up/backfill run) and the two strategies below.
 *
 * <h2>Deduplication ({@code dedup=none|batch|merge})</h2>
 *
 * The event identity is {@code (vehicle_id, event_time)} - a device re-sending a reading repeats
 * both. See {@link TelemetrySql} for why the dedup partitions by both columns and how ties are
 * broken deterministically by Kafka offset.
 *
 * <ul>
 *   <li>{@code none} - append the raw batch.
 *   <li>{@code batch} - drop duplicate identities inside the micro-batch (one cheap shuffle, no
 *       target scan). Catches the common case: producer re-sends land in the same batch.
 *   <li>{@code merge} - batch dedup <i>plus</i> a MERGE INTO scoped to the recent target partitions,
 *       so a re-delivery arriving in a <i>later</i> batch is suppressed too. This is bounded replay
 *       suppression, not a global upsert (see the CDC mirror for that).
 * </ul>
 *
 * <h2>Compaction ({@code compaction=none|inline|scheduled})</h2>
 *
 * Both variants compact only the recently <b>closed</b> hourly partitions - never the hot partition
 * being written - and never let a failed maintenance call kill the ingest query. {@code inline} runs
 * inside {@code foreachBatch} every 10 batches (manifests every 30) which is simple but lengthens
 * those batches; {@code scheduled} runs hourly on a background thread. Both compete with the writer
 * for commits, which is why {@link JobConfig#tablePropertiesMap} configures generous commit retries;
 * the recommended production baseline is the standalone {@code IcebergMaintenance} job.
 *
 * <h2>JSON dead-letter</h2>
 *
 * With {@code source=json}, records that fail to parse are not dropped: each micro-batch splits the
 * failures into {@code <table>_dead_letter} (raw line + Kafka lineage + rejection time) and ingests
 * the rest. Feed it with the producer's {@code corrupt=true} knob.
 *
 * @author acmanjon@amazon.com
 */
public class SparkCustomIcebergIngest {

  private static final Logger log = LogManager.getLogger(SparkCustomIcebergIngest.class);

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("TelemetryCustomIcebergIngest");

    final String table = cfg.table(Telemetry.TABLE);
    final String tableFqn = cfg.catalogName() + "." + JobConfig.DATABASE + "." + table;
    final String deadLetterFqn = tableFqn + "_dead_letter";
    final boolean json = cfg.source() == JobConfig.Source.JSON;
    final JobConfig.Dedup dedup = cfg.dedup(JobConfig.Dedup.NONE);
    final JobConfig.Compaction compaction = cfg.compactionMode(JobConfig.Compaction.NONE);
    if (dedup == JobConfig.Dedup.WATERMARK) {
      throw new IllegalArgumentException(
          "dedup=watermark belongs to SparkNativeIcebergIngest; this job supports none|batch|merge.");
    }

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    spark.sql(
        cfg.createTableDdl(
            table, Telemetry.COLUMNS_DDL, Telemetry.PARTITION_DDL, JobConfig.Mode.COW, Map.of()));
    if (json) {
      // Dead-letter table for unparseable JSON lines: the raw value plus enough lineage to trace the
      // record back to its Kafka coordinates.
      spark.sql(
          cfg.createTableDdl(
              table + "_dead_letter",
              "raw_value string, kafka_partition int, kafka_offset bigint, rejected_at timestamp",
              "days(rejected_at)",
              JobConfig.Mode.COW,
              Map.of()));
    }

    Dataset<Row> raw = cfg.kafkaStream(spark, cfg.topic());
    // For JSON the stream keeps the raw line alongside the parsed columns so each batch can split
    // failures into the dead-letter table. For proto/avro the decode is uniform.
    Dataset<Row> output = json ? Telemetry.decodeJsonWithRaw(raw) : Telemetry.decode(raw, cfg);

    final String mergeSql = TelemetrySql.replaySuppressionMerge(tableFqn, "telemetry_batch");
    final String rewriteSql =
        TelemetrySql.rewriteClosedHourDataFiles(JobConfig.DATABASE + "." + table);
    final String manifestsSql = TelemetrySql.rewriteManifests(JobConfig.DATABASE + "." + table);

    VoidFunction2<Dataset<Row>, Long> processBatch =
        (batch, batchId) -> {
          SparkSession session = batch.sparkSession();
          log.warn("Writing batch {}", batchId);
          // Skip empty micro-batches: no data to write and no reason to compact on an idle trigger.
          if (batch.isEmpty()) {
            log.warn("Batch {} is empty, skipping", batchId);
            return;
          }

          Dataset<Row> data = batch;
          if (json) {
            // The batch is used twice (dead-letter split + ingest): cache it so Kafka is read once.
            batch.persist();
            Dataset<Row> bad = batch.filter(col("vehicle_id").isNull());
            bad.select(
                    col("raw_value"),
                    col("kafka_partition"),
                    col("kafka_offset"),
                    current_timestamp().as("rejected_at"))
                .writeTo(deadLetterFqn)
                .append();
            data = batch.filter(col("vehicle_id").isNotNull()).drop("raw_value");
          }
          try {
            switch (dedup) {
              case BATCH:
                // Exact duplicates of the event identity collapse inside this batch; duplicates that
                // split across batches survive (use dedup=merge to also suppress those).
                data.dropDuplicates("vehicle_id", "event_time").writeTo(tableFqn).append();
                break;
              case MERGE:
                data.createOrReplaceTempView("telemetry_batch");
                session.sql(mergeSql);
                break;
              case NONE:
              default:
                data.writeTo(tableFqn).append();
            }
          } finally {
            if (json) {
              batch.unpersist();
            }
          }

          if (compaction == JobConfig.Compaction.INLINE) {
            // A failed maintenance call must never kill the ingest query: log and move on. The next
            // eligible batch simply tries again.
            try {
              if (batchId > 0 && batchId % 10 == 0) {
                log.warn("Inline compaction of closed hourly partitions (batch {})", batchId);
                session.sql(rewriteSql).show();
              }
              if (batchId > 0 && batchId % 30 == 0) {
                log.warn("Inline manifest rewrite (batch {})", batchId);
                session.sql(manifestsSql).show();
              }
            } catch (Exception e) {
              log.warn("Inline maintenance failed on batch {} (ingest continues)", batchId, e);
            }
          }
        };

    // Attach the progress listener before the query starts so batch 0 is captured too.
    StreamingProgressListener.attach(spark);

    StreamingQuery query =
        output
            .writeStream()
            .queryName("custom-ingest-" + table)
            .outputMode("append")
            .foreachBatch(processBatch)
            .trigger(cfg.trigger(60))
            .option("fanout-enabled", Boolean.toString(cfg.fanout(true)))
            .option("checkpointLocation", cfg.checkpointFor("custom-ingest-" + table))
            .start();

    if (compaction == JobConfig.Compaction.SCHEDULED) {
      ScheduledExecutorService scheduler =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread t = new Thread(r, "telemetry-compaction");
                t.setDaemon(true);
                return t;
              });
      scheduler.scheduleAtFixedRate(
          () -> {
            // scheduleAtFixedRate cancels the schedule if the task throws: guard everything so one
            // commit conflict or throttle never silently stops all future compactions.
            try {
              log.warn("Scheduled compaction of closed hourly partitions");
              spark.sql(rewriteSql).show();
              log.warn("Scheduled manifest rewrite");
              spark.sql(manifestsSql).show();
            } catch (Exception e) {
              log.warn("Scheduled maintenance failed (will retry next hour)", e);
            }
          },
          millisToNextHour(),
          60 * 60 * 1000L,
          TimeUnit.MILLISECONDS);
    }

    query.awaitTermination();
  }

  /** First run at five past the next full hour, so the previous hourly partition is closed. */
  private static long millisToNextHour() {
    LocalDateTime nextHour =
        LocalDateTime.now().plusHours(1).truncatedTo(ChronoUnit.HOURS).plusMinutes(5);
    return LocalDateTime.now().until(nextHour, ChronoUnit.MILLIS);
  }
}

package com.aws.emr.spark.iot;

import com.aws.emr.common.JobConfig;
import com.aws.emr.common.StreamingProgressListener;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * EV telemetry -&gt; Iceberg with the <b>native</b> Spark/Iceberg streaming writer ({@code toTable}):
 * the simplest, lowest-overhead ingest path - one streaming query, no {@code foreachBatch}, appends
 * only.
 *
 * <p>One class covers what used to be several: the Kafka payload format is selected with
 * {@code source=proto|avro|json} and the table layout with {@code mode=}, {@code fv=},
 * {@code fileformat=} and {@code objectstorage=} (see {@link JobConfig#usage()}). Corrupt JSON
 * records are dropped on this path; use {@code SparkCustomIcebergIngest} when you need them captured
 * in a dead-letter table.
 *
 * <h2>Deduplication ({@code dedup=watermark})</h2>
 *
 * The native writer cannot run a MERGE, so its dedup option is the stateful
 * {@code dropDuplicatesWithinWatermark} on the event identity {@code (vehicle_id, event_time)}.
 * Understand the trade-off before enabling it:
 *
 * <ul>
 *   <li>State is bounded by the watermark delay ({@code watermark=}, default 120 seconds), so only
 *       duplicates arriving within that delay of each other are caught.
 *   <li>Events <b>older than the watermark are dropped entirely</b>, not deduplicated. The demo
 *       producer emits 0.1% one-hour-late readings: with the default 120s watermark those late
 *       events are silently discarded on this path. Either widen {@code watermark=} past your
 *       late-arrival window (at the cost of more state) or use the MERGE-based dedup of
 *       {@code SparkCustomIcebergIngest}, which handles late replays without dropping late data.
 * </ul>
 *
 * @author acmanjon@amazon.com
 */
public class SparkNativeIcebergIngest {

  private static final Logger log = LogManager.getLogger(SparkNativeIcebergIngest.class);

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("TelemetryNativeIcebergIngest");

    final String table = cfg.table(Telemetry.TABLE);
    final JobConfig.Dedup dedup = cfg.dedup(JobConfig.Dedup.NONE);
    if (dedup != JobConfig.Dedup.NONE && dedup != JobConfig.Dedup.WATERMARK) {
      throw new IllegalArgumentException(
          "The native writer supports dedup=none or dedup=watermark; use SparkCustomIcebergIngest"
              + " for dedup=batch|merge.");
    }

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    spark.sql(
        cfg.createTableDdl(
            table, Telemetry.COLUMNS_DDL, Telemetry.PARTITION_DDL, JobConfig.Mode.COW, Map.of()));

    Dataset<Row> output = Telemetry.decode(cfg.kafkaStream(spark, cfg.topic()), cfg);

    if (dedup == JobConfig.Dedup.WATERMARK) {
      log.warn(
          "Watermark dedup enabled (delay={}): duplicates within the delay are dropped, and so are"
              + " events older than the watermark - late data beyond {} is DISCARDED on this path.",
          cfg.watermarkDelay(), cfg.watermarkDelay());
      output =
          output
              .withWatermark("event_time", cfg.watermarkDelay())
              .dropDuplicatesWithinWatermark("vehicle_id", "event_time");
    }

    // Attach the progress listener before the query starts so batch 0 is captured too.
    StreamingProgressListener.attach(spark);

    StreamingQuery query =
        output
            .writeStream()
            .queryName("native-ingest-" + table)
            .format("iceberg")
            .outputMode("append")
            .trigger(cfg.trigger(60))
            .option("fanout-enabled", Boolean.toString(cfg.fanout(true)))
            .option("checkpointLocation", cfg.checkpointFor("native-ingest-" + table))
            .toTable(table);
    query.awaitTermination();
  }
}

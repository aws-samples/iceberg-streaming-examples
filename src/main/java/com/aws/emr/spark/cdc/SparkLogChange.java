package com.aws.emr.spark.cdc;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.timestamp_millis;

import com.aws.emr.common.JobConfig;
import com.aws.emr.common.StreamingProgressListener;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * CDC changelog writer: consumes the DMS-like CSV feed from Kafka (see
 * {@code com.aws.emr.kafka.KafkaCDCSimulator}) and appends it to the {@code accounts_changelog}
 * Iceberg table - the raw, replayable history the mirror pipelines
 * ({@link SparkCDCMirror}, {@link SparkIncrementalPipeline}) consume.
 *
 * <p>{@code balance} is in minor units (cents) and stays {@code bigint} end to end - money never
 * touches a float. The wire timestamp is epoch millis, converted with {@code timestamp_millis}.
 *
 * <h2>Deduplication ({@code dedup=none|batch})</h2>
 *
 * The producer's {@code seq} is unique per source change, so a duplicate delivery (producer retry,
 * replayed offset range) repeats the same {@code seq}. Duplicates in the changelog are harmless to
 * the mirror (its windowed dedup picks one row per key anyway) but they pollute a table analysts
 * query directly. {@code dedup=batch} switches the writer to {@code foreachBatch} and drops repeated
 * {@code seq} values inside each micro-batch - one cheap shuffle, no state. Cross-batch duplicates
 * still land; the guarded mirror MERGE absorbs them.
 *
 * @author acmanjon@amazon.com
 */
public class SparkLogChange {

  private static final Logger log = LogManager.getLogger(SparkLogChange.class);

  static final String TOPIC = "streaming-cdc-log-ingest";
  static final String TABLE = "accounts_changelog";

  static final String COLUMNS_DDL =
      """
      operation string,
                account_id bigint,
                balance bigint,
                last_updated timestamp,
                seq bigint""";
  static final String PARTITION_DDL = "days(last_updated), bucket(8, account_id)";

  // DMS-like CSV: operation,account_id,balance,last_updated(epoch millis),seq
  private static final List<String> CSV_SCHEMA =
      Arrays.asList("operation", "account_id", "balance", "last_updated", "seq");

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("CDCLogChangeWriter");

    final JobConfig.Dedup dedup = cfg.dedup(JobConfig.Dedup.NONE);
    if (dedup != JobConfig.Dedup.NONE && dedup != JobConfig.Dedup.BATCH) {
      throw new IllegalArgumentException(
          "The changelog writer supports dedup=none or dedup=batch (it appends history; keyed"
              + " dedup belongs to the mirror jobs).");
    }

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    spark.sql(cfg.createTableDdl(TABLE, COLUMNS_DDL, PARTITION_DDL, JobConfig.Mode.COW, Map.of()));

    Dataset<Row> output = parseCdcCsv(cfg.kafkaStream(spark, TOPIC));
    output.printSchema();

    final String tableFqn = cfg.catalogName() + "." + JobConfig.DATABASE + "." + TABLE;

    StreamingProgressListener.attach(spark);

    org.apache.spark.sql.streaming.DataStreamWriter<Row> writer =
        output
            .writeStream()
            .queryName("cdc-log-change")
            .trigger(cfg.trigger(120))
            .outputMode("append")
            // per-query checkpoint so it never collides with the other streaming examples
            .option("checkpointLocation", cfg.checkpointFor("cdc-log-change"));

    StreamingQuery query;
    if (dedup == JobConfig.Dedup.BATCH) {
      query =
          writer
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>)
                      (batch, batchId) -> {
                        if (batch.isEmpty()) {
                          return;
                        }
                        // seq is unique per source change: repeated seq = duplicate delivery.
                        batch.dropDuplicates("seq").writeTo(tableFqn).append();
                      })
              .start();
    } else {
      query = writer.format("iceberg").toTable(TABLE);
    }

    query.awaitTermination();
  }

  /** Parse the raw Kafka CSV values into typed changelog columns (shared with the streaming mirror). */
  static Dataset<Row> parseCdcCsv(Dataset<Row> kafka) {
    Dataset<Row> parsed = kafka.selectExpr("CAST(value AS STRING) AS value");
    Column linesSplit = split(col("value"), ",");
    for (int i = 0; i < CSV_SCHEMA.size(); i++) {
      parsed = parsed.withColumn(CSV_SCHEMA.get(i), linesSplit.getItem(i));
    }
    return parsed
        .drop(col("value"))
        .withColumn("account_id", col("account_id").cast("bigint"))
        .withColumn("balance", col("balance").cast("bigint")) // minor units; never a float
        .withColumn("last_updated", timestamp_millis(col("last_updated").cast("bigint")))
        .withColumn("seq", col("seq").cast("bigint"));
  }
}

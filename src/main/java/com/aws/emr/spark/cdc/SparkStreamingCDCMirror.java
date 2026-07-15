package com.aws.emr.spark.cdc;

import static org.apache.spark.sql.functions.*;

import com.aws.emr.common.JobConfig;
import com.aws.emr.common.StreamingProgressListener;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Streaming ("continuous") variant of the {@link SparkCDCMirror} MERGE pattern.
 *
 * <p>Where {@link SparkCDCMirror} runs a single batch pass over the {@code accounts_changelog} table
 * and {@link SparkIncrementalPipeline} processes one snapshot range per invocation, this job keeps a
 * long-running structured-streaming query that consumes the DMS-like CDC feed straight from Kafka
 * (topic {@code streaming-cdc-log-ingest}, same CSV format as {@link SparkLogChange}) and, on every
 * micro-batch, deduplicates to the latest change per {@code account_id} and MERGEs it directly into
 * the mirror table. There is no intermediate changelog table.
 *
 * <h2>Why this is the deletion-vector workload</h2>
 *
 * The MERGE is keyed on {@code account_id} alone, so as the same accounts change again and again the
 * micro-batches overwhelmingly hit {@code WHEN MATCHED} — every update and delete rewrites an
 * existing row. In merge-on-read that means a steady stream of row-level delete files on every
 * commit, which is exactly the workload where Iceberg v3 <b>deletion vectors</b> (one compact,
 * mergeable vector per data file) clearly outperform v2 <b>positional delete files</b> (which
 * accumulate and slow reads/compaction). An insert-only MERGE, by contrast, produces pure appends
 * and shows no difference between v2 and v3.
 *
 * <h2>Comparing v2 and v3 under an identical workload</h2>
 *
 * The target table name and Iceberg format version are parameterised so the same class can be
 * launched twice against the same CDC feed:
 *
 * <pre>
 *   table=&lt;name&gt;   target mirror table in the {@link JobConfig#DATABASE} database (default: accounts_mirror)
 *   fv=2|3         Iceberg format-version of the target table (default: 3)
 * </pre>
 *
 * Run one job with {@code table=accounts_mirror_v2 fv=2} and another with
 * {@code table=accounts_mirror_v3 fv=3} (each with its own {@code checkpoint=}), then compare commit
 * latency, delete-file counts and read amplification between the two.
 *
 * <h2>Restricting the merge scope via the ON clause</h2>
 *
 * The target is bucketed by {@code account_id}, so joining on {@code a.account_id = c.account_id}
 * lets Iceberg prune the scan to just the buckets present in the incoming micro-batch instead of the
 * whole table. If your mirror is also time-partitioned, add a predicate on the partition column to
 * the ON clause (see the README notes) so the planner only rewrites the recent partitions.
 *
 * <p>The Spark session, catalog and run environment come from {@link JobConfig} {@code key=value}
 * arguments; see {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkStreamingCDCMirror {

  private static final Logger log = LogManager.getLogger(SparkStreamingCDCMirror.class);

  private static final String TOPIC = "streaming-cdc-log-ingest";
  private static final String DEFAULT_TABLE = "accounts_mirror";
  private static final String DEFAULT_FORMAT_VERSION = "3";
  // DMS-like CSV: operation,account_id,balance,last_updated(epoch millis),seq
  private static final List<String> CSV_SCHEMA =
      Arrays.asList("operation", "account_id", "balance", "last_updated", "seq");

  public static void main(String[] args) throws Exception {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("StreamingCDCMirrorMerge");

    // Example-specific options are now typed accessors on JobConfig (no more re-parsing args here).
    final String table = cfg.table(DEFAULT_TABLE);
    final String formatVersion = cfg.formatVersion(DEFAULT_FORMAT_VERSION);
    // Iceberg's automatic manifest merge-on-commit (default true). Set manifestmerge=false to
    // isolate/measure the cost of synchronous manifest merging under heavy delete churn.
    final String manifestMerge = Boolean.toString(cfg.manifestMerge(true));
    // Spark fanout writers (default true). fanout=false forces a local sort so only one file writer
    // is open at a time, cutting write-side memory under heavy partition fan-out.
    final String fanout = Boolean.toString(cfg.fanout(true));

    // Fully-qualified target name: foreachBatch runs on a cloned session that does NOT inherit the
    // USE statement below, so the MERGE must reference the table by catalog.database.table.
    final String mirrorFqn = cfg.catalogName() + "." + JobConfig.DATABASE + "." + table;

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    spark.sql(
        String.format(
            """
                        CREATE TABLE IF NOT EXISTS %1$s
                              (account_id bigint,
                              balance float,
                              last_updated timestamp,
                              seq bigint            -- last applied source sequence, for stale-change guards
                              )
                              PARTITIONED BY (bucket(64, account_id))
                              TBLPROPERTIES (
                                        'table_type'='ICEBERG',
                                        'format-version'='%2$s',   -- v3 -> deletion vectors; v2 -> positional delete files
                                        'format'='parquet',
                                        'write.delete.mode'='merge-on-read',
                                        'write.update.mode'='merge-on-read',
                                        'write.merge.mode'='merge-on-read',
                                        'write.merge.distribution-mode'='hash',
                                        'write.parquet.compression-codec'='zstd',
                                        'write.spark.fanout.enabled'='%4$s',
                                        -- keep the metadata log bounded on a long-running streaming job,
                                        -- but wide enough to survive until managed compaction kicks in
                                        -- (~3h here) without trimming the snapshots we want to observe
                                        'write.metadata.delete-after-commit.enabled'='true',
                                        'write.metadata.previous-versions-max'='400',
                                        'history.expire.min-snapshots-to-keep'='400',
                                        -- tolerate concurrent commits / retries under continuous load
                                        -- (streaming writer racing S3 Tables managed compaction)
                                        'commit.retry.num-retries'='100',
                                        'commit.retry.min-wait-ms'='250',
                                        'commit.retry.max-wait-ms'='120000',
                                        'commit.manifest-merge.enabled'='%3$s',
                                        'compatibility.snapshot-id-inheritance.enabled'='true' );
                        """,
            table, formatVersion, manifestMerge, fanout));

    // CREATE TABLE IF NOT EXISTS is a no-op when the table already exists, so a resumed run would
    // otherwise keep the previous fanout value. Enforce the requested setting on existing tables too.
    spark.sql(
        String.format(
            "ALTER TABLE %1$s SET TBLPROPERTIES ('write.spark.fanout.enabled'='%2$s')",
            mirrorFqn, fanout));

    log.warn(
        "Streaming CDC mirror -> table={} (Iceberg format-version {}, fanout={})",
        mirrorFqn, formatVersion, fanout);

    // DMS-like CSV CDC feed from Kafka: operation,account_id,balance,last_updated(epoch millis).
    Dataset<Row> df = cfg.kafkaStream(spark, TOPIC);

    Dataset<Row> parsed = df.selectExpr("CAST(value AS STRING) as value");
    Column linesSplit = split(col("value"), ",");
    for (int i = 0; i < CSV_SCHEMA.size(); i++) {
      parsed = parsed.withColumn(CSV_SCHEMA.get(i), linesSplit.getItem(i));
    }
    parsed =
        parsed
            .drop(col("value"))
            .withColumn("account_id", col("account_id").cast("bigint"))
            .withColumn("balance", col("balance").cast("float"))
            // spark sql does not support epoch millis, so divide by 1000 to get seconds
            .withColumn("last_updated", col("last_updated").divide(1000).cast("timestamp"))
            .withColumn("seq", col("seq").cast("long"));

    parsed
        .writeStream()
        .queryName("streaming-cdc-mirror-" + table)
        .outputMode("append")
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>)
                (batch, batchId) -> {
                  SparkSession session = batch.sparkSession();
                  log.warn("[cdc-mirror {}] batch {}", table, batchId);
                  if (batch.isEmpty()) {
                    return;
                  }
                  batch.createOrReplaceTempView("accounts_batch");
                  // Deduplicate to the highest source sequence per account_id within this micro-batch,
                  // then MERGE with c.seq >= a.seq guards (see CdcSql). Keying the ON clause on
                  // account_id (the bucket column) prunes the target scan to only the buckets touched
                  // by this batch. Delete/update-heavy by design -> exercises deletion vectors on v3.
                  session.sql(CdcSql.mirrorMerge(mirrorFqn, "accounts_batch"));
                })
        .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
        .option("fanout-enabled", fanout)
        // per-query checkpoint so v2/v3/no-merge runs (different table=) never share checkpoint state
        .option("checkpointLocation", cfg.checkpointFor("streaming-cdc-mirror-" + table))
        .start();

    // Log per-batch throughput/latency and Iceberg commit metrics to compare v2 vs v3 objectively.
    StreamingProgressListener.attach(spark);

    spark.streams().awaitAnyTermination();
  }
}

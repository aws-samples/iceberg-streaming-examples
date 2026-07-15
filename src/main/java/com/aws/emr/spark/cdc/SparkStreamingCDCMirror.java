package com.aws.emr.spark.cdc;

import com.aws.emr.common.JobConfig;
import com.aws.emr.common.StreamingProgressListener;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
 * micro-batches overwhelmingly hit {@code WHEN MATCHED} - every update and delete rewrites an
 * existing row. In merge-on-read that means a steady stream of row-level delete files on every
 * commit, which is exactly the workload where Iceberg v3 <b>deletion vectors</b> (one compact,
 * mergeable vector per data file) clearly outperform v2 <b>positional delete files</b> (which
 * accumulate and slow reads/compaction). An insert-only MERGE, by contrast, produces pure appends
 * and shows no difference between v2 and v3.
 *
 * <h2>Comparing v2 and v3 under an identical workload</h2>
 *
 * The target table name and Iceberg format version are the usual {@link JobConfig} knobs, so the
 * same class can be launched twice against the same CDC feed:
 *
 * <pre>
 *   table=accounts_mirror_v2 fv=2 checkpoint=&lt;cp-v2&gt;
 *   table=accounts_mirror_v3 fv=3 checkpoint=&lt;cp-v3&gt;
 * </pre>
 *
 * then compare commit latency, delete-file counts and read amplification between the two (the
 * {@code [stream-progress]} listener lines make the comparison objective; for the read side see
 * {@link SparkCDCReadBenchmark}). {@code fanout=false} and {@code manifestmerge=false} isolate the
 * write-memory and manifest-merge effects the README discusses.
 *
 * <h2>Correctness</h2>
 *
 * The dedup-then-MERGE statement is the shared {@link CdcSql#mirrorMerge}: deterministic ordering by
 * the source sequence {@code seq} and {@code c.seq >= a.seq} guards so out-of-order arrivals can
 * never overwrite newer state. The target is bucketed by {@code account_id}, so the ON clause prunes
 * the scan to the buckets present in the micro-batch.
 *
 * @author acmanjon@amazon.com
 */
public class SparkStreamingCDCMirror {

  private static final Logger log = LogManager.getLogger(SparkStreamingCDCMirror.class);

  private static final String TOPIC = "streaming-cdc-log-ingest";
  private static final String DEFAULT_TABLE = "accounts_mirror";

  public static void main(String[] args) throws Exception {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("StreamingCDCMirrorMerge");

    final String table = cfg.table(DEFAULT_TABLE);
    final String formatVersion = cfg.formatVersion("3");
    final boolean fanout = cfg.fanout(true);

    // Fully-qualified target name: foreachBatch runs on a cloned session that does NOT inherit the
    // USE statement below, so the MERGE must reference the table by catalog.database.table.
    final String mirrorFqn = cfg.catalogName() + "." + JobConfig.DATABASE + "." + table;

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    // Merge-on-read mirror; fv=/fileformat=/manifestmerge=/fanout= all flow in from JobConfig. The
    // overrides keep a wide metadata/snapshot window on this long-running job (wide enough to survive
    // until managed compaction kicks in without trimming the snapshots under observation) and very
    // generous commit retries (the streaming writer races S3 Tables managed compaction).
    spark.sql(
        cfg.createTableDdl(
            table,
            CdcSql.MIRROR_COLUMNS_DDL,
            "bucket(64, account_id)",
            JobConfig.Mode.MOR,
            Map.of(
                "write.metadata.previous-versions-max", "400",
                "history.expire.min-snapshots-to-keep", "400",
                "commit.retry.num-retries", "100",
                "commit.retry.max-wait-ms", "120000")));

    // CREATE TABLE IF NOT EXISTS is a no-op when the table already exists, so a resumed run would
    // otherwise keep the previous fanout value. Enforce the requested setting on existing tables too.
    spark.sql(
        String.format(
            "ALTER TABLE %1$s SET TBLPROPERTIES ('write.spark.fanout.enabled'='%2$s')",
            mirrorFqn, fanout));

    log.warn(
        "Streaming CDC mirror -> table={} (Iceberg format-version {}, fanout={})",
        mirrorFqn, formatVersion, fanout);

    // DMS-like CSV CDC feed from Kafka; same typed parse as the changelog writer.
    Dataset<Row> parsed = SparkLogChange.parseCdcCsv(cfg.kafkaStream(spark, TOPIC));

    final String mergeSql = CdcSql.mirrorMerge(mirrorFqn, "accounts_batch");

    // Log per-batch throughput/latency before starting so batch 0 is captured too.
    StreamingProgressListener.attach(spark);

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
                  // Deduplicate to the highest source sequence per account_id within this
                  // micro-batch, then MERGE with c.seq >= a.seq guards (see CdcSql). Delete/update
                  // heavy by design -> exercises deletion vectors on v3.
                  session.sql(mergeSql);
                })
        .trigger(cfg.trigger(60))
        .option("fanout-enabled", Boolean.toString(fanout))
        // per-query checkpoint so v2/v3/no-merge runs (different table=) never share checkpoint state
        .option("checkpointLocation", cfg.checkpointFor("streaming-cdc-mirror-" + table))
        .start();

    spark.streams().awaitAnyTermination();
  }
}

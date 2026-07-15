package com.aws.emr.spark.cdc;

import com.aws.emr.common.JobConfig;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.CommitMetadata;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;

/**
 * An example of the Iceberg incremental processing pattern applied to CDC: instead of re-scanning
 * the whole changelog on every run, we read only the snapshots appended to
 * {@code accounts_changelog} since the last processed snapshot and MERGE them into
 * {@code accounts_mirror}. The last processed source snapshot id is stored as a commit property
 * ({@code watermark:accounts_changelog}) on the target table, so the read range and the merge are
 * committed atomically and the pipeline is restartable.
 *
 * <p>Both tables are Iceberg format-version 3 (v3) tables. The Spark session, catalog and run
 * environment are selected through {@link JobConfig} {@code key=value} arguments; see
 * {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkIncrementalPipeline {

  private static final Logger log = LogManager.getLogger(SparkIncrementalPipeline.class);

  /** Commit property used to track the last processed source snapshot on the target table. */
  private static final String WATERMARK_KEY = "watermark:accounts_changelog";

  public static void main(String[] args)
      throws NoSuchTableException, ParseException, IOException, TimeoutException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("CDCIncrementalPipeline");

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);

    // The source changelog table (populated by SparkLogChange) ...
    spark.sql(
        """
                        CREATE TABLE IF NOT EXISTS accounts_changelog
                              (
                              operation string,
                              account_id bigint,
                              balance bigint,
                              last_updated timestamp,
                              seq bigint            -- source sequence (LSN surrogate) for deterministic ordering
                              )
                              PARTITIONED BY (days(last_updated),bucket(8, account_id))
                              TBLPROPERTIES (
                                        'table_type'='ICEBERG',
                                        'format-version'='3',
                                        'write.parquet.compression-codec'='zstd',
                                        'compatibility.snapshot-id-inheritance.enabled'='true' );
                        """);
    // ... and the target mirror table where the deduplicated, merged state lives.
    spark.sql(
        """
                        CREATE TABLE IF NOT EXISTS accounts_mirror
                              (account_id bigint,
                              balance float,
                              last_updated timestamp,
                              seq bigint            -- last applied source sequence, for stale-change guards
                              )
                              PARTITIONED BY (bucket(8, account_id))
                              TBLPROPERTIES (
                                        'table_type'='ICEBERG',
                                        'format-version'='3',
                                        'write.delete.mode'='merge-on-read',
                                        'write.update.mode'='merge-on-read',
                                        'write.merge.mode'='merge-on-read',
                                        'write.parquet.compression-codec'='zstd',
                                        'compatibility.snapshot-id-inheritance.enabled'='true' );
                        """);

    // Load the target table and recover the last processed source snapshot id from its history.
    Table mirrorTable = Spark3Util.loadIcebergTable(spark, "accounts_mirror");
    String lastProcessedId = null;
    for (Snapshot snap : SnapshotUtil.currentAncestors(mirrorTable)) {
      lastProcessedId = snap.summary().get(WATERMARK_KEY);
      if (lastProcessedId != null) {
        break;
      }
    }

    // Load the source table and figure out the snapshot we want to process up to.
    Table logSourceTable = Spark3Util.loadIcebergTable(spark, "accounts_changelog");
    if (logSourceTable.currentSnapshot() == null) {
      log.warn("Source table accounts_changelog has no snapshots yet, nothing to process.");
      return;
    }
    String toProcessId = Long.toString(logSourceTable.currentSnapshot().snapshotId());

    if (toProcessId.equals(lastProcessedId)) {
      log.warn("No new snapshots in accounts_changelog since {}, nothing to process.", lastProcessedId);
      return;
    }
    log.warn("Last processed source snapshot was {} and we will process up to {}", lastProcessedId, toProcessId);

    // Incremental read of only the new appends. On the very first run (no watermark yet) we read the
    // whole source table up to the current snapshot.
    Dataset<Row> newLogs;
    if (lastProcessedId == null) {
      newLogs = spark.read().format("iceberg").option("end-snapshot-id", toProcessId).table("accounts_changelog");
    } else {
      newLogs =
          spark
              .read()
              .format("iceberg")
              .option("start-snapshot-id", lastProcessedId) // exclusive
              .option("end-snapshot-id", toProcessId)
              .table("accounts_changelog");
    }
    newLogs.createOrReplaceTempView("accounts_source");

    // Update the target table and record the watermark in the same commit. If the merge fails, the
    // watermark is not advanced and the range is safely reprocessed on the next run.
    CommitMetadata.withCommitProperties(
        Map.of(WATERMARK_KEY, toProcessId),
        () -> {
          spark.sql(CdcSql.mirrorMerge("accounts_mirror", "accounts_source"));
          return 0;
        },
        RuntimeException.class);

    log.warn("Incremental merge complete, watermark advanced to {}", toProcessId);

    // Rollback & replay, if you ever need to reprocess a range:
    // CALL system.rollback_to_snapshot('accounts_mirror', <LAST-CORRECT-SNAPSHOT-ID>);
  }
}

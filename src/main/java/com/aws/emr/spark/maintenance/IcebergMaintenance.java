package com.aws.emr.spark.maintenance;

import com.aws.emr.common.JobConfig;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A dedicated, standalone Iceberg table-maintenance driver. This is the <b>recommended baseline</b>
 * for keeping the streaming tables healthy: running maintenance in a separate scheduled job avoids
 * competing with the ingestion writer inside {@code foreachBatch} (which extends batch latency and
 * causes commit conflicts). The in-job maintenance in {@code SparkCustomIcebergIngest} is kept only
 * as a deliberate "maintenance-concurrent" comparison pattern.
 *
 * <p>It bundles the four standard Iceberg maintenance actions behind one entry point:
 *
 * <ul>
 *   <li>{@code rewrite_data_files} &mdash; bin-pack / sort small files, with partial progress so a
 *       conflict only loses the current file group.
 *   <li>{@code rewrite_manifests} &mdash; rebalance the manifest list (metadata only).
 *   <li>{@code expire_snapshots} &mdash; drop old snapshots and their now-unreferenced files.
 *   <li>{@code remove_orphan_files} &mdash; delete files no snapshot references (careful on a live
 *       table &mdash; keep {@code older-than-days} comfortably larger than your longest in-flight
 *       write/compaction).
 * </ul>
 *
 * <h2>Arguments</h2>
 *
 * In addition to the usual {@link JobConfig} {@code key=value} arguments:
 *
 * <pre>
 *   table=&lt;name&gt;            table to maintain, in the {@code bigdata} database (required)
 *   action=all|rewrite_data_files|rewrite_manifests|expire_snapshots|remove_orphan_files
 *                            what to run (default: all)
 *   where=&lt;predicate&gt;       optional filter for rewrite_data_files (e.g.
 *                            "last_updated &gt;= current_timestamp() - INTERVAL 2 DAYS") so only recent
 *                            partitions are compacted
 *   sort-order=&lt;cols&gt;       sort columns for the sort rewrite strategy (default: none -> bin-pack)
 *   older-than-days=&lt;n&gt;     retention horizon for expire_snapshots / remove_orphan_files (default 7)
 *   retain-last=&lt;n&gt;         minimum snapshots to keep on expire_snapshots (default 100)
 *   target-file-size-bytes=&lt;n&gt;  target size for rewrite_data_files (default 512 MiB)
 *   dry-run=true|false       report table stats only, run no mutation (default: false)
 * </pre>
 *
 * @author acmanjon@amazon.com
 */
public final class IcebergMaintenance {

  private static final Logger log = LogManager.getLogger(IcebergMaintenance.class);

  private IcebergMaintenance() {}

  public static void main(String[] args) {
    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("IcebergMaintenance");

    spark.sql("USE " + JobConfig.DATABASE);

    String table = cfg.table(null);
    if (table == null) {
      throw new IllegalArgumentException(
          "IcebergMaintenance requires table=<name>. See JobConfig usage:\n" + JobConfig.usage());
    }
    String action = cfg.arg("action", "all").toLowerCase();
    boolean dryRun = cfg.argBool("dry-run", false);

    log.warn("[maintenance] table={} action={} dryRun={}", table, action, dryRun);
    report(spark, table, "before");

    if (dryRun) {
      log.warn("[maintenance] dry-run: no mutation performed.");
      return;
    }

    boolean all = action.equals("all");
    if (all || action.equals("rewrite_data_files")) {
      rewriteDataFiles(spark, cfg, table);
    }
    if (all || action.equals("rewrite_manifests")) {
      rewriteManifests(spark, table);
    }
    if (all || action.equals("expire_snapshots")) {
      expireSnapshots(spark, cfg, table);
    }
    if (all || action.equals("remove_orphan_files")) {
      removeOrphanFiles(spark, cfg, table);
    }

    report(spark, table, "after");
    log.warn("[maintenance] done for table={}", table);
  }

  private static void rewriteDataFiles(SparkSession spark, JobConfig cfg, String table) {
    String where = cfg.arg("where", null);
    String sortOrder = cfg.arg("sort-order", null);
    long targetSize = Long.parseLong(cfg.arg("target-file-size-bytes", Long.toString(512L * 1024 * 1024)));
    String strategy = sortOrder == null ? "binpack" : "sort";
    StringBuilder call = new StringBuilder("CALL system.rewrite_data_files(table => '" + table + "'");
    call.append(", strategy => '").append(strategy).append("'");
    if (sortOrder != null) {
      call.append(", sort_order => '").append(sortOrder).append("'");
    }
    if (where != null) {
      // single-quotes inside the predicate must be doubled for the SQL string literal
      call.append(", where => '").append(where.replace("'", "''")).append("'");
    }
    call.append(", options => map(")
        .append("'rewrite-job-order','bytes-asc',")
        .append("'target-file-size-bytes','").append(targetSize).append("',")
        .append("'max-file-group-size-bytes','10737418240',")
        .append("'partial-progress.enabled','true',")
        .append("'partial-progress.max-commits','10',")
        .append("'max-concurrent-file-group-rewrites','10000'))");
    log.warn("[maintenance] rewrite_data_files: {}", call);
    Row r = spark.sql(call.toString()).first();
    log.warn(
        "[maintenance] rewrite_data_files result: rewrittenDataFiles={} addedDataFiles={} rewrittenBytes={}",
        safeGet(r, "rewritten_data_files_count"),
        safeGet(r, "added_data_files_count"),
        safeGet(r, "rewritten_bytes_count"));
  }

  private static void rewriteManifests(SparkSession spark, String table) {
    log.warn("[maintenance] rewrite_manifests table={}", table);
    Row r = spark.sql("CALL system.rewrite_manifests(table => '" + table + "')").first();
    log.warn(
        "[maintenance] rewrite_manifests result: rewritten={} added={}",
        safeGet(r, "rewritten_manifests_count"),
        safeGet(r, "added_manifests_count"));
  }

  private static void expireSnapshots(SparkSession spark, JobConfig cfg, String table) {
    int olderThanDays = Integer.parseInt(cfg.arg("older-than-days", "7"));
    int retainLast = Integer.parseInt(cfg.arg("retain-last", "100"));
    String ts = Instant.now().minus(olderThanDays, ChronoUnit.DAYS).toString();
    String call =
        "CALL system.expire_snapshots(table => '" + table + "', older_than => TIMESTAMP '" + ts
            + "', retain_last => " + retainLast + ")";
    log.warn("[maintenance] expire_snapshots: {}", call);
    Row r = spark.sql(call).first();
    log.warn(
        "[maintenance] expire_snapshots result: deletedDataFiles={} deletedManifests={}",
        safeGet(r, "deleted_data_files_count"),
        safeGet(r, "deleted_manifest_files_count"));
  }

  private static void removeOrphanFiles(SparkSession spark, JobConfig cfg, String table) {
    int olderThanDays = Integer.parseInt(cfg.arg("older-than-days", "7"));
    String ts = Instant.now().minus(olderThanDays, ChronoUnit.DAYS).toString();
    String call =
        "CALL system.remove_orphan_files(table => '" + table + "', older_than => TIMESTAMP '" + ts + "')";
    log.warn("[maintenance] remove_orphan_files (older_than={}): {}", ts, call);
    long removed = spark.sql(call).count();
    log.warn("[maintenance] remove_orphan_files removed {} orphan file(s)", removed);
  }

  /** Log read-only table statistics from the Iceberg metadata tables. */
  private static void report(SparkSession spark, String table, String phase) {
    try {
      long snapshots = spark.sql("SELECT * FROM " + table + ".snapshots").count();
      long manifests = spark.sql("SELECT * FROM " + table + ".manifests").count();
      Row files =
          spark
              .sql(
                  "SELECT count(*) AS file_count, coalesce(sum(file_size_in_bytes),0) AS bytes FROM "
                      + table + ".files")
              .first();
      log.warn(
          "[maintenance] stats({}) table={} snapshots={} manifests={} dataFiles={} bytes={}",
          phase, table, snapshots, manifests, safeGet(files, "file_count"), safeGet(files, "bytes"));
    } catch (Exception e) {
      log.warn("[maintenance] stats({}) unavailable for {}: {}", phase, table, e.getMessage());
    }
  }

  private static Object safeGet(Row r, String field) {
    if (r == null) {
      return "n/a";
    }
    try {
      return r.getAs(field);
    } catch (Exception e) {
      return "n/a";
    }
  }
}

package com.aws.emr.spark.cdc;

import com.aws.emr.common.JobConfig;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Read benchmark comparing the read cost of an Iceberg v2 table (positional delete files) against a
 * v3 table (deletion vectors) that hold the <b>same</b> heavily-deleted merge-on-read data produced
 * by {@link SparkStreamingCDCMirror}.
 *
 * <p>The streaming write path is dominated by shuffling/writing millions of rows per micro-batch,
 * which masks the delete-encoding cost. The difference shows on the <b>read</b> path: a full scan
 * has to reconcile every live row against the accumulated delete files. For v2 that means opening
 * and merging hundreds/thousands of positional-delete parquet files; for v3 it means applying one
 * compact deletion-vector bitmap per data file. This job measures exactly that.
 *
 * <p>Each table is read <b>as of a fixed snapshot</b> ({@code snap2}/{@code snap3}) so the two runs
 * see an identical, reproducible delete-heavy state even if S3 Tables managed compaction runs during
 * the benchmark. For each table it runs a warmup then {@code iters} timed iterations of a full-scan
 * aggregation ({@code count(*)} + {@code sum(balance)} + {@code max(last_updated)}), which forces
 * reading all live rows and applying all delete files, and reports min/median/mean wall-clock time.
 *
 * <h2>Arguments</h2>
 *
 * <pre>
 *   table2=&lt;name&gt; snap2=&lt;snapshot-id&gt;   the v2 table and the snapshot to read as of
 *   table3=&lt;name&gt; snap3=&lt;snapshot-id&gt;   the v3 table and the snapshot to read as of
 *   iters=&lt;n&gt;                          timed iterations per table (default 6)
 * </pre>
 *
 * Plus the usual {@link JobConfig} {@code key=value} args (runtime, catalog, warehouse).
 *
 * @author acmanjon@amazon.com
 */
public class SparkCDCReadBenchmark {

  private static final Logger log = LogManager.getLogger(SparkCDCReadBenchmark.class);

  public static void main(String[] args) {
    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("SparkCDCReadBenchmark");

    int iters = Integer.parseInt(argOrDefault(args, "iters", "6"));
    String cat = cfg.catalogName();

    String[][] targets = {
      {"v2", argOrDefault(args, "table2", "accounts_mirror_v2"), argOrDefault(args, "snap2", "")},
      {"v3", argOrDefault(args, "table3", "accounts_mirror_v3"), argOrDefault(args, "snap3", "")},
    };

    for (String[] t : targets) {
      String label = t[0];
      String fqn = cat + "." + JobConfig.DATABASE + "." + t[1];
      String snapshot = t[2];

      // warmup (not counted)
      runOnce(spark, fqn, snapshot);

      List<Long> timings = new ArrayList<>();
      Row lastResult = null;
      for (int i = 1; i <= iters; i++) {
        long start = System.nanoTime();
        Row r = runOnce(spark, fqn, snapshot);
        long ms = (System.nanoTime() - start) / 1_000_000L;
        timings.add(ms);
        lastResult = r;
        log.warn(
            "BENCH {} iter={} elapsedMs={} count={} sumBalance={}",
            label, i, ms, r.getLong(0), r.get(1));
      }

      timings.sort(Long::compareTo);
      long min = timings.get(0);
      long median = timings.get(timings.size() / 2);
      double mean = timings.stream().mapToLong(Long::longValue).average().orElse(0);
      log.warn(
          "BENCH SUMMARY {} table={} snapshot={} iters={} minMs={} medianMs={} meanMs={} liveRows={}",
          label,
          fqn,
          snapshot.isEmpty() ? "latest" : snapshot,
          iters,
          min,
          median,
          String.format("%.1f", mean),
          lastResult != null ? lastResult.getLong(0) : -1);
    }

    spark.stop();
  }

  /**
   * Read the table (optionally as of a snapshot) and run a full-scan aggregation that forces reading
   * every live row and applying every delete file. Returns the single aggregate row.
   */
  private static Row runOnce(SparkSession spark, String fqn, String snapshot) {
    Dataset<Row> df =
        snapshot == null || snapshot.isEmpty()
            ? spark.read().format("iceberg").table(fqn)
            : spark.read().format("iceberg").option("snapshot-id", snapshot).table(fqn);
    return df.selectExpr(
            "count(*) as cnt",
            "sum(cast(balance as double)) as sum_balance",
            "max(last_updated) as max_ts")
        .first();
  }

  /** Return the value of a {@code key=value} program argument, or {@code def} if not present. */
  private static String argOrDefault(String[] args, String key, String def) {
    if (args != null) {
      String prefix = key + "=";
      for (String arg : args) {
        if (arg != null && arg.startsWith(prefix)) {
          String value = arg.substring(prefix.length()).trim();
          if (!value.isEmpty()) {
            return value;
          }
        }
      }
    }
    return def;
  }
}

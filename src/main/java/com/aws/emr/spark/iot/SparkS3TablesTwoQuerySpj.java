package com.aws.emr.spark.iot;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.protobuf.functions.*;

import com.aws.emr.common.JobConfig;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Two-query streaming benchmark on <b>Amazon S3 Tables</b>, parameterised by Iceberg format version
 * so the same class runs the <b>v2</b> (positional deletes) and <b>v3</b> (deletion vectors) variants.
 * Submit twice — {@code fv=2} and {@code fv=3} — to compare the two merge-on-read delete encodings
 * under an identical MERGE workload. Use Java for both so the comparison isn't confounded by the
 * Spark language runtime.
 *
 * <p><b>Two-query (append then merge) pattern</b> in one driver:
 *
 * <ol>
 *   <li><b>append-v{fv}</b>: Kafka protobuf -&gt; append into {@code stage_v{fv}} (bucketed by
 *       {@code bucket(42, employee_id)}, hash-distributed on write). Cheap, high-throughput landing.
 *   <li><b>merge-v{fv}</b>: Iceberg streaming read of {@code stage_v{fv}} -&gt; upsert MERGE into
 *       {@code employee_v{fv}} (also bucketed on the join key; SPJ planner flags enabled).
 * </ol>
 *
 * <p><b>Why the staging table is seeded.</b> The two queries start together, so when the merge query
 * opens its Iceberg stream the staging table may have no snapshot yet, and the streaming source fails
 * with {@code Cannot load current offset at snapshot -1}. We therefore seed the staging table with a
 * single sentinel row ({@code employee_id = -1}) at startup so a snapshot always exists, drop
 * {@code stream-from-timestamp} (start from the first snapshot), and filter the sentinel out of the
 * MERGE ({@code employee_id >= 0}).
 *
 * <p>Session, catalog ({@code catalog=s3tables}) and warehouse (the table-bucket ARN) come from
 * {@link JobConfig}. The S3 Tables Apache HTTP client pool is raised via
 * {@code --conf spark.sql.catalog.s3tablesbucket.http-client.apache.max-connections=3000} at submit.
 *
 * @author acmanjon@amazon.com
 */
public class SparkS3TablesTwoQuerySpj {

  private static final Logger log = LogManager.getLogger(SparkS3TablesTwoQuerySpj.class);

  private static final String NS = "spjbench";
  private static final String TOPIC = "protobuf-demo-topic-pure";

  /** Parse the {@code fv=2|3} argument (Iceberg format version), defaulting to 3. */
  private static String parseFv(String[] args) {
    String fv = "3";
    if (args != null) {
      for (String a : args) {
        if (a != null && a.toLowerCase().startsWith("fv=")) {
          fv = a.substring(3).trim();
        }
      }
    }
    if (!fv.equals("2") && !fv.equals("3")) {
      throw new IllegalArgumentException("fv must be 2 or 3, got: " + fv);
    }
    return fv;
  }

  public static void main(String[] args) throws Exception {
    // Iceberg format version (2 or 3) is read from an extra key=value arg: fv=2 | fv=3 (default 3).
    final String fv = parseFv(args);

    final String stage = "stage_v" + fv;
    final String target = "employee_v" + fv;
    final String kafkaGroup = "s3tables-spj-v" + fv;
    // Streaming queries execute on a cloned session that does NOT inherit `USE spjbench`, so the
    // append sink and the MERGE target are referenced by their fully-qualified catalog.namespace.table.

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("SparkS3TablesTwoQuerySpjV" + fv);
    final String cat = cfg.catalogName(); // "s3tablesbucket"
    final String stageFqn = cat + "." + NS + "." + stage;
    final String targetFqn = cat + "." + NS + "." + target;

    // Storage-Partitioned Join planner flags.
    spark.conf().set("spark.sql.sources.v2.bucketing.enabled", "true");
    spark.conf().set("spark.sql.iceberg.planning.preserve-data-grouping", "true");
    spark.conf().set("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true");
    spark.conf().set("spark.sql.requireAllClusterKeysForCoPartition", "false");
    spark.conf().set("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled", "true");

    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + NS);
    spark.sql("USE " + NS);

    // Landing/staging table: append-only, bucketed and hash-distributed so files group by bucket.
    spark.sql(
        String.format(
            """
                CREATE TABLE IF NOT EXISTS %1$s
                      (employee_id bigint, age int, start_date timestamp,
                       team string, role string, address string, name string)
                      PARTITIONED BY (bucket(42, employee_id))
                      TBLPROPERTIES (
                                'format-version'='%2$s',
                                'write.distribution-mode'='hash',
                                'write.parquet.compression-codec'='zstd',
                                'write.metadata.delete-after-commit.enabled'='true',
                                'write.metadata.previous-versions-max'='20' );
                """,
            stage, fv));

    // Target table: merge-on-read. v3 -> deletion vectors, v2 -> positional delete files.
    spark.sql(
        String.format(
            """
                CREATE TABLE IF NOT EXISTS %1$s
                      (employee_id bigint, age int, start_date timestamp,
                       team string, role string, address string, name string)
                      PARTITIONED BY (bucket(42, employee_id))
                      TBLPROPERTIES (
                                'format-version'='%2$s',
                                'write.delete.mode'='merge-on-read',
                                'write.update.mode'='merge-on-read',
                                'write.merge.mode'='merge-on-read',
                                'write.merge.distribution-mode'='hash',
                                'write.parquet.compression-codec'='zstd',
                                'write.spark.fanout.enabled'='true',
                                'write.metadata.delete-after-commit.enabled'='true',
                                'write.metadata.previous-versions-max'='50',
                                'commit.retry.num-retries'='20',
                                'commit.retry.min-wait-ms'='250',
                                'commit.retry.max-wait-ms'='60000' );
                """,
            target, fv));

    // Seed the staging table with a sentinel row so it has a snapshot before the merge stream opens
    // (only on a fresh table; on restart the checkpoint + existing snapshots make this a no-op).
    Table stageTable = Spark3Util.loadIcebergTable(spark, stageFqn);
    if (stageTable.currentSnapshot() == null) {
      log.warn("Seeding {} with a sentinel row so the merge stream has a starting snapshot", stage);
      spark.sql(
          "INSERT INTO " + stage
              + " VALUES (CAST(-1 AS bigint), 0, current_timestamp(), 'seed','seed','seed','seed')");
    }

    // --- Query A: Kafka -> append into the staging table ------------------------------------------
    Dataset<Row> kafka =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", cfg.bootstrapServers())
            .option("subscribe", TOPIC)
            .option("startingOffsets", "latest")
            .option("kafka.group.id", kafkaGroup)
            .option("minPartitions", Integer.toString(cfg.shufflePartitions()))
            .option("kafka.fetch.min.bytes", "1048576")
            .option("kafka.fetch.max.bytes", "104857600")
            .option("kafka.max.partition.fetch.bytes", "10485760")
            .option("kafka.max.poll.records", "50000")
            .option("kafka.receive.buffer.bytes", "16777216")
            .load();

    Dataset<Row> decoded =
        kafka
            .select(from_protobuf(col("value"), "Employee", cfg.protoDescriptor()).as("Employee"))
            .select(col("Employee.*"))
            .select(
                col("id").as("employee_id"),
                col("employee_age.value").as("age"),
                col("start_date"),
                col("team.name").as("team"),
                col("role"),
                col("address"),
                col("name"));

    decoded
        .writeStream()
        .queryName("append-v" + fv)
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>)
                (batch, batchId) -> {
                  var session = batch.sparkSession();
                  if (batch.isEmpty()) {
                    return;
                  }
                  // Dedup to the latest row per key BEFORE landing in staging. The topic carries
                  // ~240x redundancy (huge event rate over only ~100k keys), so collapsing here means
                  // the merge query reads ~100k rows/batch instead of the full raw stream and can keep
                  // up. The one necessary dedup shuffle is paid by this (slack) query, not the merge.
                  batch.createOrReplaceTempView("append_src_v" + fv);
                  session
                      .sql(
                          String.format(
                              """
                                  SELECT employee_id, age, start_date, team, role, address, name
                                  FROM (
                                      SELECT *, row_number() OVER (
                                                 PARTITION BY employee_id ORDER BY start_date DESC) AS rn
                                      FROM append_src_v%1$s
                                  )
                                  WHERE rn = 1
                                  """,
                              fv))
                      .writeTo(stageFqn)
                      .append();
                })
        .option("checkpointLocation", cfg.checkpointLocation() + "/append-v" + fv)
        .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
        .start();

    // --- Query B: streaming read of the staging table -> upsert MERGE into the target -------------
    Dataset<Row> stageStream =
        spark
            .readStream()
            .format("iceberg")
            .option("streaming-max-rows-per-micro-batch", "4000000")
            .load(stageFqn);

    stageStream
        .writeStream()
        .queryName("merge-v" + fv)
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>)
                (batch, batchId) -> {
                  var session = batch.sparkSession();
                  log.warn("[v{}] merge batch {}", fv, batchId);
                  if (batch.isEmpty()) {
                    return;
                  }
                  batch.createOrReplaceTempView("src_v" + fv);
                  // Dedup to the latest row per key and drop the sentinel seed row (employee_id < 0).
                  session.sql(
                      String.format(
                          """
                              MERGE INTO %1$s t
                              USING (
                                    SELECT employee_id, age, start_date, team, role, address, name
                                    FROM (
                                        SELECT *, row_number() OVER (
                                                   PARTITION BY employee_id ORDER BY start_date DESC) AS rn
                                        FROM src_v%2$s
                                    )
                                    WHERE rn = 1 AND employee_id >= 0
                              ) s
                              ON t.employee_id = s.employee_id
                              WHEN MATCHED THEN UPDATE SET *
                              WHEN NOT MATCHED THEN INSERT *
                              """,
                          targetFqn, fv));
                })
        .option("checkpointLocation", cfg.checkpointLocation() + "/merge-v" + fv)
        .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
        .start();

    spark.streams().awaitAnyTermination();
  }
}

package com.aws.emr.spark.iot;

import com.aws.emr.common.JobConfig;
import com.aws.emr.common.StreamingProgressListener;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Two-query streaming benchmark on <b>Amazon S3 Tables</b>, parameterised by Iceberg format version
 * ({@code fv=2|3}) so the same class runs the v2 (positional deletes) and v3 (deletion vectors)
 * variants of an identical latest-vehicle-state MERGE workload. Submit twice - {@code fv=2} and
 * {@code fv=3}, each with its own checkpoint - and compare with the {@code [stream-progress]} lines.
 *
 * <p><b>Two-query (append then merge) pattern</b> in one driver:
 *
 * <ol>
 *   <li><b>spj-append-v{fv}</b>: Kafka telemetry -&gt; latest reading per vehicle -&gt; append into
 *       {@code stage_v{fv}} (bucketed by {@code bucket(16, vehicle_id)}, hash-distributed). Cheap,
 *       high-throughput landing; the dedup shuffle is paid by this slack query, not the merge.
 *   <li><b>spj-merge-v{fv}</b>: Iceberg streaming read of the staging table -&gt; guarded upsert
 *       MERGE into {@code vehicle_state_v{fv}} (bucketed on the join key; SPJ planner flags
 *       enabled, so the join needs no exchange).
 * </ol>
 *
 * <p><b>Why the staging table is seeded.</b> The two queries start together, so when the merge query
 * opens its Iceberg stream the staging table may have no snapshot yet and the streaming source fails
 * with {@code Cannot load current offset at snapshot -1}. A sentinel row ({@code vehicle_id = -1})
 * guarantees a snapshot exists; the MERGE filters it out ({@code vehicle_id >= 0}).
 *
 * <p>Session, catalog ({@code catalog=s3tables}) and warehouse (the table-bucket ARN) come from
 * {@link JobConfig}. Raise the S3 Tables HTTP pool at submit:
 * {@code --conf spark.sql.catalog.s3tablesbucket.http-client.apache.max-connections=3000}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkS3TablesTwoQuerySpj {

  private static final Logger log = LogManager.getLogger(SparkS3TablesTwoQuerySpj.class);

  private static final String NS = "spjbench";
  private static final String SPJ_PARTITION_DDL = "bucket(16, vehicle_id)";

  public static void main(String[] args) throws Exception {
    JobConfig cfg = JobConfig.fromArgs(args);
    final String fv = cfg.formatVersion("3");

    final String stage = "stage_v" + fv;
    final String target = "vehicle_state_v" + fv;

    SparkSession spark = cfg.buildSession("SparkS3TablesTwoQuerySpjV" + fv);
    final String cat = cfg.catalogName();
    // Streaming queries execute on a cloned session that does NOT inherit `USE spjbench`, so both
    // sinks are referenced by their fully-qualified catalog.namespace.table.
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
        cfg.createTableDdl(
            stage,
            Telemetry.COLUMNS_DDL,
            SPJ_PARTITION_DDL,
            JobConfig.Mode.COW,
            Map.of(
                "write.distribution-mode", "hash",
                "write.metadata.previous-versions-max", "20")));

    // Target: merge-on-read latest-state table. fv=3 -> deletion vectors, fv=2 -> positional deletes.
    spark.sql(
        cfg.createTableDdl(
            target, Telemetry.COLUMNS_DDL, SPJ_PARTITION_DDL, JobConfig.Mode.MOR, Map.of()));

    // Seed the staging table with a sentinel row so it has a snapshot before the merge stream opens
    // (only on a fresh table; on restart the checkpoint + existing snapshots make this a no-op).
    Table stageTable = Spark3Util.loadIcebergTable(spark, stageFqn);
    if (stageTable.currentSnapshot() == null) {
      log.warn("Seeding {} with a sentinel row so the merge stream has a starting snapshot", stage);
      spark.sql(
          "INSERT INTO " + stage
              + " VALUES (CAST(-1 AS bigint), current_timestamp(), 'seed', 0, 0, CAST(0 AS bigint),"
              + " false, 0, CAST(-1 AS bigint))");
    }

    // --- Query A: Kafka -> latest per vehicle -> append into the staging table --------------------
    Dataset<Row> decoded = Telemetry.decode(cfg.kafkaStream(spark, cfg.topic()), cfg);

    decoded
        .writeStream()
        .queryName("spj-append-v" + fv)
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>)
                (batch, batchId) -> {
                  SparkSession session = batch.sparkSession();
                  if (batch.isEmpty()) {
                    return;
                  }
                  // Collapse to the latest reading per vehicle BEFORE landing in staging: the topic
                  // carries massive redundancy (high event rate over a bounded key space), so the
                  // merge query reads one row per vehicle per batch instead of the raw stream.
                  batch.createOrReplaceTempView("spj_append_src_v" + fv);
                  session
                      .sql(
                          String.format(
                              """
                              SELECT vehicle_id, event_time, model, speed_kmh, soc_pct, odometer_km,
                                     charging, kafka_partition, kafka_offset
                              FROM (
                                  SELECT *, row_number() OVER (
                                             PARTITION BY vehicle_id
                                             ORDER BY event_time DESC, kafka_offset DESC) AS rn
                                  FROM spj_append_src_v%1$s
                              )
                              WHERE rn = 1
                              """,
                              fv))
                      .writeTo(stageFqn)
                      .append();
                })
        .option("checkpointLocation", cfg.checkpointFor("spj-append-v" + fv))
        .trigger(cfg.trigger(60))
        .start();

    // --- Query B: streaming read of the staging table -> guarded upsert MERGE into the target -----
    Dataset<Row> stageStream =
        spark
            .readStream()
            .format("iceberg")
            .option("streaming-max-rows-per-micro-batch", "4000000")
            .load(stageFqn);

    stageStream
        .writeStream()
        .queryName("spj-merge-v" + fv)
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>)
                (batch, batchId) -> {
                  SparkSession session = batch.sparkSession();
                  log.warn("[v{}] merge batch {}", fv, batchId);
                  if (batch.isEmpty()) {
                    return;
                  }
                  batch.createOrReplaceTempView("spj_merge_src_v" + fv);
                  // Latest row per vehicle, sentinel dropped, stale rows guarded on event_time.
                  session.sql(
                      String.format(
                          """
                          MERGE INTO %1$s t
                          USING (
                                SELECT vehicle_id, event_time, model, speed_kmh, soc_pct,
                                       odometer_km, charging, kafka_partition, kafka_offset
                                FROM (
                                    SELECT *, row_number() OVER (
                                               PARTITION BY vehicle_id
                                               ORDER BY event_time DESC, kafka_offset DESC) AS rn
                                    FROM spj_merge_src_v%2$s
                                )
                                WHERE rn = 1 AND vehicle_id >= 0
                          ) s
                          ON t.vehicle_id = s.vehicle_id
                          WHEN MATCHED AND s.event_time >= t.event_time THEN UPDATE SET *
                          WHEN NOT MATCHED THEN INSERT *
                          """,
                          targetFqn, fv));
                })
        .option("checkpointLocation", cfg.checkpointFor("spj-merge-v" + fv))
        .trigger(cfg.trigger(60))
        .start();

    StreamingProgressListener.attach(spark);
    spark.streams().awaitAnyTermination();
  }
}

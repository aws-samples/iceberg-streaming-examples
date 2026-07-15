package com.aws.emr.spark.iot;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.protobuf.functions.*;

import com.aws.emr.common.JobConfig;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Prototype variant of {@link SparkCustomIcebergIngestMoR} that makes the MERGE benefit from
 * <b>Storage-Partitioned Joins (SPJ)</b>.
 *
 * <p>SPJ removes the join exchange only when <em>both</em> sides of the join expose the same storage
 * partitioning on the join key. A Kafka-derived micro-batch has no Iceberg bucketing to report, so
 * the normal {@code MERGE INTO employee USING (kafka batch)} can never be storage-partitioned. Here
 * we work around that by, for every micro-batch:
 *
 * <ol>
 *   <li>Deduplicating the batch (latest row per {@code employee_id}).
 *   <li>Writing it into a staging Iceberg table {@code employee_spj_stage} that is bucketed by
 *       {@code bucket(42, employee_id)} &mdash; the exact same transform as the target.
 *   <li>Running an upsert {@code MERGE} from the staging table into {@code employee_spj}.
 * </ol>
 *
 * Because both {@code employee_spj} and {@code employee_spj_stage} are bucketed by
 * {@code bucket(42, employee_id)} and the join is on {@code employee_id}, Spark can co-locate the
 * matching buckets and skip the join shuffle. Both tables are therefore partitioned <b>only</b> by
 * {@code bucket(42, employee_id)} (no {@code hours}/{@code team} dimensions) so the reported
 * {@code KeyGroupedPartitioning} lines up exactly on the join key.
 *
 * <p>Unlike the insert-only sibling job this one performs a full upsert
 * ({@code WHEN MATCHED THEN UPDATE SET *}), which is where SPJ actually pays off &mdash; the join has
 * to probe the (large) target, and that probe is what SPJ makes shuffle-free.
 *
 * <p>The SPJ planner flags are enabled on the session below. Inspect the SQL tab of the Spark UI (or
 * the physical plan) for the MERGE: with SPJ engaged there should be no {@code Exchange} on the join.
 *
 * <p>The Spark session, catalog and run environment are selected through {@link JobConfig}
 * {@code key=value} arguments. See {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkCustomIcebergIngestMoRSpj {

  private static final Logger log = LogManager.getLogger(SparkCustomIcebergIngestMoRSpj.class);

  private static final String TARGET = "employee_spj";
  private static final String STAGE = "employee_spj_stage";

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("JavaIoTProtoBuf2IcebergMoRSpj");

    // --- Storage-Partitioned Join planner flags ---------------------------------------------------
    // v2 bucketing must be on, Iceberg must preserve its data grouping when planning the scan, and we
    // allow SPJ even when only a subset of the clustering keys (the bucket) matches the join keys.
    spark.conf().set("spark.sql.sources.v2.bucketing.enabled", "true");
    spark.conf().set("spark.sql.iceberg.planning.preserve-data-grouping", "true");
    spark.conf().set("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true");
    spark.conf().set("spark.sql.requireAllClusterKeysForCoPartition", "false");
    spark
        .conf()
        .set("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled", "true");

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);

    // Target table: bucketed ONLY by bucket(42, employee_id) so its KeyGroupedPartitioning aligns
    // exactly with the join key. merge distribution is 'none' so the SPJ grouping is preserved into
    // the write (no post-join reshuffle).
    spark.sql(
        """
                    CREATE TABLE IF NOT EXISTS employee_spj
                          (employee_id bigint,
                          age int,
                          start_date timestamp,
                          team string,
                          role string,
                          address string,
                          name string
                          )
                          PARTITIONED BY (bucket(42, employee_id))
                          TBLPROPERTIES (
                                    'table_type'='ICEBERG',
                                    'format-version'='3',
                                    'format'='parquet',
                                    'write.parquet.compression-codec'='zstd',
                                    'write.parquet.compression-level'='7',
                                    'write.delete.mode'='merge-on-read',
                                    'write.update.mode'='merge-on-read',
                                    'write.merge.mode'='merge-on-read',
                                    -- hash-distribute the merge write so output is clustered by
                                    -- bucket (few files per commit). SPJ removes the *join*
                                    -- exchange; the *write* still needs clustering to avoid a
                                    -- task x bucket small-file explosion with fanout.
                                    'write.merge.distribution-mode'='hash',
                                    'write.spark.fanout.enabled'='true',
                                    'write.metadata.delete-after-commit.enabled'='true',
                                    'write.metadata.previous-versions-max'='50',
                                    'history.expire.max-snapshot-age-ms'='259200000',
                                    'commit.retry.num-retries'='20',
                                    'commit.retry.min-wait-ms'='250',
                                    'commit.retry.max-wait-ms'='60000',
                                    'compatibility.snapshot-id-inheritance.enabled'='true' );
                    """);

    // Correct any pre-existing employee_spj table to hash merge distribution (CREATE TABLE IF NOT
    // EXISTS does not change the properties of an already-existing table).
    spark.sql("ALTER TABLE employee_spj SET TBLPROPERTIES ('write.merge.distribution-mode'='hash')");

    // Staging table: same bucketing as the target. Written fresh each micro-batch. hash distribution
    // clusters the write by bucket so the staging files are physically grouped by bucket too.
    spark.sql(
        """
                    CREATE TABLE IF NOT EXISTS employee_spj_stage
                          (employee_id bigint,
                          age int,
                          start_date timestamp,
                          team string,
                          role string,
                          address string,
                          name string
                          )
                          PARTITIONED BY (bucket(42, employee_id))
                          TBLPROPERTIES (
                                    'table_type'='ICEBERG',
                                    'format-version'='3',
                                    'format'='parquet',
                                    'write.parquet.compression-codec'='zstd',
                                    'write.distribution-mode'='hash',
                                    'write.metadata.delete-after-commit.enabled'='true',
                                    'write.metadata.previous-versions-max'='10',
                                    'compatibility.snapshot-id-inheritance.enabled'='true' );
                    """);

    Dataset<Row> df = cfg.kafkaStream(spark, "protobuf-demo-topic-pure");

    Dataset<Row> output =
        df.select(from_protobuf(col("value"), "Employee", cfg.protoDescriptor()).as("Employee"))
            .select(col("Employee.*"))
            .select(
                col("id").as("employee_id"),
                col("employee_age.value").as("age"),
                col("start_date"),
                col("team.name").as("team"),
                col("role"),
                col("address"),
                col("name"));

    StreamingQuery query =
        output
            .writeStream()
            .queryName("streaming-protobuf-ingest-spj")
            .format("iceberg")
            .outputMode("append")
            .foreachBatch(
                (VoidFunction2<Dataset<Row>, Long>)
                    (dataframe, batchId) -> {
                      var session = dataframe.sparkSession();
                      log.warn("Writing batch {}", batchId);
                      // Skip empty micro-batches: nothing to stage or merge on an idle trigger.
                      if (dataframe.isEmpty()) {
                        log.warn("Batch {} is empty, skipping", batchId);
                        return;
                      }

                      // 1) Deduplicate the batch to the latest row per key (MERGE requires at most one
                      //    source row per target key).
                      dataframe.createOrReplaceTempView("insert_data");
                      Dataset<Row> deduped =
                          session.sql(
                              """
                                  SELECT employee_id, age, start_date, team, role, address, name
                                  FROM (
                                      SELECT *, row_number() OVER (
                                                 PARTITION BY employee_id ORDER BY start_date DESC) AS row_num
                                      FROM insert_data
                                  )
                                  WHERE row_num = 1
                                  """);

                      // 2) Replace the staging table with this batch. overwritePartitions() swaps the
                      //    buckets present in the batch; since a batch spans all 42 buckets this
                      //    effectively refreshes the whole staging table. The staging table is
                      //    bucketed by bucket(42, employee_id), matching the target.
                      deduped.writeTo("bigdata.employee_spj_stage").overwritePartitions();

                      // 3) Upsert MERGE from the bucket-aligned staging table. With the SPJ flags set
                      //    and both sides bucketed by bucket(42, employee_id), the join on employee_id
                      //    is storage-partitioned and needs no exchange.
                      session.sql(
                          """
                              MERGE INTO bigdata.employee_spj t
                              USING bigdata.employee_spj_stage s
                              ON t.employee_id = s.employee_id
                              WHEN MATCHED THEN UPDATE SET *
                              WHEN NOT MATCHED THEN INSERT *
                              """);
                    })
            .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
            .option("checkpointLocation", cfg.checkpointLocation())
            .start();

    query.awaitTermination();
  }
}

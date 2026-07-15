package com.aws.emr.spark.iot;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.protobuf.functions.*;

import com.aws.emr.common.JobConfig;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Simple single-query MERGE benchmark on <b>Amazon S3 Tables</b>, Iceberg format-version <b>3</b>
 * (deletion vectors). Direct port of {@link SparkCustomIcebergIngestMoR} to S3 Tables: consume
 * protobuf from Kafka and, inside {@code foreachBatch}, MERGE straight into the target table. No
 * staging table, no SPJ, and <b>no per-window deduplication</b> — the raw micro-batch is the MERGE
 * source. Paired with {@link SparkS3TablesMergeV2} to compare v2 vs v3 under an identical workload.
 *
 * <p>The MERGE is insert-only and restricts the target scan to the recent partitions via the ON
 * clause (hourly window + team + exact start_date).
 *
 * <p>Session, catalog ({@code catalog=s3tables}) and warehouse (the table-bucket ARN) come from
 * {@link JobConfig}. Submit with a 10-executor (8 vCPU / 32 GB) dynamic allocation and
 * {@code --conf spark.sql.catalog.s3tablesbucket.http-client.apache.max-connections=3000}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkS3TablesMergeV3 {

  private static final Logger log = LogManager.getLogger(SparkS3TablesMergeV3.class);

  private static final String NS = "spjbench";
  private static final String TABLE = "employee_mor_v3";
  private static final String TOPIC = "protobuf-demo-topic-pure";
  private static final String KAFKA_GROUP = "s3tables-mor-v3";
  private static final String FORMAT_VERSION = "3";

  public static void main(String[] args) throws Exception {
    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("SparkS3TablesMergeV3");
    final String cat = cfg.catalogName(); // "s3tablesbucket"
    final String targetFqn = cat + "." + NS + "." + TABLE;

    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + NS);
    spark.sql("USE " + NS);
    spark.sql(
        String.format(
            """
                CREATE TABLE IF NOT EXISTS %1$s
                      (employee_id bigint, age int, start_date timestamp,
                       team string, role string, address string, name string)
                      PARTITIONED BY (hours(start_date), team, bucket(42, employee_id))
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
            TABLE, FORMAT_VERSION));

    Dataset<Row> kafka =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", cfg.bootstrapServers())
            .option("subscribe", TOPIC)
            .option("startingOffsets", "latest")
            .option("kafka.group.id", KAFKA_GROUP)
            .option("minPartitions", Integer.toString(cfg.shufflePartitions()))
            .option("kafka.fetch.min.bytes", "1048576")
            .option("kafka.fetch.max.bytes", "104857600")
            .option("kafka.max.partition.fetch.bytes", "10485760")
            .option("kafka.max.poll.records", "50000")
            .option("kafka.receive.buffer.bytes", "16777216")
            .load();

    Dataset<Row> output =
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

    output
        .writeStream()
        .queryName("merge-mor-v3")
        .format("iceberg")
        .outputMode("append")
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>)
                (dataframe, batchId) -> {
                  var session = dataframe.sparkSession();
                  log.warn("[mor-v3] batch {}", batchId);
                  if (dataframe.isEmpty()) {
                    return;
                  }
                  dataframe.createOrReplaceTempView("insert_data");
                  // Plain insert-only MERGE, no dedup. ON clause prunes the target to recent
                  // partitions. Target referenced fully-qualified (foreachBatch runs on a cloned
                  // session that does not inherit USE).
                  session.sql(
                      String.format(
                          """
                              MERGE INTO %1$s as t
                              USING  insert_data as s
                              ON `s`.`employee_id`=`t`.`employee_id` AND `t`.`start_date` > current_timestamp() - INTERVAL 1 HOURS
                              AND `t`.`team`='Solutions Architects' AND `t`.`start_date`=`s`.`start_date`
                              WHEN NOT MATCHED THEN INSERT *
                              """,
                          targetFqn));
                })
        .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
        .option("fanout-enabled", "true")
        .option("checkpointLocation", cfg.checkpointLocation())
        .start();

    spark.streams().awaitAnyTermination();
  }
}

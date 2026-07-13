package com.aws.emr.spark.iot;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.protobuf.functions.*;

import com.aws.emr.common.JobConfig;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
 * Shared implementation for the merge-on-read "S3 buckets" IoT examples that write to Amazon S3 or
 * Amazon S3 Tables with object-storage layout enabled and higher throughput Kafka fetch settings.
 *
 * <p>The four public entry points ({@link SparkCustomIcebergIngestMoRS3BucketsAvro},
 * {@link SparkCustomIcebergIngestMoRS3BucketsORC},
 * {@link SparkCustomIcebergIngestMoRS3BucketsAutoAvro} and
 * {@link SparkCustomIcebergIngestMoRS3BucketsAutoORC}) only differ in the target table name and the
 * Iceberg data/delete file format (Avro or ORC), so they all delegate here to avoid the copy/paste
 * drift that previously produced bugs (for example the ORC variants writing Avro files, and one
 * variant inserting into the wrong table).
 *
 * <p>All tables are created as Iceberg format-version 3 (v3) merge-on-read tables, so deletes are
 * written as deletion vectors. The Spark session, catalog and run environment are selected through
 * {@link JobConfig} {@code key=value} arguments; see {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
final class S3BucketsMoRJob {

  private static final Logger log = LogManager.getLogger(S3BucketsMoRJob.class);

  private S3BucketsMoRJob() {}

  /**
   * Run the streaming ingest job.
   *
   * @param args the raw program arguments (parsed by {@link JobConfig})
   * @param appName the Spark application name
   * @param table the unqualified target table name
   * @param fileFormat the Iceberg data and delete file format, {@code "avro"} or {@code "orc"}
   */
  static void run(String[] args, String appName, String table, String fileFormat)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession(appName);

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    spark.sql(
        String.format(
            """
                    CREATE TABLE IF NOT EXISTS %1$s
                          (employee_id bigint,
                          age int,
                          start_date timestamp,
                          team string,
                          role string,
                          address string,
                          name string
                          )
                          PARTITIONED BY (hours(start_date), team, bucket(42, employee_id))
                          TBLPROPERTIES (
                                    'table_type'='ICEBERG',
                                    'format-version'='3',
                                    'write.format.default'='%2$s',
                                    'write.delete.format.default'='%2$s',
                                    'write.delete.mode'='merge-on-read',
                                    'write.update.mode'='merge-on-read',
                                    'write.merge.mode'='merge-on-read',
                                    'write.parquet.row-group-size-bytes' = '134217728',  -- 128MB
                                    'write.parquet.page-size-bytes' = '1048576',  -- 2MB
                                    'write.target-file-size-bytes' = '536870912',  -- 256MB
                                    'write.distribution-mode' = 'hash',
                                    'write.delete.distribution-mode' = 'none',
                                    'write.update.distribution-mode' =  'none',
                                    'write.merge.distribution-mode' = 'none',
                                    'write.object-storage.enabled' = 'true',
                                    'write.spark.fanout.enabled' = 'true',
                                    'write.metadata.delete-after-commit.enabled' = 'false',
                                    'write.metadata.previous-versions-max' = '50',
                                    'history.expire.max-snapshot-age-ms' = '259200000',  -- 3 days
                                    'commit.retry.num-retries'='20',	--Number of times to retry a commit before failing
                                    'commit.retry.min-wait-ms'='250',	--Minimum time in milliseconds to wait before retrying a commit
                                    'commit.retry.max-wait-ms'='60000', -- (1 min)	Maximum time in milliseconds to wait before retrying a commit
                                    'write.parquet.compression-codec'='zstd',
                                    -- if you have a huge number of columns remember to tune dict-size and page-size
                                    'compatibility.snapshot-id-inheritance.enabled'='true' );
                    """,
            table, fileFormat));

    final boolean removeDuplicates = cfg.removeDuplicates();

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

    final String qualifiedTable = JobConfig.DATABASE + "." + table;

    StreamingQuery query =
        output
            .writeStream()
            .queryName("streaming-protobuf-ingest")
            .format("iceberg")
            .outputMode("append")
            .foreachBatch(
                (VoidFunction2<Dataset<Row>, Long>)
                    (dataframe, batchId) -> {
                      var session = dataframe.sparkSession();
                      log.warn("Writing batch {}", batchId);
                      if (removeDuplicates) {
                        dataframe.createOrReplaceTempView("insert_data");
                        String merge =
                            String.format(
                                """
                                  MERGE INTO %1$s as t
                                  USING  insert_data as s
                                  ON `s`.`employee_id`=`t`.`employee_id` AND `t`.`start_date` > current_timestamp() - INTERVAL 1 HOURS
                                  AND `t`.`team`='Solutions Architects' AND `t`.`start_date`=`s`.`start_date`
                                  WHEN NOT MATCHED THEN INSERT *
                                  """,
                                qualifiedTable);
                        session.sql(merge);
                      } else {
                        dataframe.write().insertInto(qualifiedTable);
                      }
                    })
            .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
            .option("fanout-enabled", "true") // disable ordering
            .option("checkpointLocation", cfg.checkpointLocation())
            .start();

    if (cfg.compaction()) {
      ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
      scheduledExecutor.scheduleAtFixedRate(
          new Compact(spark, table), millisToNextHour(), 60 * 60 * 1000, TimeUnit.MILLISECONDS);
    }

    query.awaitTermination();
  }

  private static long millisToNextHour() {
    // we wait 5 minutes to start the compaction process for previous partition
    LocalDateTime nextHour =
        LocalDateTime.now().plusHours(1).truncatedTo(ChronoUnit.HOURS).plusMinutes(5);
    return LocalDateTime.now().until(nextHour, ChronoUnit.MILLIS);
  }

  private static class Compact implements Runnable {
    private final SparkSession spark;
    private final String table;

    Compact(SparkSession spark, String table) {
      this.spark = spark;
      this.table = table;
    }

    @Override
    public void run() {
      log.warn("\nCompaction in progress:\n");
      spark
          .sql(
              String.format(
                  """
                         CALL system.rewrite_data_files(
                         table => '%1$s',
                          strategy => 'sort',
                          sort_order => 'start_date',
                          where => 'start_date >= current_timestamp() - INTERVAL 1 HOURS', -- this sql needs to be adapted to only compact older partitions
                          options => map(
                            'rewrite-job-order','bytes-asc',
                            'target-file-size-bytes','273741824',
                            'max-file-group-size-bytes','10737418240',
                            'partial-progress.enabled', 'true',
                            'max-concurrent-file-group-rewrites', '1000',
                            'partial-progress.max-commits', '10'
                          ))
                          """,
                  table))
          .show();
      log.warn("\nManifest compaction in progress:\n");
      spark
          .sql(
              String.format(
                  """
                            CALL system.rewrite_manifests(
                              table => '%1$s'
                             )
                             """,
                  table))
          .show();
    }
  }
}

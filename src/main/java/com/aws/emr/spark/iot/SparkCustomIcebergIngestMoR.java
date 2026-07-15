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
 * An example of consuming messages from Kafka using Protocol Buffers and writing them to Iceberg
 * using the native data source and a custom Spark/Iceberg writing mechanism, this time using a
 * merge-on-read (MoR) table and asynchronous compaction on a scheduled executor.
 *
 * <p>With Iceberg format-version 3 (v3) the merge-on-read deletes are stored as deletion vectors
 * (Puffin files) instead of the v2 positional delete files, which removes a lot of the write
 * amplification of frequent updates/deletes.
 *
 * <p>The Spark session, catalog and run environment are selected through {@link JobConfig}
 * {@code key=value} arguments. See {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkCustomIcebergIngestMoR {

  private static final Logger log = LogManager.getLogger(SparkCustomIcebergIngestMoR.class);

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("JavaIoTProtoBufDescriptor2Iceberg");

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    spark.sql(
        """
                    CREATE TABLE IF NOT EXISTS employee
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
                                    'write.parquet.compression-level'='7',
                                    'format'='parquet',
                                    'write.delete.mode'='copy-on-write',
                                    'write.update.mode'='merge-on-read',
                                    'write.merge.mode'='merge-on-read',
                                    'write.parquet.row-group-size-bytes' = '134217728',  -- 128MB
                                    'write.parquet.page-size-bytes' = '1048576',  -- 2MB
                                    'write.target-file-size-bytes' = '536870912',  -- 256MB
                                    'write.distribution-mode' = 'hash',
                                    'write.delete.distribution-mode' = 'hash',
                                    'write.update.distribution-mode' =  'hash',
                                    'write.merge.distribution-mode' = 'hash',
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
                    """);

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
                      // Skip empty micro-batches: no data to merge/insert on an idle trigger.
                      if (dataframe.isEmpty()) {
                        log.warn("Batch {} is empty, skipping", batchId);
                        return;
                      }
                      if (removeDuplicates) {
                        dataframe.createOrReplaceTempView("insert_data");
                        // here we are pushing some filters like the team and the date (we know that
                        // we will have late events from hour ago....
                        // we could improve this filtering by bucket and just merge data from that
                        // bucket ( using 8 merge queries), one per bucket. Iceberg bucketing can be
                        // calculated via 'system.bucket(8,employee_id)'.
                        // Deduplicate the micro-batch first: keep only the latest row per employee_id
                        // (by start_date) so a key resent within the same batch is not inserted twice.
                        String merge =
                            """
                                  MERGE INTO bigdata.employee as t
                                  USING (
                                        SELECT employee_id, age, start_date, team, role, address, name
                                        FROM (
                                            SELECT *, row_number() OVER (
                                                       PARTITION BY employee_id ORDER BY start_date DESC) AS row_num
                                            FROM insert_data
                                        )
                                        WHERE row_num = 1
                                  ) as s
                                  ON `s`.`employee_id`=`t`.`employee_id` AND `t`.`start_date` > current_timestamp() - INTERVAL 1 HOURS
                                  AND `t`.`team`='Solutions Architects' AND `t`.`start_date`=`s`.`start_date`
                                  WHEN NOT MATCHED THEN INSERT *
                                  """;
                        session.sql((merge));
                      } else {
                        dataframe.write().insertInto("bigdata.employee");
                      }
                    })
            .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
            .option("fanout-enabled", "true") // disable ordering
            .option("checkpointLocation", cfg.checkpointLocation())
            .start();

    if (cfg.compaction()) {
      ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
      scheduledExecutor.scheduleAtFixedRate(
          new Compact(spark), millisToNextHour(), 60 * 60 * 1000, TimeUnit.MILLISECONDS);
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

    public Compact(SparkSession spark) {
      this.spark = spark;
    }

    @Override
    public void run() {
      // the main idea behind this is in cases where you may be receiving late data randomly and
      // doing the compaction jobs with optimistic concurrency will lead into a lot of conflicts, so
      // we compact older partitions on a schedule instead.
      log.warn("\nCompaction in progress:\n");
      spark
          .sql(
              """
                         CALL system.rewrite_data_files(
                         table => 'employee',
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
                          """)
          .show();
      // rewrite manifests from time to time
      log.warn("\nManifest compaction in progress:\n");
      spark
          .sql(
              """
                            CALL system.rewrite_manifests(
                              table => 'employee'
                             )
                             """)
          .show();
      // old snapshots expiration can be done in another job for older partitions.
    }
  }
}

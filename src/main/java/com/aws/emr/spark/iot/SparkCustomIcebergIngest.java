package com.aws.emr.spark.iot;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.protobuf.functions.*;

import com.aws.emr.common.JobConfig;
import com.aws.emr.common.StreamingProgressListener;
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
 * An example of consuming messages from Kafka using Protocol Buffers and writing them to Iceberg
 * using the native data source and writing via a custom Spark/Iceberg writing mechanism.
 *
 * <p>This implements all the features and mechanisms that we want to be demonstrated:
 *
 * <ul>
 *   <li>Watermark deduplication
 *   <li>Compaction
 *   <li>MERGE INTO Deduplication
 * </ul>
 *
 * <p>The Spark session, catalog and run environment (local, local on S3/S3 Tables, or EMR on
 * S3/S3 Tables) are all selected through {@link JobConfig} {@code key=value} arguments. The
 * {@code employee} table is created as an Iceberg format-version 3 (v3) table. See
 * {@link JobConfig#usage()} for the full argument list.
 *
 * @author acmanjon@amazon.com
 */
public class SparkCustomIcebergIngest {

  private static final Logger log = LogManager.getLogger(SparkCustomIcebergIngest.class);

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
                          PARTITIONED BY (bucket(8, employee_id), hours(start_date), team)
                          TBLPROPERTIES (
                                    'table_type'='ICEBERG',
                                    'format-version'='3',
                                    'write.parquet.compression-level'='7',
                                    'format'='parquet',
                                    'commit.retry.num-retries'='10',	--Number of times to retry a commit before failing
                                    'commit.retry.min-wait-ms'='250',	--Minimum time in milliseconds to wait before retrying a commit
                                    'commit.retry.max-wait-ms'='60000', -- (1 min)	Maximum time in milliseconds to wait before retrying a commit
                                    'write.parquet.compression-codec'='zstd',
                                    -- if you have a huge number of columns remember to tune dict-size and page-size
                                    'compatibility.snapshot-id-inheritance.enabled'='true' );
                    """);

    final boolean removeDuplicates = cfg.removeDuplicates();
    final boolean compactionEnabled = cfg.compaction();

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
                    // here we want to make normal "commits" and then for each 10 trigger run
                    // compactions!
                    (dataframe, batchId) -> {
                      var session = dataframe.sparkSession();
                      log.warn("Writing batch {}", batchId);
                      // Skip empty micro-batches: no data to merge/insert and no reason to run the
                      // MERGE or the periodic compaction on an idle trigger.
                      if (dataframe.isEmpty()) {
                        log.warn("Batch {} is empty, skipping", batchId);
                        return;
                      }
                      if (removeDuplicates) {
                        dataframe.createOrReplaceTempView("insert_data");
                        // here we are pushing some filters like the team and the date (we know that
                        // we will have late events from hour ago....
                        // we could improve this filtering by bucket and just merge data from that
                        // bucket ( using 8 merge queries), one per bucket. Iceberg bucketing  can be calculated via
                        // 'system.bucket(8,employee_id)'
                        // t.employee_id in (1,2,3,...) or t.employee_id in (7,8,9,....)
                        // in each 'in' you can put 1000 values.
                        // another way is to generate a column for the bucket and then make the join/ON there
                        // this one maybe be easier instead of generate that long in(1,3,4,5,6....) list,
                        // the problem is that you wouldn't able to use INSERT *
                        // another thing to test storage-partitioned joins but from streaming sources the performance gains...
                        // should be tested on cluster, on local laptop mode they hurt, already tested
                        //
                        // NOTE ON SEMANTICS: this is *bounded replay suppression*, NOT a global key
                        // upsert. The ON clause is scoped to the last hour, team='Solutions Architects'
                        // and an exact start_date match, and the only action is INSERT when NOT MATCHED.
                        // So it suppresses duplicate re-arrivals of the same (employee_id,start_date)
                        // event within that recent window; it does NOT update existing rows and it will
                        // still insert an older replay outside the window or for another team. For a
                        // global upsert keyed on the business key, see the CDC mirror (SparkCDCMirror /
                        // SparkStreamingCDCMirror) and the README "CDC correctness assumptions".
                        // The inner row_number() first collapses duplicates of the same key within this
                        // one micro-batch so INSERT * cannot write them twice.
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
                      if (compactionEnabled) {
                        // the main idea behind this is in cases where you may have receiving "late
                        // data randomly and
                        // doing the compaction jobs with optimistic concurrency will lead into a
                        // lot of conflicts where you could increase the number of retries ( as we
                        // are using partial
                        // progress we need to increase the commit retries though), or you can just
                        // use this
                        // strategy for compaction, older partitions on each N batches.
                        if (batchId > 0 && batchId % 10 == 0) {
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
                                      'max-concurrent-file-group-rewrites', '10000',
                                      'partial-progress.max-commits', '10'
                                    ))
                                    """)
                              .show();
                        }
                        // rewrite manifests from time to time
                        if (batchId > 0 && batchId % 30 == 0) {
                          log.warn("\nManifest compaction in progress:\n");
                          spark
                              .sql(
                                  """
                                      CALL system.rewrite_manifests(
                                        table => 'employee'
                                       )
                                       """)
                              .show();
                        }

                        // old snapshots expiration can be done in another job for older partitions.
                      }
                    })
            .trigger(Trigger.ProcessingTime(5, TimeUnit.MINUTES))
            .option("fanout-enabled", "true") // disable ordering
            .option("checkpointLocation", cfg.checkpointFor("streaming-protobuf-ingest"))
            .start();
    StreamingProgressListener.attach(spark);
    query.awaitTermination();
  }
}

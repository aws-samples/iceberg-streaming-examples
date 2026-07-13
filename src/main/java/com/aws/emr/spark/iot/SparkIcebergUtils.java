package com.aws.emr.spark.iot;

import com.aws.emr.common.JobConfig;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * An example of Iceberg table maintenance (compaction, snapshot expiration and partition level
 * deduplication) that works against any of the supported catalogs.
 *
 * <p>By default it runs locally against the Hadoop catalog, but you can point it at Glue or S3
 * Tables with {@link JobConfig} {@code key=value} arguments (for example
 * {@code catalog=glue warehouse=s3://bucket/wh}). The {@code employee} table is created as an
 * Iceberg format-version 3 (v3) table if it does not already exist.
 *
 * @author acmanjon@amazon.com
 */
public class SparkIcebergUtils {

  private static final Logger log = LogManager.getLogger(SparkIcebergUtils.class);
  private static final boolean snapshotExpiration = true;
  private static final boolean compactionEnabled = true;
  private static final boolean removeDuplicates = true;

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("SparkIcebergUtils");

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

    if (snapshotExpiration) {
      // remember to config the tables or look the defaults to see what is going to be deleted
      spark
          .sql(
              """
                        CALL system.expire_snapshots(
                         table => 'employee'
                        )
                        """)
          .show();
    }
    if (compactionEnabled) {
      spark
          .sql(
              """
                              CALL system.rewrite_data_files(
                              table => 'employee',
                               strategy => 'sort',
                               sort_order => 'start_date',
                               where => 'start_date >= (current_timestamp() - INTERVAL 2 HOURS) AND start_date <= (current_timestamp() - INTERVAL 1 HOURS)', -- this sql needs to be adapted to only compact older partitions
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

      if (removeDuplicates) {
        // iceberg prefer dynamic overwrite, just set it
        spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
        // remove duplicates from a partition or a set of partitions, this query needs to be tested
        spark
            .sql(
                """
                            INSERT OVERWRITE employee
                            SELECT employee_id, first(age), start_date, first(team), first(role), first(address), first(name)
                            FROM employee
                            WHERE cast(start_date as date) = '2020-07-01'  -- here we remove from a predefined day
                            GROUP BY employee_id, start_date
                             """)
            .show();
      }
    }
  }
}

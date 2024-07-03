package com.aws.emr.spark.iot;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 *
 * An example compaction and snapshot expiration on Apache Iceberg on the local environment mode.
 *
 * Implementation for Glue Catalog up to the user
 *
 * @author acmanjon@amazon.com
 */

public class SparkIcebergUtils {

    private static final Logger log = LogManager.getLogger(SparkIcebergUtils.class);
    private static boolean snapshotExpiration = true;
    private static boolean compactionEnabled = true;
    private static boolean removeDuplicates = true;

    public static void main(String[] args) throws IOException, TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("")
                .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type","hive")
                .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type","hadoop")
                .config("spark.sql.shuffle.partitions","50") //we are not using AQE then we need to tune this
                .config("spark.sql.catalog.local.warehouse","warehouse")
                .config("spark.sql.defaultCatalog","local")
                .getOrCreate();

      spark.sql("""
        CREATE DATABASE IF NOT EXISTS bigdata;
      """);

      spark.sql("""
        USE bigdata;
      """);

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
        if(compactionEnabled){
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

        if(removeDuplicates){
            //iceberg prefer dynamic overwrite, just set it
            spark.sparkContext().conf().set("spark.sql.sources.partitionOverwriteMode","dynamic");
            //remove duplicates from a partition or a set of partitions, this query needs to be tested
            spark
                    .sql("""
                            INSERT OVERWRITE employee
                            SELECT employee_id, start_date, first(team),first(role),first(address),first(name)
                            FROM employee
                            WHERE cast(start_date as date) = '2020-07-01'  -- here we remove from a predefined day
                            GROUP BY employee_id, start_date
                             """
                    )
                    .show();
        }
    }
    }

}


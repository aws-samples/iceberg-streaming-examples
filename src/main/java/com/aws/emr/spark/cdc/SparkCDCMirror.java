package com.aws.emr.spark.cdc;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * An example of MERGE INTO in CDC scenario via Mirror table
 *
 * @author acmanjon@amazon.com
 */

public class SparkCDCMirror {

    private static final Logger log = LogManager.getLogger(SparkCDCMirror.class);
    private static String master = "";
    private static String icebergWarehouse = "warehouse/";
    private static String checkpointDir = "tmp/";
    private static String bootstrapServers = "localhost:9092";

    public static void main(String[] args)
            throws IOException, TimeoutException, StreamingQueryException {

        SparkSession spark = null;
        //local environment
        if (args.length < 1) {
            master = "local[*]";
            log.warn(
                    "No arguments provided, running using local default settings: master={} and Iceberg hadoop based file catalog ",
                    master);
            log.warn(
                    "Iceberg warehouse dir will be 'warehouse/' from the run dir  and the checkpoint directory will be 'tmp/'\n"
                            + " this mode is for local based execution and development. Kafka broker in this case will also be 'localhost:9092'."
                            + " Remember to clean the checkpoint dir for any changes or if you want to start 'clean'");
            spark =
                    SparkSession.builder()
                            .master(master)
                            .appName("CDCLogChangeWriter")
                            .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                            .config("spark.sql.catalog.spark_catalog.type", "hive")
                            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                            .config("spark.sql.catalog.local.type", "hadoop")
                            .config("spark.sql.shuffle.partitions","50") // as we are not using AQE then we need to tune this
                            .config("spark.sql.catalog.local.warehouse", "warehouse")
                            .config("spark.sql.defaultCatalog", "local")
                            .getOrCreate();
            //local environment with deduplication via watermarking
        } else if (args.length == 1) {
            master = "local[*]";
            log.warn(
                    "Running with local master: {} and Iceberg hadoop based file catalog",
                    master
            );
            log.warn(
                    "Iceberg warehouse dir will be 'warehouse/' from the run dir  and the checkpoint directory will be 'tmp/'\n"
                            + " this mode is for local based execution. Kafka broker in this case will also be 'localhost:9092'.");

            spark =
                    SparkSession.builder()
                            .master(master)
                            .appName("CDCLogChangeWriter")
                            .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                            .config("spark.sql.catalog.spark_catalog.type", "hive")
                            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                            .config("spark.sql.catalog.local.type", "hadoop")
                            .config("spark.sql.shuffle.partitions","50") // as we are not using AQE then we need to tune this
                            .config("spark.sql.catalog.local.warehouse", "warehouse")
                            .config("spark.sql.defaultCatalog", "local")
                            .getOrCreate();
        } else if (args.length == 6) {
            icebergWarehouse = args[1];
            checkpointDir = args[3];
            bootstrapServers = args[4];
            log.warn(
                    "Master will be inferred from the environment Iceberg Glue catalog will be used, with the warehouse being: {} \n "
                            + ", the checkpoint is at: {}\n "
                            + "and Kafka bootstrap is: {}",
                    icebergWarehouse,
                    checkpointDir,
                    bootstrapServers
            );
            spark =
                    SparkSession.builder()
                            .appName("CDCLogChangeWriter")
                            .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                            .config("spark.sql.catalog.glue_catalog.warehouse", "org.apache.iceberg.spark.SparkCatalog")
                            .config("spark.sql.catalog.glue_catalog.warehouse", icebergWarehouse)
                            .config("spark.sql.catalog.glue_catalog.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
                            .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                            .config("spark.hadoop.fs.s3.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
                            .config("spark.sql.iceberg.data-prefetch.enabled","true")
                            .config("spark.sql.shuffle.partitions","50") // as we are not using AQE then we need to tune this
                            .config("spark.sql.defaultCatalog", "glue_catalog")
                            .getOrCreate();
        } else {
            log.error(
                    "Invalid number of arguments provided, please check the readme for the correct usage");
            System.exit(1);
        }
        spark.sql(
                """
        CREATE DATABASE IF NOT EXISTS bigdata;
        """);

        spark.sql(
                """
        USE bigdata;
        """);
        spark.sql(
                """
                        CREATE TABLE IF NOT EXISTS accounts_mirror
                              (account_id bigint,
                              balance float,
                              last_updated timestamp
                              )
                              PARTITIONED BY (bucket(8, account_id))
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

        // we just filter changes from the last day as we don't want to scan for latest change while deduplication
        // on a huge dataset, I filter by timestamp, but it would be great to use advanced techniques such as the ones
        // presented on https://tabular.io/apache-iceberg-cookbook/data-engineering-incremental-processing/

        spark.sql("""
                WITH windowed_changes AS (
                SELECT
                    account_id,
                    balance,
                    last_updated,
                    operation,
                    row_number() OVER (
                        PARTITION BY account_id
                        ORDER BY last_updated DESC) AS row_num
                FROM accounts_changelog where last_updated > current_timestamp() - INTERVAL 1 DAY
                ),
                accounts_changes AS (
                    SELECT * FROM windowed_changes WHERE row_num = 1
                )
                MERGE INTO accounts_mirror a USING accounts_changes c
                ON a.account_id = c.account_id
                WHEN MATCHED AND c.operation = 'D' THEN DELETE
                WHEN MATCHED THEN UPDATE
                    SET a.balance = c.balance,
                        a.last_updated = c.last_updated
                WHEN NOT MATCHED AND c.operation != 'D' THEN
                    INSERT (account_id, balance, last_updated)
                    VALUES (c.account_id, c.balance, c.last_updated);

""");
    }
}

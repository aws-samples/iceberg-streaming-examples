package com.aws.emr.spark.cdc;

import static org.apache.spark.sql.functions.*;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * An example of consuming messages from Kafka using a CDC like String format and writing them to Iceberg
 * via native Spark/Iceberg writing mechanism
 *
 * @author acmanjon@amazon.com
 */

public class SparkLogChange {

    private static final Logger log = LogManager.getLogger(SparkLogChange.class);
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

        Dataset<Row> df =
                spark
                        .readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", bootstrapServers)
                        .option("subscribe", "streaming-cdc-log-ingest")
                        .load();


        Dataset<Row> output =df
                .selectExpr("CAST(value AS STRING)")
                .select(split(col("value"),",")).as("data")
                .select("data.*");



         output.printSchema();
        StreamingQuery query =
                output
                        .writeStream()
                        .queryName("streaming-cdc-log-ingest")
                        .format("console")
                        .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
                        .outputMode("append")
                        //.option("checkpointLocation", "tmp/") // iceberg native writing requires this to be enabled
                        .option("fanout-enabled", "true") // disable ordering for low latency writes
                        .start();
                        //.toTable("employee");

        query.awaitTermination();
    }
}

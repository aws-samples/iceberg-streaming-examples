package com.aws.emr.spark.cdc;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.CommitMetadata;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;

import java.util.Map;

public class SparkIncrementalPipeline {

    private static final Logger log = LogManager.getLogger(SparkLogChange.class);
    private static String master = "";
    private static String icebergWarehouse = "warehouse/";
    public static void main(String[] args) throws NoSuchTableException, ParseException {

        SparkSession spark;
        //local environment
        if (args.length < 1) {
            master = "local[*]";
            log.warn(
                    "No arguments provided, running using local default settings: master={} and Iceberg hadoop based file catalog ",
                    master);
            log.warn(
                    "Iceberg warehouse dir will be 'warehouse/' from the run dir  and the checkpoint directory will be 'tmp/'\n"
                            + " this mode is for local based execution and development'");
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
            log.warn(
                    "Master will be inferred from the environment Iceberg Glue catalog will be used, with the warehouse being: {} \n "
                    ,icebergWarehouse
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
            spark = null;
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
        // load the logs table (this is the target table that has been deduplicated)
        Table logsTable = Spark3Util.loadIcebergTable(spark, "accounts_mirror");
// Retrieve last processed snapshot ID from the table
        // Find the last processed ID when the last incremental run isn't the current snapshot
        String lastProcessedId = null;
        for (Snapshot snap : SnapshotUtil.currentAncestors(logsTable)) {
            lastProcessedId = snap.summary().get("watermark:accounts_changelog");
            if (lastProcessedId != null) {
                break;
            }
        }

        // load the source table, log_source
        Table logSourceTable = Spark3Util.loadIcebergTable(spark, "accounts_changelog");
        String toProcessId = Long.toString(logSourceTable.currentSnapshot().snapshotId());

        log.warn("last processed id was {} and the to process id is {}", lastProcessedId, toProcessId);
        // do the incremental read from the source table
        Dataset<Row> newLogs = spark.read()
                .format("iceberg")
                .option("start-snapshot-id", lastProcessedId)
                .option("end-snapshot-id", toProcessId)
                .table("raw.log_source");
        // create a temp view from the result so we can use it in the MERGE INTO sql
        newLogs.createOrReplaceTempView("accounts_source");
    // Update the target table and set the watermark in the same commit
        CommitMetadata.withCommitProperties(
                Map.of("watermark:accounts_source", toProcessId),
                () -> {
                    spark.sql("MERGE INTO ...");
                    return 0;
                }, RuntimeException.class);

    //       Rollback & replay
    //        CALL spark.rollback_to_snapshot('accounts_mirror', <LAST-CORRECT-SNAPSHOT-ID>);

  }
}

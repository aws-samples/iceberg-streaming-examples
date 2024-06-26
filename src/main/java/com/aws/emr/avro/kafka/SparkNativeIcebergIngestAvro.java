package com.aws.emr.avro.kafka;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.protobuf.functions.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

public class SparkNativeIcebergIngestAvro {

  private static final Logger log = LogManager.getLogger(SparkNativeIcebergIngestAvro.class);
  private static String master = "";
  private static boolean removeDuplicates = false;
  private static String protoDescFile = "Employee.desc";
  private static String icebergWarehouse = "src/main/resources/iot_data.pb";
  private static String checkpointDir = "src/main/resources/iot_data.pb";
  private static String bootstrapServers = "localhost:9092";

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    SparkSession spark = null;
    if (args.length < 1) {
      master = "local[*]";
      log.warn(
          "No arguments provided, running using local default settings: master={} and Iceberg hadoop based file catalog ",
          master);
      log.warn(
          "Iceberg warehouse dir will be 'warehouse/' from the run dir  and the checkpoint directory will be 'tmp/'\n"
              + " this mode is for local based execution and development. Kafka broker in this case will also be 'localhost:9092'."
              + " Remember to clean the checkpoint dir for any changes or if you want to start 'clean'");
      removeDuplicates = false;
      spark =
          SparkSession.builder()
              .master(master)
              .appName("JavaIoTProtoBufDescriptor2Iceberg")
              .config(
                  "spark.sql.extensions",
                  "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
              .config(
                  "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
              .config("spark.sql.catalog.spark_catalog.type", "hive")
              .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
              .config("spark.sql.catalog.local.type", "hadoop")
              .config(
                  "spark.sql.shuffle.partitions",
                  "50") // as we are not using AQE then we need to tune this
              .config("spark.sql.catalog.local.warehouse", "warehouse")
              .config("spark.sql.defaultCatalog", "local")
              .getOrCreate();
    } else if (args.length == 1) {
      removeDuplicates = Boolean.parseBoolean(args[0]);
      master = "local[*]";
      log.warn(
          "Running with local master: {} and Iceberg hadoop based file catalog  "
              + "removing duplicates within the watermark is {}",
          master,
          removeDuplicates);
      log.warn(
          "Iceberg warehouse dir will be 'warehouse/' from the run dir  and the checkpoint directory will be 'tmp/'\n"
              + " this mode is for local based execution. Kafka broker in this case will also be 'localhost:9092'.");

      spark =
          SparkSession.builder()
              .master(master)
              .appName("JavaIoTProtoBufDescriptor2Iceberg")
              .config(
                  "spark.sql.extensions",
                  "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
              .config(
                  "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
              .config("spark.sql.catalog.spark_catalog.type", "hive")
              .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
              .config("spark.sql.catalog.local.type", "hadoop")
              .config(
                  "spark.sql.shuffle.partitions",
                  "50") // as we are not using AQE then we need to tune this
              .config("spark.sql.catalog.local.warehouse", "warehouse")
              .config("spark.sql.defaultCatalog", "local")
              .getOrCreate();
    } else if (args.length == 5) {
      removeDuplicates = Boolean.parseBoolean(args[0]);
      icebergWarehouse = args[1];
      protoDescFile = args[2];
      checkpointDir = args[3];
      bootstrapServers = args[4];
      log.warn(
          "Master will be inferred from the environment Iceberg Glue catalog will be used, with the warehouse being: {} \n "
              + "removing duplicates within the watermark is {}, the descriptor file is at: {} and the checkpoint is at: {}\n "
              + "Kafka bootstrap is: {}",
          icebergWarehouse,
          removeDuplicates,
          protoDescFile,
          checkpointDir,
          bootstrapServers);
      spark =
          SparkSession.builder()
              .appName("JavaIoTProtoBufDescriptor2Iceberg")
              .config(
                  "spark.sql.extensions",
                  "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
              .config(
                  "spark.sql.catalog.glue_catalog.warehouse",
                  "org.apache.iceberg.spark.SparkCatalog")
              .config("spark.sql.catalog.glue_catalog.warehouse", icebergWarehouse)
              .config(
                  "spark.sql.catalog.glue_catalog.catalog-impl",
                  "org.apache.iceberg.aws.glue.GlueCatalog")
              .config(
                  "spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
              .config(
                  "spark.sql.shuffle.partitions",
                  "50") // as we are not using AQE then we need to tune this
              .config("spark.sql.defaultCatalog", "glue_catalog")
              .getOrCreate();
    } else {
      log.error(
          "Invalid number of arguments provided, please check the readme for the correct usage");
      System.exit(1);
    }

    // in production you should configure this via env, spark configs or log4j
    spark.sparkContext().setLogLevel("WARN");

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
            .option("subscribe", "protobuf-demo-topic-pure")
            .load();

    Dataset<Row> output =
        df.select(from_protobuf(col("value"), "Employee", protoDescFile).as("Employee"))
            .select(col("Employee.*"))
            .select(
                col("id").as("employee_id"),
                col("employee_age.value").as("age"),
                col("start_date"),
                col("team.name").as("team"),
                col("role"),
                col("address"),
                col("name"));

    if (removeDuplicates) {
      output =
          output
              .withWatermark("start_date", "120 seconds")
              .dropDuplicatesWithinWatermark("start_date", "employee_id");
    }
    // output.printSchema();
    StreamingQuery query =
        output
            .writeStream()
            .queryName("streaming-protobuf-ingest")
            .format("iceberg")
            .trigger(Trigger.ProcessingTime(5, TimeUnit.MINUTES))
            .outputMode("append")
            .option(
                "checkpointLocation", "tmp/") // iceberg native writing requires this to be enabled
            .option("fanout-enabled", "true") // disable ordering for low latency writes
            .toTable("local.employee");
    query.awaitTermination();
  }
}

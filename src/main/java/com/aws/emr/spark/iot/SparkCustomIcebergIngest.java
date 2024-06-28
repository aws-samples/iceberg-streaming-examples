package com.aws.emr.spark.iot;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.protobuf.functions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.aws.emr.avro.kafka.SparkNativeIcebergIngestAvro;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 *
 * An example of consuming messages from Kafka using Protocol Buffers and writing them to Iceberg using the native
 * data source and writing via custom Spark/Iceberg writing mechanism
 *
 * This implements all the features and mechanisms that we want to be demostrated.
 *
 * Watermark deduplication
 * Compaction
 * MERGE INTO Deduplication
 *
 * @author acmanjon@amazon.com
 *
 */

public class SparkCustomIcebergIngest {

  private static final Logger log = LogManager.getLogger(SparkCustomIcebergIngest.class);
  private static String master = "";
  private static boolean removeDuplicates = true;
  private static String protoDescFile = "Employee.desc";
  private static String icebergWarehouse = "warehouse/";
  private static String checkpointDir = "tmp/";
  private static String bootstrapServers = "localhost:9092";
  private static boolean compactionEnabled = false;


  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    SparkSession spark;
    //default local env.
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
              .appName("JavaIoTProtoBufDescriptor2Iceberg")
              .config( "spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
              .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
              .config("spark.sql.catalog.spark_catalog.type", "hive")
              .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
              .config("spark.sql.catalog.local.type", "hadoop")
              .config("spark.sql.shuffle.partitions","50") // as we are not using AQE then we need to tune this
              .config("spark.sql.catalog.local.warehouse", "warehouse")
              .config("spark.sql.defaultCatalog", "local")
                  /**
                  //enable SPJ
                   .config("spark.sql.sources.v2.bucketing.enabled","true")
                  .config("spark.sql.sources.v2.bucketing.pushPartValues.enabled","true")
                  .config("spark.sql.requireAllClusterKeysForCoPartition","false")
                  .config("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled","true")
                  .config("spark.sql.sources.v2.bucketing.pushPartKeys.enabled","true")
                  .config("spark.sql.iceberg.planning.preserve-data-grouping","true")
                  .config("spark.sql.sources.v2.bucketing.allowJoinKeysSubsetOfPartitionKeys.enabled","false")
                  .config("spark.sql.optimizer.runtime.rowLevelOperationGroupFilter.enabled","false")
                  // enable shuffle hash join
                  .config("spark.sql.join.preferSortMergeJoin","false")
                  .config("spark.sql.shuffledHashJoinFactor","1")
                  //set none to distribution mode
                  .config("spark.sql.iceberg.distribution-mode","none")
                  //disable adaptative
                  .config("spark.sql.adaptive.coalescePartitions.enabled","false")
                  .config("spark.sql.adaptive.skewJoin.enabled","false")
                  .config("spark.sql.adaptive.enabled","false")**/

                  .getOrCreate();
      //local env with optional compaction and duplicate removal
    } else if (args.length == 2) {
      removeDuplicates = Boolean.parseBoolean(args[0]);
      compactionEnabled = Boolean.parseBoolean(args[1]);
      master = "local[*]";
      log.warn(
          "Running with local master: {} and Iceberg hadoop based file catalog  "
              + "removing duplicates within the watermark is {}, compactions each 'n' batch are {}",
          master,
          removeDuplicates,
          compactionEnabled);
      log.warn(
          "Iceberg warehouse dir will be 'warehouse/' from the run dir  and the checkpoint directory will be 'tmp/'\n"
              + " this mode is for local based execution. Kafka broker in this case will also be 'localhost:9092'.");
      spark =
          SparkSession.builder()
              .master(master)
              .appName("JavaIoTProtoBufDescriptor2Iceberg")
              .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
              .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
              .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
              .config("spark.sql.catalog.spark_catalog.type", "hive")
              .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
              .config("spark.sql.catalog.local.type", "hadoop")
              .config("spark.sql.shuffle.partitions","50") // as we are not using AQE then we need to tune this for the size of our cluster/tasks
                                                           // remember that we should be ideally at 200MB per task minimum
              .config("spark.sql.catalog.local.warehouse", "warehouse")
              .config("spark.sql.defaultCatalog", "local")
              .getOrCreate();
      //local env connected to Glue catalog
    } else if ( args.length == 4){
      removeDuplicates = Boolean.parseBoolean(args[0]);
      compactionEnabled = Boolean.parseBoolean(args[1]);
      icebergWarehouse = args[2];
      checkpointDir = args[3];
      master = "local[*]";
      log.warn(
              "Running with local master and Iceberg hadoop based file catalog  "
                      + "removing duplicates within the watermark is {}, compactions each 'n' batch are {}",

              removeDuplicates,
              compactionEnabled);
      log.warn(
              "Iceberg warehouse dir will be {} from the run dir and the checkpoint directory will be  {}'\n"
                      + " this mode is for local based execution. Kafka broker in this case will also be 'localhost:9092'.",
              icebergWarehouse,checkpointDir);
      spark =
              SparkSession.builder()
                      .master(master)
                      .appName("JavaIoTProtoBufDescriptor2Iceberg")
                      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                      .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                      .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
                      .config("spark.sql.catalog.glue_catalog.warehouse", icebergWarehouse)
                      .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                      .config("spark.sql.shuffle.partitions", "100") // as we are not using AQE then we need to tune this
                      .config("spark.sql.defaultCatalog", "glue_catalog")
                      .getOrCreate();

    }else if (args.length == 6) {
      removeDuplicates = Boolean.parseBoolean(args[0]);
      icebergWarehouse = args[1];
      protoDescFile = args[2];
      checkpointDir = args[3];
      bootstrapServers = args[4];
      compactionEnabled = Boolean.parseBoolean(args[5]);
      log.warn(
          "Master will be inferred from the environment Iceberg Glue catalog will be used, with the warehouse being: {} \n "
              + "removing duplicates within the watermark is {}, the descriptor file is at: {} and the checkpoint is at: {}\n "
              + "Kafka bootstrap is: {}, compactions on each 'n' batch are {}",
              removeDuplicates,icebergWarehouse, protoDescFile, checkpointDir, bootstrapServers, compactionEnabled);
      spark =
          SparkSession.builder()
              .appName("JavaIoTProtoBufDescriptor2Iceberg")
              .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
              .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
              .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
              .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
              .config("spark.sql.catalog.glue_catalog.warehouse", icebergWarehouse)
              .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
              .config("spark.sql.shuffle.partitions", "100") // as we are not using AQE then we need to tune this
              .config("spark.sql.defaultCatalog", "glue_catalog")

              .getOrCreate();
    } else {
      spark = null;
      log.error(
          "Invalid number of arguments provided, please check the readme for the correct usage");
      System.exit(1);
    }
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


    Dataset<Row> df =
            spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", "protobuf-demo-topic-pure")
                .load();
    
        Dataset<Row> output =
            df.select(from_protobuf(col("value"),"Employee", protoDescFile).as("Employee"))
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
                      var session=dataframe.sparkSession();
                      log.warn("Writing batch {}", batchId);
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
                        String merge =
                            """
                                  MERGE INTO bigdata.employee as t
                                  USING  insert_data as s
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
                        if (batchId % 10 == 0) {
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
                        log.warn("\nManifest compaction in progress:\n");
                        if (batchId % 30 == 0) {
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
            .option("checkpointLocation", checkpointDir) // on local mode connected to glue disable it or add hadoop-aws library to add S3 file api .
            .start();
    query.awaitTermination();
      }
    }

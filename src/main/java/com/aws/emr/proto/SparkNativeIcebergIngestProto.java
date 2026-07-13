package com.aws.emr.proto;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.protobuf.functions.*;

import com.aws.emr.common.JobConfig;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * An example of consuming messages from Kafka using Protocol Buffers and writing them to Iceberg
 * using the native data source and the native Spark/Iceberg writing mechanism ({@code toTable}).
 *
 * <p>The {@code employee} table is created as an Iceberg format-version 3 (v3) table. Optional
 * deduplication is done with an event-time watermark. The Spark session, catalog and run
 * environment are selected through {@link JobConfig} {@code key=value} arguments; see
 * {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkNativeIcebergIngestProto {

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
                      PARTITIONED BY (bucket(32, employee_id), hours(start_date), team)
                      TBLPROPERTIES (
                                'table_type'='ICEBERG',
                                'format-version'='3',
                                'write.parquet.compression-level'='7',
                                'format'='parquet',
                                'commit.retry.num-retries'='20',	--Number of times to retry a commit before failing
                                'commit.retry.min-wait-ms'='250',	--Minimum time in milliseconds to wait before retrying a commit
                                'commit.retry.max-wait-ms'='60000', -- (1 min)	Maximum time in milliseconds to wait before retrying a commit
                                'write.parquet.compression-codec'='zstd',
                                -- if you have a huge number of columns remember to tune dict-size and page-size
                                'compatibility.snapshot-id-inheritance.enabled'='true' );
                """);

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

    if (cfg.removeDuplicates()) {
      output =
          output
              .withWatermark("start_date", "120 seconds")
              .dropDuplicatesWithinWatermark("start_date", "employee_id");
    }

    StreamingQuery query =
        output
            .writeStream()
            .queryName("streaming-protobuf-ingest")
            .format("iceberg")
            .trigger(Trigger.ProcessingTime(5, TimeUnit.MINUTES))
            .outputMode("append")
            .option("checkpointLocation", cfg.checkpointLocation()) // required by iceberg native writing
            .option("fanout-enabled", "true") // disable ordering for low latency writes
            .toTable("employee");
    query.awaitTermination();
  }
}

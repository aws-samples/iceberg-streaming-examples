package com.aws.emr.proto;

import static org.apache.spark.sql.functions.col;

import com.aws.emr.common.JobConfig;
import gsr.proto.post.EmployeeOuterClass;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * An example of consuming messages from Kafka using Protocol Buffers and writing them to Iceberg
 * using a Spark UDF to do the protobuf decoding (as opposed to the native {@code from_protobuf}
 * connector used by the other examples).
 *
 * <p>The UDF parses each Kafka value into the full {@code Employee} struct, which is then written to
 * an Iceberg format-version 3 (v3) {@code employee} table. The Spark session, catalog and run
 * environment are selected through {@link JobConfig} {@code key=value} arguments; see
 * {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkProtoUDF {

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("JavaIoTProtoBufUDF2Iceberg");

    // The struct returned by the decoding UDF, matching the employee table schema.
    StructType employeeSchema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("employee_id", DataTypes.LongType, true),
              DataTypes.createStructField("age", DataTypes.IntegerType, true),
              DataTypes.createStructField("start_date", DataTypes.TimestampType, true),
              DataTypes.createStructField("team", DataTypes.StringType, true),
              DataTypes.createStructField("role", DataTypes.StringType, true),
              DataTypes.createStructField("address", DataTypes.StringType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });

    spark
        .udf()
        .register(
            "proto",
            (UDF1<byte[], Row>)
                messageValue -> {
                  EmployeeOuterClass.Employee emp =
                      EmployeeOuterClass.Employee.parseFrom(messageValue);
                  Timestamp startDate = new Timestamp(emp.getStartDate().getSeconds() * 1000L);
                  return RowFactory.create(
                      (long) emp.getId(),
                      emp.getEmployeeAge().getValue(),
                      startDate,
                      emp.getTeam().getName(),
                      emp.getRole().name(),
                      emp.getAddress(),
                      emp.getName());
                },
            employeeSchema);

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
                                'write.parquet.compression-codec'='zstd',
                                'compatibility.snapshot-id-inheritance.enabled'='true' );
                """);

    Dataset<Row> df = cfg.kafkaStream(spark, "protobuf-demo-topic-pure");

    Dataset<Row> output =
        df.select(functions.callUDF("proto", col("value")).as("employee")).select(col("employee.*"));

    StreamingQuery query =
        output
            .writeStream()
            .queryName("streaming-protobuf-udf-ingest")
            .format("iceberg")
            .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
            .outputMode("append")
            .option("checkpointLocation", cfg.checkpointLocation())
            .option("fanout-enabled", "true")
            .toTable("employee");

    query.awaitTermination();
  }
}

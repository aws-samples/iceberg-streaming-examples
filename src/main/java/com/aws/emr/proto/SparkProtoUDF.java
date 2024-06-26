package com.aws.emr.proto;

import gsr.proto.post.EmployeeOuterClass;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 *
 * An example of consuming messages from Kafka using Protocol Buffers and writing them to Iceberg using a Spark UDF
 * to make the "translation"
 *
 * @author acmanjon@amazon.com
 */

public class SparkProtoUDF {

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws IOException the io exception
   * @throws TimeoutException the timeout exception
   * @throws StreamingQueryException the streaming query exception
   */
  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {
    SparkSession spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("JavaIoTProtoBufDescriptor2Iceberg")
            .getOrCreate();

    spark
        .udf()
        .register(
            "proto",
            new UDF1<byte[], String>() {
              @Override
              public String call(byte[] messageValue) throws Exception {
                // String[]  strArr = messageValue.split(",");
                EmployeeOuterClass.Employee emp =
                    EmployeeOuterClass.Employee.parseFrom(messageValue);
                return emp.getName();
              }
            },
            DataTypes.StringType);

    Dataset<Row> df =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "protobuf-demo-topic-pure")
            // .option("startingOffsets","latest")
            .load();

    Dataset<Row> output = df.select(col("value"));

    output.createOrReplaceTempView("employee");

    Dataset<Row> result = spark.sql("SELECT proto(value) as name FROM employee");

    StreamingQuery query = result.writeStream().format("console").outputMode("append").start();

    query.awaitTermination();
    // wait for user input
    System.in.read();
  }
}

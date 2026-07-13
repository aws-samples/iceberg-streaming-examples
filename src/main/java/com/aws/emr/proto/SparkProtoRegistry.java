package com.aws.emr.proto;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.aws.emr.common.JobConfig;
import gsr.proto.post.EmployeeOuterClass;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import static org.apache.spark.sql.functions.col;

/**
 * A native Spark Structured Streaming consumer that reads Protocol Buffers messages produced with
 * the AWS Glue Schema Registry and writes them to an Iceberg format-version 3 (v3) table.
 *
 * <p>Unlike {@link SparkNativeIcebergIngestProto} (which uses the native {@code from_protobuf}
 * connector against a raw descriptor), this example integrates with the Glue Schema Registry: the
 * Kafka value bytes carry the GSR wire header, so we decode them with the
 * {@link GlueSchemaRegistryKafkaDeserializer}. Because that deserializer is not serializable it is
 * created once per partition inside {@code mapPartitions} rather than being shipped from the driver.
 *
 * <p>This complements the plain Java {@code ProtoConsumerSchemaRegistry} example by scaling the same
 * Glue Schema Registry consumption pattern out on Spark and landing the data in Iceberg. To use it
 * you must have created the {@code employee-schema-registry} registry and the {@code Employee.proto}
 * schema, and be producing with {@code ProtoProducerSchemaRegistry}.
 *
 * <p>The Spark session, catalog and run environment are selected through {@link JobConfig}
 * {@code key=value} arguments; the Glue Schema Registry region can be set with {@code region=}
 * (default {@code eu-west-1}). See {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkProtoRegistry {

  private static final Logger log = LogManager.getLogger(SparkProtoRegistry.class);

  private static final String TOPIC = "protobuf-demo-topic";

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    final String region = regionFromArgs(args);

    SparkSession spark = cfg.buildSession("SparkGlueSchemaRegistryProto2Iceberg");

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

    Dataset<byte[]> rawValues =
        cfg.kafkaStream(spark, TOPIC).select(col("value")).as(Encoders.BINARY());

    Dataset<EmployeeBean> decoded =
        rawValues.mapPartitions(new DecodeGsrProtobuf(region), Encoders.bean(EmployeeBean.class));

    // Reorder / rename the bean properties to match the Iceberg table columns.
    Dataset<Row> output =
        decoded.selectExpr(
            "employeeId as employee_id",
            "age",
            "startDate as start_date",
            "team",
            "role",
            "address",
            "name");

    StreamingQuery query =
        output
            .writeStream()
            .queryName("streaming-gsr-protobuf-ingest")
            .format("iceberg")
            .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
            .outputMode("append")
            .option("checkpointLocation", cfg.checkpointLocation())
            .option("fanout-enabled", "true")
            .toTable("employee");

    query.awaitTermination();
  }

  private static String regionFromArgs(String[] args) {
    if (args != null) {
      for (String arg : args) {
        if (arg != null && arg.toLowerCase().startsWith("region=")) {
          return arg.substring("region=".length()).trim();
        }
      }
    }
    return "eu-west-1";
  }

  /**
   * Decodes a partition of Glue Schema Registry protobuf byte arrays into {@link EmployeeBean}s. The
   * {@link GlueSchemaRegistryKafkaDeserializer} is instantiated per partition because it is not
   * serializable and therefore cannot be created on the driver and shipped to the executors.
   */
  private static class DecodeGsrProtobuf
      implements MapPartitionsFunction<byte[], EmployeeBean> {

    private final String region;

    DecodeGsrProtobuf(String region) {
      this.region = region;
    }

    @Override
    public Iterator<EmployeeBean> call(Iterator<byte[]> input) {
      Map<String, Object> config = new HashMap<>();
      config.put(AWSSchemaRegistryConstants.AWS_REGION, region);
      config.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, ProtobufMessageType.POJO.getName());

      GlueSchemaRegistryKafkaDeserializer deserializer = new GlueSchemaRegistryKafkaDeserializer();
      deserializer.configure(config, false);

      List<EmployeeBean> out = new ArrayList<>();
      while (input.hasNext()) {
        byte[] value = input.next();
        if (value == null) {
          continue;
        }
        Object decoded = deserializer.deserialize(TOPIC, value);
        if (decoded instanceof EmployeeOuterClass.Employee emp) {
          out.add(EmployeeBean.fromProto(emp));
        } else {
          log.warn("Skipping record of unexpected type {}", decoded == null ? "null" : decoded.getClass());
        }
      }
      deserializer.close();
      return out.iterator();
    }
  }

  /** JavaBean used as the {@code mapPartitions} output type (encoded with {@link Encoders#bean}). */
  public static class EmployeeBean implements Serializable {
    private long employeeId;
    private int age;
    private Timestamp startDate;
    private String team;
    private String role;
    private String address;
    private String name;

    public static EmployeeBean fromProto(EmployeeOuterClass.Employee emp) {
      EmployeeBean b = new EmployeeBean();
      b.setEmployeeId(emp.getId());
      b.setAge(emp.getEmployeeAge().getValue());
      b.setStartDate(new Timestamp(emp.getStartDate().getSeconds() * 1000L));
      b.setTeam(emp.getTeam().getName());
      b.setRole(emp.getRole().name());
      b.setAddress(emp.getAddress());
      b.setName(emp.getName());
      return b;
    }

    public long getEmployeeId() {
      return employeeId;
    }

    public void setEmployeeId(long employeeId) {
      this.employeeId = employeeId;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }

    public Timestamp getStartDate() {
      return startDate;
    }

    public void setStartDate(Timestamp startDate) {
      this.startDate = startDate;
    }

    public String getTeam() {
      return team;
    }

    public void setTeam(String team) {
      this.team = team;
    }

    public String getRole() {
      return role;
    }

    public void setRole(String role) {
      this.role = role;
    }

    public String getAddress() {
      return address;
    }

    public void setAddress(String address) {
      this.address = address;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }
}

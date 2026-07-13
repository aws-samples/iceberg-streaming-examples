package com.aws.emr.avro.kafka;

import static org.apache.spark.sql.functions.col;

import com.aws.emr.common.JobConfig;
import gsr.avro.post.Employee;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Avro-source counterpart of {@code SparkCustomIcebergIngestMoRS3BucketsAvro} that stores the data
 * as <b>Parquet</b> instead of Avro.
 *
 * <p>It consumes the Apache Avro messages produced by
 * {@link com.aws.emr.avro.kafka.producer.AvroProducer} (topic {@code avro-demo-topic-pure}) and
 * writes them to an Iceberg format-version 3 (v3) merge-on-read table {@code employee_avro_parquet}
 * whose data and delete files are in Parquet format, with object-storage layout enabled (the
 * "S3 buckets" style).
 *
 * <p><b>Decoding.</b> {@code AvroProducer} serializes each record with the generated class's Avro
 * <i>single-object encoding</i> ({@code Employee.toByteBuffer()} — a {@code 0xC3 0x01} marker plus
 * the 8-byte schema fingerprint, then the Avro body). The battle-tested, symmetric way to read that
 * back is with the same generated class's {@link org.apache.avro.message.BinaryMessageDecoder}
 * ({@code Employee.getDecoder()}) rather than {@code from_avro} plus a hard-coded byte offset: the
 * decoder validates the marker and fingerprint and is robust to header changes. We run it inside
 * {@code mapPartitions} because the decoder/Kafka payload handling belongs on the executors.
 *
 * <p>For real deployments with schema evolution, prefer a schema registry (AWS Glue Schema Registry,
 * or Confluent Schema Registry via the ABRiS library) instead of shipping the schema with the job.
 *
 * <p>The Spark session, catalog and run environment are selected through {@link JobConfig}
 * {@code key=value} arguments. See {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkCustomIcebergIngestMoRAvroParquet {

  private static final Logger log =
      LogManager.getLogger(SparkCustomIcebergIngestMoRAvroParquet.class);

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("JavaAvro2IcebergMoRParquet");

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    spark.sql(
        """
                    CREATE TABLE IF NOT EXISTS employee_avro_parquet
                          (employee_id bigint,
                          age int,
                          start_date timestamp,
                          team string,
                          role string,
                          address string,
                          name string
                          )
                          PARTITIONED BY (hours(start_date), team, bucket(42, employee_id))
                          TBLPROPERTIES (
                                    'table_type'='ICEBERG',
                                    'format-version'='3',
                                    'write.format.default'='parquet',
                                    'write.delete.format.default'='parquet',
                                    'write.delete.mode'='merge-on-read',
                                    'write.update.mode'='merge-on-read',
                                    'write.merge.mode'='merge-on-read',
                                    'write.parquet.compression-codec'='zstd',
                                    'write.parquet.compression-level'='7',
                                    'write.parquet.row-group-size-bytes' = '134217728',
                                    'write.parquet.page-size-bytes' = '1048576',
                                    'write.target-file-size-bytes' = '536870912',
                                    'write.distribution-mode' = 'hash',
                                    'write.delete.distribution-mode' = 'none',
                                    'write.update.distribution-mode' = 'none',
                                    'write.merge.distribution-mode' = 'none',
                                    'write.object-storage.enabled' = 'true',
                                    'write.spark.fanout.enabled' = 'true',
                                    'write.metadata.delete-after-commit.enabled' = 'false',
                                    'write.metadata.previous-versions-max' = '50',
                                    'history.expire.max-snapshot-age-ms' = '259200000',
                                    'commit.retry.num-retries'='20',
                                    'commit.retry.min-wait-ms'='250',
                                    'commit.retry.max-wait-ms'='60000',
                                    'compatibility.snapshot-id-inheritance.enabled'='true' );
                    """);

    final boolean removeDuplicates = cfg.removeDuplicates();

    Dataset<byte[]> raw =
        cfg.kafkaStream(spark, "avro-demo-topic-pure").select(col("value")).as(Encoders.BINARY());

    Dataset<Row> output =
        raw.mapPartitions(new DecodeAvro(), Encoders.bean(EmployeeBean.class))
            .selectExpr(
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
            .queryName("streaming-avro-parquet-ingest")
            .format("iceberg")
            .outputMode("append")
            .foreachBatch(
                (VoidFunction2<Dataset<Row>, Long>)
                    (dataframe, batchId) -> {
                      var session = dataframe.sparkSession();
                      log.warn("Writing batch {}", batchId);
                      if (removeDuplicates) {
                        dataframe.createOrReplaceTempView("insert_data");
                        session.sql(
                            """
                                  MERGE INTO bigdata.employee_avro_parquet as t
                                  USING insert_data as s
                                  ON `s`.`employee_id`=`t`.`employee_id` AND `t`.`start_date` > current_timestamp() - INTERVAL 1 HOURS
                                  AND `t`.`team`='Solutions Architects' AND `t`.`start_date`=`s`.`start_date`
                                  WHEN NOT MATCHED THEN INSERT *
                                  """);
                      } else {
                        dataframe.writeTo("bigdata.employee_avro_parquet").append();
                      }
                    })
            .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
            .option("fanout-enabled", "true")
            .option("checkpointLocation", cfg.checkpointLocation())
            .start();

    if (cfg.compaction()) {
      ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
      scheduledExecutor.scheduleAtFixedRate(
          new Compact(spark), millisToNextHour(), 60 * 60 * 1000, TimeUnit.MILLISECONDS);
    }

    query.awaitTermination();
  }

  /**
   * Decodes a partition of Avro single-object-encoded byte arrays into {@link EmployeeBean}s using
   * the generated class's {@code BinaryMessageDecoder} (symmetric with the producer's
   * {@code toByteBuffer()}).
   */
  private static class DecodeAvro implements MapPartitionsFunction<byte[], EmployeeBean> {
    @Override
    public Iterator<EmployeeBean> call(Iterator<byte[]> input) throws Exception {
      List<EmployeeBean> out = new ArrayList<>();
      while (input.hasNext()) {
        byte[] value = input.next();
        if (value == null) {
          continue;
        }
        Employee e = Employee.getDecoder().decode(value);
        out.add(EmployeeBean.fromAvro(e));
      }
      return out.iterator();
    }
  }

  /** JavaBean used as the {@code mapPartitions} output type (encoded with {@code Encoders.bean}). */
  public static class EmployeeBean implements Serializable {
    private long employeeId;
    private int age;
    private Timestamp startDate;
    private String team;
    private String role;
    private String address;
    private String name;

    static EmployeeBean fromAvro(Employee e) {
      EmployeeBean b = new EmployeeBean();
      b.setEmployeeId(e.getEmployeeId());
      b.setAge(e.getAge());
      b.setStartDate(new Timestamp(e.getStartDate())); // producer writes epoch millis
      b.setTeam(String.valueOf(e.getTeam()));
      b.setRole(String.valueOf(e.getRole()));
      b.setAddress(String.valueOf(e.getAddress()));
      b.setName(String.valueOf(e.getName()));
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

  private static long millisToNextHour() {
    LocalDateTime nextHour =
        LocalDateTime.now().plusHours(1).truncatedTo(ChronoUnit.HOURS).plusMinutes(5);
    return LocalDateTime.now().until(nextHour, ChronoUnit.MILLIS);
  }

  private static class Compact implements Runnable {
    private final SparkSession spark;

    Compact(SparkSession spark) {
      this.spark = spark;
    }

    @Override
    public void run() {
      log.warn("\nCompaction in progress:\n");
      spark
          .sql(
              """
                         CALL system.rewrite_data_files(
                         table => 'employee_avro_parquet',
                          strategy => 'sort',
                          sort_order => 'start_date',
                          where => 'start_date >= current_timestamp() - INTERVAL 1 HOURS',
                          options => map(
                            'rewrite-job-order','bytes-asc',
                            'target-file-size-bytes','273741824',
                            'max-file-group-size-bytes','10737418240',
                            'partial-progress.enabled', 'true',
                            'max-concurrent-file-group-rewrites', '1000',
                            'partial-progress.max-commits', '10'))
                          """)
          .show();
      spark.sql("CALL system.rewrite_manifests(table => 'employee_avro_parquet')").show();
    }
  }
}

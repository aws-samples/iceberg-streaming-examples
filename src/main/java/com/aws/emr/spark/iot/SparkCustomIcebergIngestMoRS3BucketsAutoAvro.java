package com.aws.emr.spark.iot;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * Merge-on-read IoT ingest into an Iceberg v3 table stored in Amazon S3 / S3 Tables, writing data
 * and delete files in <b>Avro</b> format into the {@code employee_avro} table (companion of
 * {@link SparkCustomIcebergIngestMoRS3BucketsAvro} using the non-"uncompacted" table name).
 *
 * <p>Delegates to {@link S3BucketsMoRJob}. Run environment and catalog are configured with
 * {@link com.aws.emr.common.JobConfig} {@code key=value} arguments.
 *
 * @author acmanjon@amazon.com
 */
public class SparkCustomIcebergIngestMoRS3BucketsAutoAvro {
  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {
    S3BucketsMoRJob.run(
        args, "SparkCustomIcebergIngestMoRS3BucketsAutoAvro", "employee_avro", "avro");
  }
}

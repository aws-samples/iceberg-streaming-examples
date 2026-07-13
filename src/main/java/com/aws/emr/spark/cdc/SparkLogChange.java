package com.aws.emr.spark.cdc;

import static org.apache.spark.sql.functions.*;

import com.aws.emr.common.JobConfig;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * An example of consuming CDC messages from Kafka in a DMS-like String format and writing them to
 * an Iceberg changelog table via the native Spark/Iceberg writing mechanism.
 *
 * <p>The {@code accounts_changelog} table is created as an Iceberg format-version 3 (v3) table. The
 * Spark session, catalog and run environment are selected through {@link JobConfig}
 * {@code key=value} arguments; see {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkLogChange {

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("CDCLogChangeWriter");

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    spark.sql(
        """
                        CREATE TABLE IF NOT EXISTS accounts_changelog
                              (
                              operation string,
                              account_id bigint,
                              balance bigint,
                              last_updated timestamp
                              )
                              PARTITIONED BY (days(last_updated),bucket(8, account_id))
                              TBLPROPERTIES (
                                        'table_type'='ICEBERG',
                                        'format-version'='3',
                                        'write.parquet.compression-level'='7',
                                        'format'='parquet',
                                        'commit.retry.num-retries'='10',	--Number of times to retry a commit before failing
                                        'commit.retry.min-wait-ms'='250',	--Minimum time in milliseconds to wait before retrying a commit
                                        'commit.retry.max-wait-ms'='60000', -- (1 min)	Maximum time in milliseconds to wait before retrying a commit
                                        'write.parquet.compression-codec'='zstd',
                                        -- if you have a huge number of columns remember to tune dict-size and page-size
                                        'compatibility.snapshot-id-inheritance.enabled'='true' );
                        """);

    Dataset<Row> df = cfg.kafkaStream(spark, "streaming-cdc-log-ingest");

    var output = df.selectExpr("CAST(value AS STRING)");

    List<String> schemaList = Arrays.asList("operation", "account_id", "balance", "last_updated");
    Column column = functions.col("value");
    Column linesSplit = functions.split(column, ",");
    for (int i = 0; i < schemaList.size(); i++) {
      output = output.withColumn(schemaList.get(i), linesSplit.getItem(i));
    }

    output = output.drop(col("value"));
    output =
        output
            .withColumn("account_id", col("account_id").cast("integer"))
            .withColumn("balance", col("balance").cast("integer"))
            .withColumn("last_updated", col("last_updated").divide(1000).cast("timestamp"));
    // remember that spark sql does not support epoch milliseconds, so you need to divide it by 1000
    output.printSchema();
    StreamingQuery query =
        output
            .writeStream()
            .queryName("cdc")
            .format("iceberg")
            .trigger(Trigger.ProcessingTime(2, TimeUnit.MINUTES))
            .outputMode("append")
            .option("checkpointLocation", cfg.checkpointLocation()) // required by iceberg native writing
            .toTable("accounts_changelog");

    query.awaitTermination();
  }
}

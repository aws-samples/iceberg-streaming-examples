package com.aws.emr.proto;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.protobuf.functions.*;

import com.aws.emr.common.JobConfig;
import java.io.IOException;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * A Spark Structured Streaming consumer implemented in Java that decodes Protocol Buffers using the
 * native Spark connectors, injecting the protobuf descriptor as an inline hex string (handy when
 * you cannot ship a descriptor file, e.g. on some serverless setups).
 *
 * <p>The {@code employee} table is created as an Iceberg format-version 3 (v3) table. The Spark
 * session, catalog and run environment are selected through {@link JobConfig} {@code key=value}
 * arguments; see {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkCustomIcebergIngestProtoHex {

  private static final Logger log = LogManager.getLogger(SparkCustomIcebergIngestProtoHex.class);

  private static final String hexData =
      "0A86040A1E676F6F676C652F70726F746F6275662F77726170706572732E70726F746F120F676F6F676C652E70726F746F62756622230A0B446F75626C6556616C756512140A0576616C7565180120012801520576616C756522220A0A466C6F617456616C756512140A0576616C7565180120012802520576616C756522220A0A496E74363456616C756512140A0576616C7565180120012803520576616C756522230A0B55496E74363456616C756512140A0576616C7565180120012804520576616C756522220A0A496E74333256616C756512140A0576616C7565180120012805520576616C756522230A0B55496E74333256616C756512140A0576616C756518012001280D520576616C756522210A09426F6F6C56616C756512140A0576616C7565180120012808520576616C756522230A0B537472696E6756616C756512140A0576616C7565180120012809520576616C756522220A0A427974657356616C756512140A0576616C756518012001280C520576616C75654283010A13636F6D2E676F6F676C652E70726F746F627566420D577261707065727350726F746F50015A31676F6F676C652E676F6C616E672E6F72672F70726F746F6275662F74797065732F6B6E6F776E2F77726170706572737062F80101A20203475042AA021E476F6F676C652E50726F746F6275662E57656C6C4B6E6F776E5479706573620670726F746F330AFF010A1F676F6F676C652F70726F746F6275662F74696D657374616D702E70726F746F120F676F6F676C652E70726F746F627566223B0A0954696D657374616D7012180A077365636F6E647318012001280352077365636F6E647312140A056E616E6F7318022001280552056E616E6F734285010A13636F6D2E676F6F676C652E70726F746F627566420E54696D657374616D7050726F746F50015A32676F6F676C652E676F6C616E672E6F72672F70726F746F6275662F74797065732F6B6E6F776E2F74696D657374616D707062F80101A20203475042AA021E476F6F676C652E50726F746F6275662E57656C6C4B6E6F776E5479706573620670726F746F330AEE030A0E456D706C6F7965652E70726F746F120E6773722E70726F746F2E706F73741A1E676F6F676C652F70726F746F6275662F77726170706572732E70726F746F1A1F676F6F676C652F70726F746F6275662F74696D657374616D702E70726F746F2297020A08456D706C6F796565120E0A0269641801200128055202696412120A046E616D6518022001280952046E616D6512180A0761646472657373180320012809520761646472657373123E0A0C656D706C6F7965655F61676518042001280B321B2E676F6F676C652E70726F746F6275662E496E74333256616C7565520B656D706C6F79656541676512390A0A73746172745F6461746518052001280B321A2E676F6F676C652E70726F746F6275662E54696D657374616D70520973746172744461746512280A047465616D18062001280B32142E6773722E70726F746F2E706F73742E5465616D52047465616D12280A04726F6C6518072001280E32142E6773722E70726F746F2E706F73742E526F6C655204726F6C6522360A045465616D12120A046E616D6518012001280952046E616D65121A0A086C6F636174696F6E18022001280952086C6F636174696F6E2A310A04526F6C65120B0A074D414E414745521000120D0A09444556454C4F5045521001120D0A094152434849544543541002620670726F746F33";

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
                          PARTITIONED BY (bucket(8, employee_id), hours(start_date), team)
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

    final boolean removeDuplicates = cfg.removeDuplicates();
    final boolean compactionEnabled = cfg.compaction();

    Dataset<Row> df = cfg.kafkaStream(spark, "protobuf-demo-topic-pure");

    Dataset<Row> output =
        df.select(
                from_protobuf(col("value"), "Employee", HexFormat.of().parseHex(hexData))
                    .as("Employee"))
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
                    (dataframe, batchId) -> {
                      log.warn("Writing batch {}", batchId);
                      if (removeDuplicates) {
                        // first we want to filter affected partitions for filtering
                        var partitions =
                            dataframe
                                .select(col("start_date"), col("team"), col("employee_id"))
                                .withColumn("day", date_trunc("day", col("start_date")))
                                .withColumn("hour", date_trunc("hour", col("start_date")))
                                .select(col("employee_id"), col("day"), col("hour"), col("team"))
                                .dropDuplicates();
                        log.warn("Affected partitions: {}", partitions.count());
                        log.warn("Partitions to merge: {}", partitions);
                      } else {
                        dataframe.write().insertInto("bigdata.employee");
                      }
                      if (compactionEnabled) {
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
                      }
                    })
            .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
            .option("fanout-enabled", "true") // disable ordering
            .option("checkpointLocation", cfg.checkpointLocation())
            .start();
    query.awaitTermination();
  }
}

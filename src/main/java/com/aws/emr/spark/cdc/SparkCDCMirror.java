package com.aws.emr.spark.cdc;

import com.aws.emr.common.JobConfig;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * An example of the MERGE INTO CDC "mirror" pattern: a Spark batch pipeline that deduplicates the
 * {@code accounts_changelog} table (produced by {@link SparkLogChange}) and merges the latest change
 * per key into the {@code accounts_mirror} table.
 *
 * <p>The {@code accounts_mirror} table is created as an Iceberg format-version 3 (v3) table, so the
 * deletes produced by the MERGE are written as deletion vectors. The Spark session, catalog and run
 * environment are selected through {@link JobConfig} {@code key=value} arguments; see
 * {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkCDCMirror {

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("CDCMirrorMerge");

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    spark.sql(
        """
                        CREATE TABLE IF NOT EXISTS accounts_mirror
                              (account_id bigint,
                              balance float,
                              last_updated timestamp
                              )
                              PARTITIONED BY (bucket(8, account_id))
                              TBLPROPERTIES (
                                        'table_type'='ICEBERG',
                                        'format-version'='3',
                                        'write.parquet.compression-level'='7',
                                        'format'='parquet',
                                        'write.delete.mode'='merge-on-read',
                                        'write.update.mode'='merge-on-read',
                                        'write.merge.mode'='merge-on-read',
                                        'commit.retry.num-retries'='10',	--Number of times to retry a commit before failing
                                        'commit.retry.min-wait-ms'='250',	--Minimum time in milliseconds to wait before retrying a commit
                                        'commit.retry.max-wait-ms'='60000', -- (1 min)	Maximum time in milliseconds to wait before retrying a commit
                                        'write.parquet.compression-codec'='zstd',
                                        -- if you have a huge number of columns remember to tune dict-size and page-size
                                        'compatibility.snapshot-id-inheritance.enabled'='true' );
                        """);

    // we just filter changes from the last day as we don't want to scan for the latest change while
    // deduplicating on a huge dataset. Here we filter by timestamp, but it would be great to use the
    // advanced incremental techniques from
    // https://tabular.io/apache-iceberg-cookbook/data-engineering-incremental-processing/
    spark.sql(
        """
                WITH windowed_changes AS (
                SELECT
                    account_id,
                    balance,
                    last_updated,
                    operation,
                    row_number() OVER (
                        PARTITION BY account_id
                        ORDER BY last_updated DESC) AS row_num
                FROM accounts_changelog where last_updated > current_timestamp() - INTERVAL 1 DAY
                ),
                accounts_changes AS (
                    SELECT * FROM windowed_changes WHERE row_num = 1
                )
                MERGE INTO accounts_mirror a USING accounts_changes c
                ON a.account_id = c.account_id
                WHEN MATCHED AND c.operation = 'D' THEN DELETE
                WHEN MATCHED THEN UPDATE
                    SET a.balance = c.balance,
                        a.last_updated = c.last_updated
                WHEN NOT MATCHED AND c.operation != 'D' THEN
                    INSERT (account_id, balance, last_updated)
                    VALUES (c.account_id, c.balance, c.last_updated);
""");
  }
}

package com.aws.emr.spark.cdc;

import com.aws.emr.common.JobConfig;
import java.io.IOException;
import java.util.Map;
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
    // Merge-on-read mirror by default (the MERGE's deletes/updates become deletion vectors on v3);
    // override with mode=/fv=/fileformat= like every other example.
    spark.sql(
        cfg.createTableDdl(
            "accounts_mirror",
            CdcSql.MIRROR_COLUMNS_DDL,
            CdcSql.MIRROR_PARTITION_DDL,
            JobConfig.Mode.MOR,
            Map.of()));

    // We only scan changes from the last day so we don't deduplicate over the whole (huge) changelog.
    // Dedup keeps the highest source sequence per key (deterministic) and the MERGE guards updates and
    // deletes with c.seq >= a.seq so a stale change can never overwrite newer state. The 1-day filter
    // is a coarse late-arrival window; for exact incremental reads see SparkIncrementalPipeline and
    // https://tabular.io/apache-iceberg-cookbook/data-engineering-incremental-processing/
    spark.sql(
        CdcSql.mirrorMerge(
            "accounts_mirror",
            "(SELECT * FROM accounts_changelog WHERE last_updated > current_timestamp() - INTERVAL 1 DAY) src"));
  }
}

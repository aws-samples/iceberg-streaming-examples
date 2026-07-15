package com.aws.emr.spark.iot;

import com.aws.emr.common.JobConfig;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * Ad-hoc Iceberg table utilities for the telemetry table: snapshot expiration, compaction of a
 * bounded window of <b>closed</b> partitions, and a partition-level duplicate rewrite. Works against
 * any of the supported catalogs ({@code catalog=local|glue|s3tables}).
 *
 * <p>The duplicate rewrite is the "later cleanup" companion to the streaming dedup: if replays older
 * than the MERGE window did land (bounded replay suppression lets them through), this rewrites the
 * affected partitions keeping one row per {@code (vehicle_id, event_time)} identity. It uses dynamic
 * partition overwrite so only the partitions holding the selected day are replaced.
 *
 * <p>For the recommended scheduled baseline (expire/compact/orphans with a dry-run mode) see
 * {@code com.aws.emr.spark.maintenance.IcebergMaintenance}.
 *
 * <h2>Arguments</h2>
 *
 * In addition to the usual {@link JobConfig} arguments:
 *
 * <pre>
 *   table=&lt;name&gt;      telemetry table (default vehicle_telemetry)
 *   day=&lt;YYYY-MM-DD&gt;  day to deduplicate in the duplicate rewrite (default: yesterday)
 * </pre>
 *
 * @author acmanjon@amazon.com
 */
public class SparkIcebergUtils {

  private static final Logger log = LogManager.getLogger(SparkIcebergUtils.class);

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("SparkIcebergUtils");

    final String table = cfg.table(Telemetry.TABLE);
    final String day = cfg.arg("day", "current_date() - INTERVAL 1 DAY");
    // A literal day must be quoted in SQL; the default expression must not.
    final String dayExpr = day.startsWith("current_date") ? day : "'" + day + "'";

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    spark.sql(
        cfg.createTableDdl(
            table, Telemetry.COLUMNS_DDL, Telemetry.PARTITION_DDL, JobConfig.Mode.COW, Map.of()));

    // 1) Expire old snapshots (defaults apply; tune the table properties or pass explicit
    //    older_than/retain_last in a real deployment - see IcebergMaintenance).
    log.warn("Expiring old snapshots of {}", table);
    spark.sql(String.format("CALL system.expire_snapshots(table => '%s')", table)).show();

    // 2) Compact a bounded window of closed hourly partitions (never the hot current hour).
    log.warn("Compacting closed hourly partitions of {}", table);
    spark.sql(TelemetrySql.rewriteClosedHourDataFiles(JobConfig.DATABASE + "." + table)).show();

    // 3) Partition-level duplicate rewrite: keep one row per (vehicle_id, event_time) identity for
    //    the selected day. first(...) picks the surviving values deterministically by ordering on
    //    the Kafka offset. Dynamic overwrite replaces only the touched partitions.
    log.warn("Rewriting duplicates for day {} of {}", day, table);
    spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
    spark
        .sql(
            String.format(
                """
                INSERT OVERWRITE %1$s
                SELECT vehicle_id, event_time,
                       first(model), first(speed_kmh), first(soc_pct),
                       first(odometer_km), first(charging),
                       first(kafka_partition), first(kafka_offset)
                FROM (SELECT * FROM %1$s ORDER BY kafka_offset DESC)
                WHERE cast(event_time as date) = %2$s
                GROUP BY vehicle_id, event_time
                """,
                table, dayExpr))
        .show();
  }
}

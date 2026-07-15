"""Ad-hoc Iceberg table utilities for the telemetry table.

PySpark counterpart of ``com.aws.emr.spark.iot.SparkIcebergUtils``: snapshot expiration, compaction
of a bounded window of **closed** partitions, and a partition-level duplicate rewrite. Works against
any catalog selected via :class:`iceberg_streaming.common.JobConfig`.

The duplicate rewrite is the "later cleanup" companion to the streaming dedup: if replays older than
the MERGE window did land (bounded replay suppression lets them through), this rewrites the affected
partitions keeping one row per ``(vehicle_id, event_time)`` identity, using dynamic partition
overwrite so only the touched partitions are replaced.

Arguments (plus the usual JobConfig ones)::

    table=<name>      telemetry table (default vehicle_telemetry)
    day=<YYYY-MM-DD>  day to deduplicate in the duplicate rewrite (default: yesterday)
"""

from __future__ import annotations

import logging
import sys

from iceberg_streaming.common import DATABASE, JobConfig, Mode
from iceberg_streaming.iot import _sql, _telemetry

log = logging.getLogger("iceberg_streaming.iot.spark_iceberg_utils")


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkIcebergUtils")

    table = cfg.table(_telemetry.TABLE)
    day = cfg.arg("day", "current_date() - INTERVAL 1 DAY")
    # A literal day must be quoted in SQL; the default expression must not.
    day_expr = day if day.startswith("current_date") else f"'{day}'"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(cfg.create_table_ddl(table, _telemetry.COLUMNS_DDL, _telemetry.PARTITION_DDL, Mode.COW))

    # 1) Expire old snapshots (defaults apply; see iceberg-maintenance for the tunable version).
    log.warning("Expiring old snapshots of %s", table)
    spark.sql(f"CALL system.expire_snapshots(table => '{table}')").show()

    # 2) Compact a bounded window of closed hourly partitions (never the hot current hour).
    log.warning("Compacting closed hourly partitions of %s", table)
    spark.sql(_sql.rewrite_closed_hour_data_files(f"{DATABASE}.{table}")).show()

    # 3) Partition-level duplicate rewrite: keep one row per (vehicle_id, event_time) identity for
    #    the selected day; first(...) picks survivors deterministically by Kafka offset order.
    log.warning("Rewriting duplicates for day %s of %s", day, table)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.sql(
        f"""
        INSERT OVERWRITE {table}
        SELECT vehicle_id, event_time,
               first(model), first(speed_kmh), first(soc_pct),
               first(odometer_km), first(charging),
               first(kafka_partition), first(kafka_offset)
        FROM (SELECT * FROM {table} ORDER BY kafka_offset DESC)
        WHERE cast(event_time as date) = {day_expr}
        GROUP BY vehicle_id, event_time
        """
    ).show()


if __name__ == "__main__":
    main()

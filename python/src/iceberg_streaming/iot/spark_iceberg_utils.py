"""Iceberg table maintenance: compaction, snapshot expiration and partition-level dedup.

PySpark counterpart of ``com.aws.emr.spark.iot.SparkIcebergUtils``. Batch job (no Kafka) that works
against any catalog selected via :class:`iceberg_streaming.common.JobConfig`. Creates the
``employee`` table as Iceberg v3 if it does not exist.
"""

from __future__ import annotations

import logging
import sys

from iceberg_streaming.common import DATABASE, JobConfig

log = logging.getLogger("iceberg_streaming.iot.spark_iceberg_utils")

_SNAPSHOT_EXPIRATION = True
_COMPACTION_ENABLED = True
_REMOVE_DUPLICATES = True

_CREATE_TABLE = """
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
                    'commit.retry.num-retries'='10',
                    'commit.retry.min-wait-ms'='250',
                    'commit.retry.max-wait-ms'='60000',
                    'write.parquet.compression-codec'='zstd',
                    'compatibility.snapshot-id-inheritance.enabled'='true' )
"""


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkIcebergUtils")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(_CREATE_TABLE)

    if _SNAPSHOT_EXPIRATION:
        spark.sql("CALL system.expire_snapshots(table => 'employee')").show()

    if _COMPACTION_ENABLED:
        spark.sql(
            """
            CALL system.rewrite_data_files(
              table => 'employee',
              strategy => 'sort',
              sort_order => 'start_date',
              where => 'start_date >= (current_timestamp() - INTERVAL 2 HOURS) AND start_date <= (current_timestamp() - INTERVAL 1 HOURS)',
              options => map(
                'rewrite-job-order','bytes-asc',
                'target-file-size-bytes','273741824',
                'max-file-group-size-bytes','10737418240',
                'partial-progress.enabled', 'true',
                'max-concurrent-file-group-rewrites', '10000',
                'partial-progress.max-commits', '10'))
            """
        ).show()

        if _REMOVE_DUPLICATES:
            # iceberg prefers dynamic overwrite, just set it
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            spark.sql(
                """
                INSERT OVERWRITE employee
                SELECT employee_id, first(age), start_date, first(team), first(role), first(address), first(name)
                FROM employee
                WHERE cast(start_date as date) = '2020-07-01'
                GROUP BY employee_id, start_date
                """
            ).show()


if __name__ == "__main__":
    main()

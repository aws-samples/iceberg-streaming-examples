"""CDC mirror MERGE pattern (batch).

PySpark counterpart of ``com.aws.emr.spark.cdc.SparkCDCMirror``. Deduplicates the
``accounts_changelog`` table (latest change per key) and merges it into the ``accounts_mirror``
Iceberg v3 table, so the deletes are written as deletion vectors.
"""

from __future__ import annotations

import sys

from iceberg_streaming.common import DATABASE, JobConfig

_CREATE_TABLE = """
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
                    'commit.retry.num-retries'='10',
                    'commit.retry.min-wait-ms'='250',
                    'commit.retry.max-wait-ms'='60000',
                    'write.parquet.compression-codec'='zstd',
                    'compatibility.snapshot-id-inheritance.enabled'='true' )
"""

_MERGE = """
    WITH windowed_changes AS (
        SELECT account_id, balance, last_updated, operation,
               row_number() OVER (PARTITION BY account_id ORDER BY last_updated DESC) AS row_num
        FROM accounts_changelog WHERE last_updated > current_timestamp() - INTERVAL 1 DAY
    ),
    accounts_changes AS (
        SELECT * FROM windowed_changes WHERE row_num = 1
    )
    MERGE INTO accounts_mirror a USING accounts_changes c
    ON a.account_id = c.account_id
    WHEN MATCHED AND c.operation = 'D' THEN DELETE
    WHEN MATCHED THEN UPDATE SET a.balance = c.balance, a.last_updated = c.last_updated
    WHEN NOT MATCHED AND c.operation != 'D' THEN
        INSERT (account_id, balance, last_updated) VALUES (c.account_id, c.balance, c.last_updated)
"""


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkCDCMirrorMerge")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(_CREATE_TABLE)
    spark.sql(_MERGE)


if __name__ == "__main__":
    main()

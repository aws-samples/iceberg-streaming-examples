"""CDC mirror MERGE pattern (batch).

PySpark counterpart of ``com.aws.emr.spark.cdc.SparkCDCMirror``. Deduplicates the
``accounts_changelog`` table (latest change per key) and merges it into the ``accounts_mirror``
Iceberg v3 table, so the deletes are written as deletion vectors.
"""

from __future__ import annotations

import sys

from iceberg_streaming.cdc._sql import mirror_merge
from iceberg_streaming.common import DATABASE, JobConfig

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS accounts_mirror
          (account_id bigint,
          balance float,
          last_updated timestamp,
          seq bigint            -- last applied source sequence, for stale-change guards
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

# Only scan the last day of changes so we don't deduplicate over the whole changelog. Dedup keeps the
# highest source sequence per key and the MERGE guards updates/deletes with c.seq >= a.seq.
_MERGE = mirror_merge(
    "accounts_mirror",
    "(SELECT * FROM accounts_changelog WHERE last_updated > current_timestamp() - INTERVAL 1 DAY) src",
)


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkCDCMirrorMerge")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(_CREATE_TABLE)
    spark.sql(_MERGE)


if __name__ == "__main__":
    main()

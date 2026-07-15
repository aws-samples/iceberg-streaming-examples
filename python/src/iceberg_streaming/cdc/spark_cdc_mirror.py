"""CDC mirror MERGE pattern (batch).

PySpark counterpart of ``com.aws.emr.spark.cdc.SparkCDCMirror``. Deduplicates the
``accounts_changelog`` table (highest source ``seq`` per key) and merges it into the
``accounts_mirror`` table with the shared, guarded MERGE (see :mod:`iceberg_streaming.cdc._sql`).
The mirror is merge-on-read by default, so the deletes are written as deletion vectors on v3;
override with ``mode=``/``fv=``/``fileformat=`` like every other example.
"""

from __future__ import annotations

import sys

from iceberg_streaming.cdc import _sql
from iceberg_streaming.common import DATABASE, JobConfig, Mode

# Only scan the last day of changes so we don't deduplicate over the whole changelog. Dedup keeps the
# highest source sequence per key and the MERGE guards updates/deletes with c.seq >= a.seq.
_MERGE = _sql.mirror_merge(
    "accounts_mirror",
    "(SELECT * FROM accounts_changelog WHERE last_updated > current_timestamp() - INTERVAL 1 DAY) src",
)


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkCDCMirrorMerge")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(
        cfg.create_table_ddl(
            "accounts_mirror", _sql.MIRROR_COLUMNS_DDL, _sql.MIRROR_PARTITION_DDL, Mode.MOR
        )
    )
    spark.sql(_MERGE)


if __name__ == "__main__":
    main()

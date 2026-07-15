"""CDC incremental processing pipeline.

PySpark counterpart of ``com.aws.emr.spark.cdc.SparkIncrementalPipeline``. Reads only the snapshots
appended to ``accounts_changelog`` since the last processed one and merges them into
``accounts_mirror``.

**Difference from the Java version:** the Java job attaches the source watermark to the *same* Iceberg
commit as the MERGE using the JVM-only ``org.apache.iceberg.spark.CommitMetadata`` thread-local. That
API is impractical to drive from Python workers, so this version stores the watermark as a table
property (``watermark:accounts_changelog``) on ``accounts_mirror`` in a follow-up ``ALTER TABLE``
statement. It is therefore not committed atomically with the MERGE; if the pipeline dies between the
two, the range is simply reprocessed on the next run (the MERGE is idempotent).
"""

from __future__ import annotations

import logging
import sys

from iceberg_streaming.cdc import _sql
from iceberg_streaming.cdc._sql import mirror_merge
from iceberg_streaming.common import DATABASE, JobConfig, Mode

log = logging.getLogger("iceberg_streaming.cdc.spark_incremental_pipeline")

WATERMARK_KEY = "watermark:accounts_changelog"

_MERGE = mirror_merge("accounts_mirror", "accounts_source")


def _read_watermark(spark) -> str | None:
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES accounts_mirror ('{WATERMARK_KEY}')").collect()
    except Exception:  # property not set / not supported yet
        return None
    if not rows:
        return None
    value = rows[0]["value"]
    # Spark returns a placeholder string when the property is absent.
    if not value or "does not have property" in value:
        return None
    return value


def _current_source_snapshot(spark) -> str | None:
    rows = spark.sql(
        "SELECT snapshot_id FROM accounts_changelog.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return str(rows[0]["snapshot_id"]) if rows else None


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkCDCIncrementalPipeline")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(
        cfg.create_table_ddl(
            "accounts_changelog", _sql.CHANGELOG_COLUMNS_DDL, _sql.CHANGELOG_PARTITION_DDL, Mode.COW
        )
    )
    # Merge-on-read mirror by default so the MERGE's deletes become deletion vectors on v3.
    spark.sql(
        cfg.create_table_ddl(
            "accounts_mirror", _sql.MIRROR_COLUMNS_DDL, _sql.MIRROR_PARTITION_DDL, Mode.MOR
        )
    )

    to_process = _current_source_snapshot(spark)
    if to_process is None:
        log.warning("Source table accounts_changelog has no snapshots yet, nothing to process.")
        return

    last_processed = _read_watermark(spark)
    if to_process == last_processed:
        log.warning("No new snapshots in accounts_changelog since %s, nothing to process.", last_processed)
        return
    log.warning("Last processed source snapshot was %s, processing up to %s", last_processed, to_process)

    reader = spark.read.format("iceberg").option("end-snapshot-id", to_process)
    if last_processed is not None:
        reader = reader.option("start-snapshot-id", last_processed)  # exclusive
    reader.table("accounts_changelog").createOrReplaceTempView("accounts_source")

    spark.sql(_MERGE)

    # Persist the watermark (see the module docstring for the atomicity caveat vs. the Java version).
    spark.sql(
        f"ALTER TABLE accounts_mirror SET TBLPROPERTIES ('{WATERMARK_KEY}' = '{to_process}')"
    )
    log.warning("Incremental merge complete, watermark advanced to %s", to_process)


if __name__ == "__main__":
    main()

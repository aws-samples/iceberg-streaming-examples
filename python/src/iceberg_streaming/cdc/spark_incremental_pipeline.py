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

from iceberg_streaming.common import DATABASE, JobConfig

log = logging.getLogger("iceberg_streaming.cdc.spark_incremental_pipeline")

WATERMARK_KEY = "watermark:accounts_changelog"

_CREATE_SOURCE = """
    CREATE TABLE IF NOT EXISTS accounts_changelog
          (operation string, account_id bigint, balance bigint, last_updated timestamp)
          PARTITIONED BY (days(last_updated), bucket(8, account_id))
          TBLPROPERTIES ('table_type'='ICEBERG','format-version'='3',
                         'write.parquet.compression-codec'='zstd',
                         'compatibility.snapshot-id-inheritance.enabled'='true')
"""

_CREATE_TARGET = """
    CREATE TABLE IF NOT EXISTS accounts_mirror
          (account_id bigint, balance float, last_updated timestamp)
          PARTITIONED BY (bucket(8, account_id))
          TBLPROPERTIES ('table_type'='ICEBERG','format-version'='3',
                         'write.delete.mode'='merge-on-read','write.update.mode'='merge-on-read',
                         'write.merge.mode'='merge-on-read','write.parquet.compression-codec'='zstd',
                         'compatibility.snapshot-id-inheritance.enabled'='true')
"""

_MERGE = """
    WITH windowed_changes AS (
        SELECT account_id, balance, last_updated, operation,
               row_number() OVER (PARTITION BY account_id ORDER BY last_updated DESC) AS row_num
        FROM accounts_source
    ),
    accounts_changes AS (SELECT * FROM windowed_changes WHERE row_num = 1)
    MERGE INTO accounts_mirror a USING accounts_changes c
    ON a.account_id = c.account_id
    WHEN MATCHED AND c.operation = 'D' THEN DELETE
    WHEN MATCHED THEN UPDATE SET a.balance = c.balance, a.last_updated = c.last_updated
    WHEN NOT MATCHED AND c.operation != 'D' THEN
        INSERT (account_id, balance, last_updated) VALUES (c.account_id, c.balance, c.last_updated)
"""


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
    spark.sql(_CREATE_SOURCE)
    spark.sql(_CREATE_TARGET)

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

"""Standalone Iceberg table-maintenance driver.

PySpark counterpart of ``com.aws.emr.spark.maintenance.IcebergMaintenance``. This is the
**recommended baseline** for keeping the streaming tables healthy: running maintenance in a separate
scheduled job avoids competing with the ingestion writer inside ``foreachBatch`` (which extends batch
latency and causes commit conflicts). The in-job maintenance in ``spark_custom_iceberg_ingest`` is
kept only as a deliberate "maintenance-concurrent" comparison pattern.

Actions (behind ``action=``): ``rewrite_data_files``, ``rewrite_manifests``, ``expire_snapshots``,
``remove_orphan_files``, or ``all`` (default).

Arguments (plus the usual :class:`~iceberg_streaming.common.JobConfig` ``key=value`` args)::

    table=<name>            table to maintain, in the bigdata database (required)
    action=all|rewrite_data_files|rewrite_manifests|expire_snapshots|remove_orphan_files
    where=<predicate>       optional filter for rewrite_data_files (recent partitions only)
    sort-order=<cols>       sort columns for the sort strategy (default: none -> bin-pack)
    older-than-days=<n>     retention horizon for expire_snapshots / remove_orphan_files (default 7)
    retain-last=<n>         minimum snapshots to keep on expire_snapshots (default 100)
    target-file-size-bytes=<n>  target size for rewrite_data_files (default 512 MiB)
    dry-run=true|false      report table stats only, run no mutation (default: false)

Usage: ``uv run iceberg-maintenance table=accounts_mirror action=all dry-run=true``
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone

from iceberg_streaming.common import DATABASE, JobConfig

log = logging.getLogger("iceberg_streaming.maintenance")


def _older_than_ts(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def _report(spark, table: str, phase: str) -> None:
    try:
        snapshots = spark.sql(f"SELECT * FROM {table}.snapshots").count()
        manifests = spark.sql(f"SELECT * FROM {table}.manifests").count()
        files = spark.sql(
            f"SELECT count(*) AS file_count, coalesce(sum(file_size_in_bytes),0) AS bytes FROM {table}.files"
        ).first()
        log.warning(
            "[maintenance] stats(%s) table=%s snapshots=%s manifests=%s dataFiles=%s bytes=%s",
            phase, table, snapshots, manifests, files["file_count"], files["bytes"],
        )
    except Exception as exc:  # metadata table not available (e.g. brand new table)
        log.warning("[maintenance] stats(%s) unavailable for %s: %s", phase, table, exc)


def _rewrite_data_files(spark, cfg: JobConfig, table: str) -> None:
    where = cfg.arg("where")
    sort_order = cfg.arg("sort-order")
    target_size = cfg.arg("target-file-size-bytes", str(512 * 1024 * 1024))
    strategy = "binpack" if sort_order is None else "sort"
    call = f"CALL system.rewrite_data_files(table => '{table}', strategy => '{strategy}'"
    if sort_order is not None:
        call += f", sort_order => '{sort_order}'"
    if where is not None:
        call += ", where => '" + where.replace("'", "''") + "'"
    call += (
        ", options => map("
        "'rewrite-job-order','bytes-asc',"
        f"'target-file-size-bytes','{target_size}',"
        "'max-file-group-size-bytes','10737418240',"
        "'partial-progress.enabled','true',"
        "'partial-progress.max-commits','10',"
        "'max-concurrent-file-group-rewrites','10000'))"
    )
    log.warning("[maintenance] rewrite_data_files: %s", call)
    spark.sql(call).show(truncate=False)


def _rewrite_manifests(spark, table: str) -> None:
    log.warning("[maintenance] rewrite_manifests table=%s", table)
    spark.sql(f"CALL system.rewrite_manifests(table => '{table}')").show(truncate=False)


def _expire_snapshots(spark, cfg: JobConfig, table: str) -> None:
    older_than = _older_than_ts(int(cfg.arg("older-than-days", "7")))
    retain_last = int(cfg.arg("retain-last", "100"))
    call = (
        f"CALL system.expire_snapshots(table => '{table}', "
        f"older_than => TIMESTAMP '{older_than}', retain_last => {retain_last})"
    )
    log.warning("[maintenance] expire_snapshots: %s", call)
    spark.sql(call).show(truncate=False)


def _remove_orphan_files(spark, cfg: JobConfig, table: str) -> None:
    older_than = _older_than_ts(int(cfg.arg("older-than-days", "7")))
    call = f"CALL system.remove_orphan_files(table => '{table}', older_than => TIMESTAMP '{older_than}')"
    log.warning("[maintenance] remove_orphan_files (older_than=%s): %s", older_than, call)
    removed = spark.sql(call).count()
    log.warning("[maintenance] remove_orphan_files removed %s orphan file(s)", removed)


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("IcebergMaintenance")
    spark.sql(f"USE {DATABASE}")

    table = cfg.table()
    if table is None:
        from iceberg_streaming.common import usage

        raise ValueError("IcebergMaintenance requires table=<name>.\n" + usage())
    action = cfg.arg("action", "all").lower()
    dry_run = cfg.arg_bool("dry-run", False)

    log.warning("[maintenance] table=%s action=%s dryRun=%s", table, action, dry_run)
    _report(spark, table, "before")

    if dry_run:
        log.warning("[maintenance] dry-run: no mutation performed.")
        return

    run_all = action == "all"
    if run_all or action == "rewrite_data_files":
        _rewrite_data_files(spark, cfg, table)
    if run_all or action == "rewrite_manifests":
        _rewrite_manifests(spark, table)
    if run_all or action == "expire_snapshots":
        _expire_snapshots(spark, cfg, table)
    if run_all or action == "remove_orphan_files":
        _remove_orphan_files(spark, cfg, table)

    _report(spark, table, "after")
    log.warning("[maintenance] done for table=%s", table)


if __name__ == "__main__":
    main()

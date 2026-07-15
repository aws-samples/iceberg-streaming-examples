"""Shared SQL templates for the CDC "mirror" MERGE pattern.

Python counterpart of ``com.aws.emr.spark.cdc.CdcSql``. Used by the batch (:mod:`spark_cdc_mirror`),
snapshot-incremental (:mod:`spark_incremental_pipeline`) and continuous
(:mod:`spark_streaming_cdc_mirror`) jobs so the merge semantics cannot drift between them and can be
unit-tested without a Spark session.

Correctness properties (see the Java docstring and the README "CDC correctness assumptions"):

* **Deterministic dedup** -- one row per key ordered by the source sequence ``seq`` (a monotonic
  producer counter standing in for a database LSN), not by ``last_updated`` (which is not unique).
* **Stale-change guards** -- the matched UPDATE/DELETE branches only fire when ``c.seq >= a.seq``, so
  an older event arriving in a later batch can never overwrite or delete newer state. The winning
  ``seq`` is stored on the target so the comparison survives across batches and restarts.

Known residual limitation: the mirror uses **physical deletes** (deliberately, to exercise v2
positional deletes vs v3 deletion vectors). A truly stale insert arriving after a legitimate delete
is re-inserted, because a physically deleted row leaves no ``seq`` to compare against. Eliminating
that needs tombstones removed by a later maintenance pass -- see the README.
"""

from __future__ import annotations

#: Column list of the mirror tables, shared by the batch, incremental and streaming variants.
#: ``balance`` is in minor units (cents) and stays ``bigint`` end to end, matching the changelog -
#: money never touches a float.
MIRROR_COLUMNS_DDL = (
    "account_id bigint,\n"
    "          balance bigint,\n"
    "          last_updated timestamp,\n"
    "          seq bigint"
)

#: Partition spec of the mirror tables: bucketed on the merge key so the ON clause prunes.
MIRROR_PARTITION_DDL = "bucket(8, account_id)"

#: Column list of the changelog table written by ``cdc-log-change``.
CHANGELOG_COLUMNS_DDL = (
    "operation string,\n"
    "          account_id bigint,\n"
    "          balance bigint,\n"
    "          last_updated timestamp,\n"
    "          seq bigint"
)

#: Partition spec of the changelog table.
CHANGELOG_PARTITION_DDL = "days(last_updated), bucket(8, account_id)"


def mirror_merge(target_table: str, source_relation: str) -> str:
    """Build the deduplicate-then-MERGE statement for the mirror pattern.

    :param target_table: target mirror table (name or fully-qualified ``cat.db.table``)
    :param source_relation: source of change rows -- a view/table name (``accounts_batch``,
        ``accounts_source``) or an aliased subquery
        (``(SELECT * FROM accounts_changelog WHERE ...) src``). Must expose
        ``account_id, balance, last_updated, operation, seq``.
    """
    return f"""
        WITH windowed_changes AS (
            SELECT account_id, balance, last_updated, operation, seq,
                   row_number() OVER (
                       PARTITION BY account_id
                       ORDER BY seq DESC) AS row_num
            FROM {source_relation}
        ),
        accounts_changes AS (
            SELECT * FROM windowed_changes WHERE row_num = 1
        )
        MERGE INTO {target_table} a USING accounts_changes c
        ON a.account_id = c.account_id
        WHEN MATCHED AND c.operation = 'D' AND c.seq >= a.seq THEN DELETE
        WHEN MATCHED AND c.seq >= a.seq THEN UPDATE
            SET a.balance = c.balance, a.last_updated = c.last_updated, a.seq = c.seq
        WHEN NOT MATCHED AND c.operation != 'D' THEN
            INSERT (account_id, balance, last_updated, seq)
            VALUES (c.account_id, c.balance, c.last_updated, c.seq)
    """

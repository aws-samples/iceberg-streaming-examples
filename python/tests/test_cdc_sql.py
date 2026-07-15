"""Invariant tests for the shared CDC mirror MERGE SQL (parity with the Java CdcSqlTest).

These guard deterministic ordering by the source sequence and the stale-change guards on the matched
UPDATE/DELETE branches. Pure-Python: no Spark session.
"""

from __future__ import annotations

import re

from iceberg_streaming.cdc._sql import mirror_merge

SQL = mirror_merge("accounts_mirror", "accounts_batch")
FLAT = re.sub(r"\s+", " ", SQL).strip()


def test_target_and_source_interpolated():
    assert "MERGE INTO accounts_mirror a" in FLAT
    assert "FROM accounts_batch" in FLAT


def test_dedupes_by_seq_not_timestamp():
    assert "ORDER BY seq DESC" in FLAT
    assert "ORDER BY last_updated" not in FLAT


def test_update_and_delete_guarded():
    assert "WHEN MATCHED AND c.operation = 'D' AND c.seq >= a.seq THEN DELETE" in FLAT
    assert "WHEN MATCHED AND c.seq >= a.seq THEN UPDATE" in FLAT


def test_delete_branch_before_update_branch():
    assert 0 <= FLAT.index("THEN DELETE") < FLAT.index("THEN UPDATE")


def test_seq_persisted_on_insert_and_update():
    assert "a.seq = c.seq" in FLAT
    assert "INSERT (account_id, balance, last_updated, seq)" in FLAT


def test_inserts_only_non_delete_rows():
    assert "WHEN NOT MATCHED AND c.operation != 'D' THEN" in FLAT

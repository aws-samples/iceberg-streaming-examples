package com.aws.emr.spark.cdc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Invariant tests for the shared CDC mirror MERGE SQL produced by {@link CdcSql}. These guard the
 * two correctness properties the review called out: deterministic ordering by the source sequence,
 * and stale-change guards on the matched UPDATE/DELETE branches. They run without a Spark session.
 */
class CdcSqlTest {

  private final String sql = CdcSql.mirrorMerge("accounts_mirror", "accounts_batch");

  @Test
  void targetAndSourceAreInterpolated() {
    assertTrue(sql.contains("MERGE INTO accounts_mirror a"), "target table missing");
    assertTrue(sql.contains("FROM accounts_batch"), "source relation missing");
  }

  @Test
  void dedupesDeterministicallyBySeqNotTimestamp() {
    assertTrue(sql.contains("ORDER BY\n                           seq DESC") || sql.contains("ORDER BY seq DESC")
            || sql.replaceAll("\\s+", " ").contains("ORDER BY seq DESC"),
        "dedup must order by seq DESC");
    // Ordering by last_updated would be nondeterministic on equal timestamps.
    assertFalse(sql.replaceAll("\\s+", " ").contains("ORDER BY last_updated"),
        "must not order by last_updated");
  }

  @Test
  void updateAndDeleteAreGuardedAgainstStaleChanges() {
    String flat = sql.replaceAll("\\s+", " ");
    assertTrue(flat.contains("WHEN MATCHED AND c.operation = 'D' AND c.seq >= a.seq THEN DELETE"),
        "delete branch must be guarded by c.seq >= a.seq");
    assertTrue(flat.contains("WHEN MATCHED AND c.seq >= a.seq THEN UPDATE"),
        "update branch must be guarded by c.seq >= a.seq");
  }

  @Test
  void deleteBranchIsEvaluatedBeforeUpdateBranch() {
    String flat = sql.replaceAll("\\s+", " ");
    int deleteIdx = flat.indexOf("THEN DELETE");
    int updateIdx = flat.indexOf("THEN UPDATE");
    assertTrue(deleteIdx >= 0 && updateIdx >= 0 && deleteIdx < updateIdx,
        "the delete branch must come before the generic update branch");
  }

  @Test
  void seqIsPersistedOnInsertAndUpdate() {
    String flat = sql.replaceAll("\\s+", " ");
    assertTrue(flat.contains("a.seq = c.seq"), "update must persist the winning seq");
    assertTrue(flat.contains("INSERT (account_id, balance, last_updated, seq)"),
        "insert must include the seq column");
  }

  @Test
  void insertsOnlyNonDeleteRows() {
    assertTrue(sql.replaceAll("\\s+", " ").contains("WHEN NOT MATCHED AND c.operation != 'D' THEN"),
        "must not insert a tombstone as a live row");
  }
}

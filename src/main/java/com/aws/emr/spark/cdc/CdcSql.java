package com.aws.emr.spark.cdc;

/**
 * Centralised SQL templates for the CDC "mirror" MERGE pattern, shared by {@link SparkCDCMirror}
 * (batch), {@link SparkIncrementalPipeline} (snapshot-incremental) and {@link SparkStreamingCDCMirror}
 * (continuous). Keeping the SQL in one place means the batch, incremental and streaming variants
 * cannot drift apart and the merge semantics can be unit-tested (see {@code CdcSqlTest}) without a
 * Spark session.
 *
 * <h2>Correctness: deterministic ordering and stale-change guards</h2>
 *
 * <p>A CDC feed can deliver changes for the same key out of order (different Kafka partitions, retries,
 * a later micro-batch carrying an older event). Two rules keep the mirror correct regardless of arrival
 * order:
 *
 * <ol>
 *   <li><b>Deterministic dedup.</b> Within the source we keep one row per key ordered by the source
 *       sequence {@code seq} (a monotonically increasing number stamped by the producer, standing in
 *       for a database LSN), <em>not</em> by {@code last_updated}. Two events with the same millisecond
 *       timestamp would otherwise pick UPDATE vs DELETE arbitrarily.
 *   <li><b>Stale-change guards.</b> The matched UPDATE and DELETE branches only fire when
 *       {@code c.seq >= a.seq}, so an older event that arrives in a later batch can never overwrite or
 *       delete newer state. The winning {@code seq} is stored on the target so the comparison survives
 *       across batches and restarts.
 * </ol>
 *
 * <h2>Known residual limitation (documented, not hidden)</h2>
 *
 * <p>This mirror uses <b>physical deletes</b> (the row is removed), which is deliberate: the deletion of
 * matched rows on a merge-on-read target is exactly what exercises v2 positional delete files vs v3
 * deletion vectors in the benchmark. The trade-off is the classic CDC "resurrection" case: if a truly
 * stale insert/update for a key arrives in a batch <em>after</em> that key was legitimately deleted, the
 * {@code WHEN NOT MATCHED} branch re-inserts it, because a physically deleted row leaves no {@code seq}
 * to compare against. Eliminating that requires keeping tombstones (a soft-delete column or a separate
 * latest-sequence table) and removing them only in a later maintenance pass. See the README
 * ("CDC correctness assumptions") for the four standard options and why this showcase keeps physical
 * deletes.
 */
public final class CdcSql {

  private CdcSql() {}

  /**
   * Column list of the mirror tables, shared by the batch, incremental and streaming variants.
   * {@code balance} is in minor units (cents) and stays {@code bigint} end to end, matching the
   * changelog - money never touches a float.
   */
  public static final String MIRROR_COLUMNS_DDL =
      """
      account_id bigint,
                balance bigint,
                last_updated timestamp,
                seq bigint""";

  /** Partition spec of the mirror tables: bucketed on the merge key so the ON clause prunes. */
  public static final String MIRROR_PARTITION_DDL = "bucket(8, account_id)";

  /**
   * Build the deduplicate-then-MERGE statement for the mirror pattern.
   *
   * @param targetTable the target mirror table (a name or fully-qualified {@code cat.db.table})
   * @param sourceRelation the source of change rows: a view name (e.g. {@code accounts_batch}), a
   *     table name (e.g. {@code accounts_source}) or an aliased subquery (e.g.
   *     {@code (SELECT * FROM accounts_changelog WHERE ...) src}). Must expose the columns
   *     {@code account_id, balance, last_updated, operation, seq}.
   * @return a single Spark SQL statement
   */
  public static String mirrorMerge(String targetTable, String sourceRelation) {
    return String.format(
        """
            WITH windowed_changes AS (
                SELECT account_id, balance, last_updated, operation, seq,
                       row_number() OVER (
                           PARTITION BY account_id
                           ORDER BY seq DESC) AS row_num
                FROM %2$s
            ),
            accounts_changes AS (
                SELECT * FROM windowed_changes WHERE row_num = 1
            )
            MERGE INTO %1$s a USING accounts_changes c
            ON a.account_id = c.account_id
            WHEN MATCHED AND c.operation = 'D' AND c.seq >= a.seq THEN DELETE
            WHEN MATCHED AND c.seq >= a.seq THEN UPDATE
                SET a.balance = c.balance,
                    a.last_updated = c.last_updated,
                    a.seq = c.seq
            WHEN NOT MATCHED AND c.operation != 'D' THEN
                INSERT (account_id, balance, last_updated, seq)
                VALUES (c.account_id, c.balance, c.last_updated, c.seq)
            """,
        targetTable, sourceRelation);
  }
}

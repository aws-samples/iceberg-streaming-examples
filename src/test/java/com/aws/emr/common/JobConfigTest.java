package com.aws.emr.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JobConfig} argument parsing, defaults, catalog mapping, the typed accessors
 * and per-query checkpoint derivation. These are pure JVM tests: they never build a Spark session.
 */
class JobConfigTest {

  @Test
  void defaultsToLocalRuntimeAndCatalog() {
    JobConfig cfg = JobConfig.fromArgs(new String[] {});
    assertEquals(JobConfig.Runtime.LOCAL, cfg.runtime());
    assertEquals(JobConfig.Catalog.LOCAL, cfg.catalog());
    assertEquals("local", cfg.catalogName());
    assertEquals("warehouse", cfg.warehouse());
    // A fresh checkpoint default that is safe to derive per query.
    assertEquals("tmp/", cfg.checkpointLocation());
  }

  @Test
  void parsesKeyValueArgumentsOrderIndependently() {
    JobConfig cfg =
        JobConfig.fromArgs(new String[] {"catalog=glue", "warehouse=s3://b/wh", "runtime=emr"});
    assertEquals(JobConfig.Runtime.EMR, cfg.runtime());
    assertEquals(JobConfig.Catalog.GLUE, cfg.catalog());
    assertEquals("glue_catalog", cfg.catalogName());
    assertEquals("s3://b/wh", cfg.warehouse());
  }

  @Test
  void glueAndS3TablesRequireWarehouse() {
    assertThrows(IllegalArgumentException.class, () -> JobConfig.fromArgs(new String[] {"catalog=glue"}));
    assertThrows(
        IllegalArgumentException.class, () -> JobConfig.fromArgs(new String[] {"catalog=s3tables"}));
  }

  @Test
  void unknownCatalogThrows() {
    assertThrows(IllegalArgumentException.class, () -> JobConfig.fromArgs(new String[] {"catalog=nope"}));
  }

  @Test
  void typedAccessorsReadExampleSpecificArgs() {
    JobConfig cfg =
        JobConfig.fromArgs(
            new String[] {"table=accounts_mirror_v2", "fv=2", "fanout=false", "manifestmerge=false"});
    assertEquals("accounts_mirror_v2", cfg.table("accounts_mirror"));
    assertEquals("2", cfg.formatVersion("3"));
    assertFalse(cfg.fanout(true));
    assertFalse(cfg.manifestMerge(true));
  }

  @Test
  void typedAccessorsFallBackToDefaults() {
    JobConfig cfg = JobConfig.fromArgs(new String[] {});
    assertEquals("accounts_mirror", cfg.table("accounts_mirror"));
    assertEquals("3", cfg.formatVersion("3"));
    assertTrue(cfg.fanout(true));
    assertTrue(cfg.manifestMerge(true));
    assertEquals("latest", cfg.startingOffsets());
    assertEquals("fallback", cfg.arg("does-not-exist", "fallback"));
  }

  @Test
  void startingOffsetsIsConfigurable() {
    JobConfig cfg = JobConfig.fromArgs(new String[] {"startingOffsets=earliest"});
    assertEquals("earliest", cfg.startingOffsets());
  }

  @Test
  void checkpointForDerivesAUniquePerQueryPath() {
    JobConfig cfg = JobConfig.fromArgs(new String[] {"checkpoint=s3://bucket/cp"});
    assertEquals("s3://bucket/cp/streaming-cdc-mirror-accounts_v3", cfg.checkpointFor("streaming-cdc-mirror-accounts_v3"));
    // Two different queries must not collide.
    assertFalse(cfg.checkpointFor("q1").equals(cfg.checkpointFor("q2")));
  }

  @Test
  void checkpointForNormalisesTrailingSlashAndUnsafeChars() {
    JobConfig cfg = JobConfig.fromArgs(new String[] {"checkpoint=tmp/"});
    // trailing slash on the base is not doubled
    assertEquals("tmp/cdc-log-change", cfg.checkpointFor("cdc-log-change"));
    // unsafe characters are replaced so the path stays valid
    assertEquals("tmp/a_b_c", cfg.checkpointFor("a/b c"));
  }
}

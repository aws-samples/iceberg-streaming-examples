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

  // ------------------------------------------------------------------ table / behaviour knobs

  @Test
  void tableKnobsHaveSensibleDefaults() {
    JobConfig cfg = JobConfig.fromArgs(new String[] {});
    assertEquals(JobConfig.Mode.COW, cfg.mode(JobConfig.Mode.COW));
    assertEquals(JobConfig.Mode.MOR, cfg.mode(JobConfig.Mode.MOR)); // per-job default respected
    assertEquals(JobConfig.FileFormat.PARQUET, cfg.fileFormat(JobConfig.FileFormat.PARQUET));
    assertFalse(cfg.objectStorage(false));
    assertEquals(JobConfig.Source.PROTO, cfg.source());
    assertEquals("telemetry-proto", cfg.topic());
    assertEquals(JobConfig.Dedup.NONE, cfg.dedup(JobConfig.Dedup.NONE));
    assertEquals(JobConfig.Compaction.NONE, cfg.compactionMode(JobConfig.Compaction.NONE));
    assertEquals("120 seconds", cfg.watermarkDelay());
    assertEquals("eu-west-1", cfg.region());
  }

  @Test
  void tableKnobsParse() {
    JobConfig cfg =
        JobConfig.fromArgs(
            new String[] {
              "mode=mor", "fileformat=orc", "objectstorage=true", "source=json",
              "dedup=batch", "compaction=scheduled", "fv=2"
            });
    assertEquals(JobConfig.Mode.MOR, cfg.mode(JobConfig.Mode.COW));
    assertEquals(JobConfig.FileFormat.ORC, cfg.fileFormat(JobConfig.FileFormat.PARQUET));
    assertTrue(cfg.objectStorage(false));
    assertEquals(JobConfig.Source.JSON, cfg.source());
    assertEquals("telemetry-json", cfg.topic());
    assertEquals(JobConfig.Dedup.BATCH, cfg.dedup(JobConfig.Dedup.NONE));
    assertEquals(JobConfig.Compaction.SCHEDULED, cfg.compactionMode(JobConfig.Compaction.NONE));
    assertEquals("2", cfg.formatVersion("3"));
  }

  @Test
  void legacyBooleanDedupAndCompactionStillAccepted() {
    JobConfig cfg = JobConfig.fromArgs(new String[] {"dedup=true", "compaction=true"});
    assertEquals(JobConfig.Dedup.MERGE, cfg.dedup(JobConfig.Dedup.NONE));
    assertEquals(JobConfig.Compaction.INLINE, cfg.compactionMode(JobConfig.Compaction.NONE));
    JobConfig off = JobConfig.fromArgs(new String[] {"dedup=false", "compaction=false"});
    assertEquals(JobConfig.Dedup.NONE, off.dedup(JobConfig.Dedup.MERGE));
    assertEquals(JobConfig.Compaction.NONE, off.compactionMode(JobConfig.Compaction.INLINE));
  }

  @Test
  void invalidKnobValuesThrow() {
    assertThrows(IllegalArgumentException.class,
        () -> JobConfig.fromArgs(new String[] {"mode=upsert"}).mode(JobConfig.Mode.COW));
    assertThrows(IllegalArgumentException.class,
        () -> JobConfig.fromArgs(new String[] {"fileformat=csv"}).fileFormat(JobConfig.FileFormat.PARQUET));
    assertThrows(IllegalArgumentException.class,
        () -> JobConfig.fromArgs(new String[] {"dedup=maybe"}).dedup(JobConfig.Dedup.NONE));
    assertThrows(IllegalArgumentException.class,
        () -> JobConfig.fromArgs(new String[] {"fv=4"}).formatVersion("3"));
    assertThrows(IllegalArgumentException.class,
        () -> JobConfig.fromArgs(new String[] {"source=xml"}).source());
  }

  @Test
  void tablePropertiesFollowTheKnobs() {
    JobConfig cfg = JobConfig.fromArgs(new String[] {"mode=mor", "fv=2", "fileformat=orc", "objectstorage=true"});
    var props = cfg.tablePropertiesMap(JobConfig.Mode.COW, java.util.Map.of());
    assertEquals("2", props.get("format-version"));
    assertEquals("orc", props.get("write.format.default"));
    assertEquals("merge-on-read", props.get("write.merge.mode"));
    assertEquals("hash", props.get("write.merge.distribution-mode"));
    assertEquals("true", props.get("write.object-storage.enabled"));
    // Format-specific compression only for the format in use: no parquet tuning on an ORC table.
    assertEquals("zstd", props.get("write.orc.compression-codec"));
    assertFalse(props.containsKey("write.parquet.compression-codec"));
  }

  @Test
  void tablePropertiesDefaultsAreCopyOnWriteParquet() {
    JobConfig cfg = JobConfig.fromArgs(new String[] {});
    var props = cfg.tablePropertiesMap(JobConfig.Mode.COW, java.util.Map.of());
    assertEquals("3", props.get("format-version"));
    assertEquals("parquet", props.get("write.format.default"));
    assertEquals("copy-on-write", props.get("write.merge.mode"));
    assertFalse(props.containsKey("write.object-storage.enabled"));
    assertEquals("zstd", props.get("write.parquet.compression-codec"));
    // overrides win
    var overridden =
        cfg.tablePropertiesMap(JobConfig.Mode.COW, java.util.Map.of("commit.retry.num-retries", "100"));
    assertEquals("100", overridden.get("commit.retry.num-retries"));
  }

  @Test
  void createTableDdlInterpolatesEverything() {
    JobConfig cfg = JobConfig.fromArgs(new String[] {"mode=mor"});
    String ddl = cfg.createTableDdl("t1", "id bigint, ts timestamp", "hours(ts)", JobConfig.Mode.COW,
        java.util.Map.of());
    assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS t1"));
    assertTrue(ddl.contains("PARTITIONED BY (hours(ts))"));
    assertTrue(ddl.contains("'write.merge.mode'='merge-on-read'"));
  }

  @Test
  void triggerParsesSecondsAndAvailableNow() {
    assertEquals(
        org.apache.spark.sql.streaming.Trigger.ProcessingTime(60, java.util.concurrent.TimeUnit.SECONDS),
        JobConfig.fromArgs(new String[] {}).trigger(60));
    assertEquals(
        org.apache.spark.sql.streaming.Trigger.ProcessingTime(5, java.util.concurrent.TimeUnit.SECONDS),
        JobConfig.fromArgs(new String[] {"trigger=5"}).trigger(60));
    assertEquals(
        org.apache.spark.sql.streaming.Trigger.AvailableNow(),
        JobConfig.fromArgs(new String[] {"trigger=availablenow"}).trigger(60));
    assertThrows(IllegalArgumentException.class,
        () -> JobConfig.fromArgs(new String[] {"trigger=often"}).trigger(60));
  }
}


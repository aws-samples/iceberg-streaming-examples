"""Unit tests for JobConfig parsing, defaults, typed accessors and per-query checkpoints.

Pure-Python: no Spark session is created.
"""

from __future__ import annotations

import pytest

from iceberg_streaming.common import Catalog, Compaction, Dedup, FileFormat, JobConfig, Mode, Runtime, Source


def test_defaults_to_local():
    cfg = JobConfig.from_args([])
    assert cfg.runtime is Runtime.LOCAL
    assert cfg.catalog is Catalog.LOCAL
    assert cfg.catalog_name == "local"
    assert cfg.warehouse == "warehouse"
    assert cfg.checkpoint_location == "tmp/"


def test_parses_key_value_order_independent():
    cfg = JobConfig.from_args(["catalog=glue", "warehouse=s3://b/wh", "runtime=emr"])
    assert cfg.runtime is Runtime.EMR
    assert cfg.catalog is Catalog.GLUE
    assert cfg.catalog_name == "glue_catalog"
    assert cfg.warehouse == "s3://b/wh"


def test_glue_and_s3tables_require_warehouse():
    with pytest.raises(ValueError):
        JobConfig.from_args(["catalog=glue"])
    with pytest.raises(ValueError):
        JobConfig.from_args(["catalog=s3tables"])


def test_unknown_catalog_raises():
    with pytest.raises(ValueError):
        JobConfig.from_args(["catalog=nope"])


def test_typed_accessors_read_example_args():
    cfg = JobConfig.from_args(["table=accounts_mirror_v2", "fv=2", "fanout=false", "manifestmerge=false"])
    assert cfg.table("accounts_mirror") == "accounts_mirror_v2"
    assert cfg.format_version("3") == "2"
    assert cfg.fanout(True) is False
    assert cfg.manifest_merge(True) is False


def test_typed_accessors_fall_back_to_defaults():
    cfg = JobConfig.from_args([])
    assert cfg.table("accounts_mirror") == "accounts_mirror"
    assert cfg.format_version("3") == "3"
    assert cfg.fanout(True) is True
    assert cfg.manifest_merge(True) is True
    assert cfg.starting_offsets() == "latest"
    assert cfg.arg("does-not-exist", "fallback") == "fallback"


def test_starting_offsets_configurable():
    cfg = JobConfig.from_args(["startingOffsets=earliest"])
    assert cfg.starting_offsets() == "earliest"


def test_checkpoint_for_is_unique_per_query():
    cfg = JobConfig.from_args(["checkpoint=s3://bucket/cp"])
    assert cfg.checkpoint_for("streaming-cdc-mirror-accounts_v3") == "s3://bucket/cp/streaming-cdc-mirror-accounts_v3"
    assert cfg.checkpoint_for("q1") != cfg.checkpoint_for("q2")


def test_checkpoint_for_normalises():
    cfg = JobConfig.from_args(["checkpoint=tmp/"])
    assert cfg.checkpoint_for("cdc-log-change") == "tmp/cdc-log-change"
    assert cfg.checkpoint_for("a/b c") == "tmp/a_b_c"


# ---------------------------------------------------------------------- table / behaviour knobs


def test_table_knobs_have_sensible_defaults():
    cfg = JobConfig.from_args([])
    assert cfg.mode(Mode.COW) is Mode.COW
    assert cfg.mode(Mode.MOR) is Mode.MOR  # per-job default respected
    assert cfg.file_format() is FileFormat.PARQUET
    assert cfg.object_storage() is False
    assert cfg.source() is Source.PROTO
    assert cfg.topic() == "telemetry-proto"
    assert cfg.dedup(Dedup.NONE) is Dedup.NONE
    assert cfg.compaction_mode(Compaction.NONE) is Compaction.NONE
    assert cfg.watermark_delay() == "120 seconds"
    assert cfg.region() == "eu-west-1"


def test_table_knobs_parse():
    cfg = JobConfig.from_args(
        ["mode=mor", "fileformat=orc", "objectstorage=true", "source=json", "dedup=batch",
         "compaction=scheduled", "fv=2"]
    )
    assert cfg.mode(Mode.COW) is Mode.MOR
    assert cfg.file_format() is FileFormat.ORC
    assert cfg.object_storage() is True
    assert cfg.source() is Source.JSON
    assert cfg.topic() == "telemetry-json"
    assert cfg.dedup(Dedup.NONE) is Dedup.BATCH
    assert cfg.compaction_mode(Compaction.NONE) is Compaction.SCHEDULED
    assert cfg.format_version() == "2"


def test_legacy_boolean_dedup_and_compaction_still_accepted():
    cfg = JobConfig.from_args(["dedup=true", "compaction=true"])
    assert cfg.dedup(Dedup.NONE) is Dedup.MERGE
    assert cfg.compaction_mode(Compaction.NONE) is Compaction.INLINE
    off = JobConfig.from_args(["dedup=false", "compaction=false"])
    assert off.dedup(Dedup.MERGE) is Dedup.NONE
    assert off.compaction_mode(Compaction.INLINE) is Compaction.NONE


def test_invalid_knob_values_raise():
    with pytest.raises(ValueError):
        JobConfig.from_args(["mode=upsert"]).mode(Mode.COW)
    with pytest.raises(ValueError):
        JobConfig.from_args(["fileformat=csv"]).file_format()
    with pytest.raises(ValueError):
        JobConfig.from_args(["dedup=maybe"]).dedup(Dedup.NONE)
    with pytest.raises(ValueError):
        JobConfig.from_args(["fv=4"]).format_version()
    with pytest.raises(ValueError):
        JobConfig.from_args(["source=xml"]).source()
    with pytest.raises(ValueError):
        JobConfig.from_args(["trigger=often"]).trigger_kwargs(60)


def test_table_properties_follow_the_knobs():
    cfg = JobConfig.from_args(["mode=mor", "fv=2", "fileformat=orc", "objectstorage=true"])
    props = cfg.table_properties_map(Mode.COW)
    assert props["format-version"] == "2"
    assert props["write.format.default"] == "orc"
    assert props["write.merge.mode"] == "merge-on-read"
    assert props["write.merge.distribution-mode"] == "hash"
    assert props["write.object-storage.enabled"] == "true"
    # Format-specific compression only for the format in use: no parquet tuning on an ORC table.
    assert props["write.orc.compression-codec"] == "zstd"
    assert "write.parquet.compression-codec" not in props


def test_table_properties_defaults_are_cow_parquet():
    cfg = JobConfig.from_args([])
    props = cfg.table_properties_map(Mode.COW)
    assert props["format-version"] == "3"
    assert props["write.format.default"] == "parquet"
    assert props["write.merge.mode"] == "copy-on-write"
    assert "write.object-storage.enabled" not in props
    assert props["write.parquet.compression-codec"] == "zstd"
    # overrides win
    overridden = cfg.table_properties_map(Mode.COW, {"commit.retry.num-retries": "100"})
    assert overridden["commit.retry.num-retries"] == "100"


def test_create_table_ddl_interpolates_everything():
    cfg = JobConfig.from_args(["mode=mor"])
    ddl = cfg.create_table_ddl("t1", "id bigint, ts timestamp", "hours(ts)", Mode.COW)
    assert "CREATE TABLE IF NOT EXISTS t1" in ddl
    assert "PARTITIONED BY (hours(ts))" in ddl
    assert "'write.merge.mode'='merge-on-read'" in ddl


def test_trigger_parses_seconds_and_availablenow():
    assert JobConfig.from_args([]).trigger_kwargs(60) == {"processingTime": "60 seconds"}
    assert JobConfig.from_args(["trigger=5"]).trigger_kwargs(60) == {"processingTime": "5 seconds"}
    assert JobConfig.from_args(["trigger=availablenow"]).trigger_kwargs(60) == {"availableNow": True}

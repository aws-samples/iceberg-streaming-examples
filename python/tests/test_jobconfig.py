"""Unit tests for JobConfig parsing, defaults, typed accessors and per-query checkpoints.

Pure-Python: no Spark session is created.
"""

from __future__ import annotations

import pytest

from iceberg_streaming.common import Catalog, JobConfig, Runtime


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

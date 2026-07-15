"""Unit tests for the deterministic scenario model (the oracle).

Pure-Python: no Spark, no Kafka. These pin the properties the end-to-end runner relies on, and prove
the correctness guarantees the guarded MERGE must satisfy — most importantly that for the guarded,
delete-free workload the expected final state is independent of arrival order.
"""

from __future__ import annotations

import pytest

from iceberg_streaming.scenarios import SCENARIO_NAMES, build
from iceberg_streaming.scenarios.events import Event, _max_seq_state


def test_all_registered_scenarios_build():
    for name in SCENARIO_NAMES:
        s = build(name, seed=1, keys=10, events_per_key=5)
        assert s.events, f"{name} produced no events"
        assert s.name == name


def test_build_is_deterministic():
    a = build("cdc-out-of-order", seed=7, keys=20)
    b = build("cdc-out-of-order", seed=7, keys=20)
    assert [e.to_csv() for e in a.events] == [e.to_csv() for e in b.events]
    assert a.expected_state == b.expected_state


def test_unknown_scenario_raises():
    with pytest.raises(KeyError):
        build("does-not-exist")


def test_seq_is_unique_and_dense():
    s = build("cdc-ordered", seed=3, keys=30)
    seqs = sorted(e.seq for e in s.events)
    assert seqs == list(range(len(seqs))), "seq must be a dense 0..n-1 range"


def test_out_of_order_expected_state_is_arrival_order_independent():
    """The key property: reshuffling arrival order must not change the expected final state."""
    s = build("cdc-out-of-order", seed=11, keys=40)
    import random

    reshuffled = list(s.events)
    random.Random(999).shuffle(reshuffled)
    # Oracle over any permutation of the same events yields the same result (max-seq per key).
    assert _max_seq_state(reshuffled) == s.expected_state
    # And out-of-order has no deletes, so every key survives.
    assert len(s.expected_state) == 40


def test_out_of_order_is_actually_shuffled_and_multibatch():
    s = build("cdc-out-of-order", seed=11, keys=40)
    in_seq_order = [e.seq for e in s.events] == sorted(e.seq for e in s.events)
    assert not in_seq_order, "arrival order should differ from seq order"
    assert s.batch < len(s.events), "batch size must force multiple micro-batches"


def test_cdc_ordered_arrival_equals_seq_order_and_has_deletes():
    s = build("cdc-ordered", seed=5, keys=60)
    assert [e.seq for e in s.events] == sorted(e.seq for e in s.events)
    assert any(e.operation == "D" for e in s.events)
    # A key whose max-seq event is a delete must be absent from the expected state.
    latest: dict[int, Event] = {}
    for e in s.events:
        if e.account_id not in latest or e.seq > latest[e.account_id].seq:
            latest[e.account_id] = e
    for aid, e in latest.items():
        assert (aid in s.expected_state) == (e.operation != "D")


def test_append_only_dedupes_duplicates():
    s = build("append-only", seed=2, keys=25)
    # duplicate re-sends -> more events than keys, but exactly one surviving row per key
    assert len(s.events) > len(s.expected_state)
    assert len(s.expected_state) == 25


def test_resurrection_demo_expects_resurrected_rows():
    """Demonstrates the documented physical-delete limitation: the late insert wins, row present."""
    s = build("resurrection-demo", seed=4, keys=15)
    assert len(s.expected_state) == 15  # every key resurrected despite a higher-seq delete
    # deletes are produced before their inserts (arrival order)
    first_ops = [e.operation for e in s.events[:15]]
    assert set(first_ops) == {"D"}


def test_mor_v2_and_v3_share_state_but_differ_in_format():
    v2 = build("mor-v2", seed=8, keys=30)
    v3 = build("mor-v3", seed=8, keys=30)
    assert v2.format_version == "2"
    assert v3.format_version == "3"
    assert v2.expected_state == v3.expected_state

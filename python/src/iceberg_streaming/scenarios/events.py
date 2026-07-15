"""Deterministic scenario model for the CDC mirror harness.

This module is **pure Python** (no Spark, no Kafka) so it can be unit-tested in CI and used as the
*oracle* the end-to-end runner asserts against. It generates a fixed, seeded stream of CDC events and
computes the exact final mirror state the shipped guarded MERGE (see
:mod:`iceberg_streaming.cdc._sql`) must produce after consuming them.

Each scenario carries its events **in arrival order** (the order the runner produces/consumes them),
which may differ from the source-sequence (``seq``) order. The whole point of the correctness
scenarios is that, for the guarded workloads, the final state is independent of arrival order.

Correctness properties encoded here (matching the shipped SQL):

* dedup keeps the highest ``seq`` per key within a batch;
* matched UPDATE/DELETE only apply when ``c.seq >= a.seq`` (stale changes cannot overwrite newer
  state), so after consuming *all* events the surviving row per key is the one with the global max
  ``seq`` -- present unless that max-``seq`` event is a delete;
* physical deletes mean a stale INSERT arriving *after* a delete resurrects the row (documented
  limitation) -- the ``resurrection-demo`` scenario reproduces and asserts exactly this.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

# Fixed epoch-millis base so timestamps are stable across runs.
_BASE_TS = 1_700_000_000_000


@dataclass(frozen=True)
class Event:
    """A single DMS-like CDC event. ``operation`` is ``I`` (insert), ``U`` (update) or ``D`` (delete)."""

    operation: str
    account_id: int
    balance: int
    last_updated: int  # epoch millis
    seq: int

    def to_csv(self) -> str:
        """Wire format consumed by the jobs: ``operation,account_id,balance,last_updated,seq``."""
        return f"{self.operation},{self.account_id},{self.balance},{self.last_updated},{self.seq}"


@dataclass(frozen=True)
class Scenario:
    """A named, deterministic scenario: arrival-ordered events + the expected final mirror state."""

    name: str
    events: list[Event]
    #: account_id -> (balance, seq) for every row that MUST be present in the mirror at the end.
    expected_state: dict[int, tuple[int, int]]
    format_version: str = "3"
    #: Recommended micro-batch size (maxOffsetsPerTrigger / memory chunk) so out-of-order events span
    #: multiple batches and genuinely exercise the cross-batch guards.
    batch: int = 100
    note: str = ""
    meta: dict = field(default_factory=dict)


def _max_seq_state(events: list[Event]) -> dict[int, tuple[int, int]]:
    """Oracle for the guarded workloads: the max-``seq`` event per key wins; a delete means absent.

    Valid only when the dataset has no "resurrection" (a stale insert arriving after a delete). The
    generators below guarantee this for every scenario that uses this oracle (either no deletes, or
    arrival order == seq order).
    """
    latest: dict[int, Event] = {}
    for e in events:
        cur = latest.get(e.account_id)
        if cur is None or e.seq > cur.seq:
            latest[e.account_id] = e
    return {aid: (e.balance, e.seq) for aid, e in latest.items() if e.operation != "D"}


def _append_only(rnd: random.Random, keys: int) -> Scenario:
    """Insert-only, unique keys, with duplicate re-sends; arrival shuffled. Tests in-batch dedup."""
    events: list[Event] = []
    seq = 0
    for k in range(1, keys + 1):
        bal = rnd.randint(1000, 9999)
        e = Event("I", k, bal, _BASE_TS + seq, seq)
        events.append(e)
        events.append(e)  # duplicate re-send (same seq) -> must be deduped to one row
        seq += 1
    rnd.shuffle(events)
    return Scenario(
        name="append-only",
        events=events,
        expected_state=_max_seq_state(events),
        note="insert-only with duplicate re-sends; every key present exactly once",
    )


def _lifecycle(rnd: random.Random, keys: int, events_per_key: int, with_deletes: bool) -> list[Event]:
    """Per key: one insert, some updates, optionally a terminal delete. Returned in seq order."""
    logical: list[Event] = []
    seq = 0
    for k in range(1, keys + 1):
        n = rnd.randint(1, events_per_key)
        logical.append(Event("I", k, rnd.randint(1000, 9999), _BASE_TS + seq, seq))
        seq += 1
        for _ in range(n - 1):
            logical.append(Event("U", k, rnd.randint(1000, 9999), _BASE_TS + seq, seq))
            seq += 1
        if with_deletes and rnd.random() < 0.3:
            logical.append(Event("D", k, 0, _BASE_TS + seq, seq))
            seq += 1
    return logical


def _cdc_ordered(rnd: random.Random, keys: int, events_per_key: int, fv: str = "3") -> Scenario:
    """Full I/U/D lifecycle, arrival == seq order (in-order). Tests updates + terminal deletes."""
    logical = _lifecycle(rnd, keys, events_per_key, with_deletes=True)
    logical.sort(key=lambda e: e.seq)  # arrival order == seq order
    return Scenario(
        name=f"cdc-ordered-v{fv}" if fv != "3" else "cdc-ordered",
        events=logical,
        expected_state=_max_seq_state(logical),
        format_version=fv,
        note="in-order I/U/D lifecycle; ~30% of keys end deleted",
        meta={"has_deletes": True},
    )


def _cdc_out_of_order(rnd: random.Random, keys: int, events_per_key: int) -> Scenario:
    """Inserts + updates only (NO deletes), arrival shuffled, multi-batch.

    This is the key regression test for the stale-change guards: a stale update landing in a later
    micro-batch must NOT overwrite newer state. Without ``c.seq >= a.seq`` guards the final balances
    would be wrong and the assertion fails. No deletes -> no resurrection -> the max-seq oracle is
    exact regardless of arrival order.
    """
    logical = _lifecycle(rnd, keys, events_per_key, with_deletes=False)
    shuffled = list(logical)
    rnd.shuffle(shuffled)
    return Scenario(
        name="cdc-out-of-order",
        events=shuffled,
        expected_state=_max_seq_state(logical),
        batch=max(1, keys // 4),  # force several batches so stale updates cross batch boundaries
        note="shuffled inserts+updates; stale updates must not overwrite newer rows",
    )


def _resurrection_demo(rnd: random.Random, keys: int) -> Scenario:
    """Reproduce the documented physical-delete limitation.

    Per key: an insert (low seq) and a delete (high seq). Arrival puts every delete BEFORE its insert
    (as if the insert event was delayed) and in separate batches. Ideal tombstone semantics would end
    with the key absent; with physical deletes the late insert resurrects it. We assert the *actual*
    (resurrected) state so the limitation is a demonstrated, tested fact rather than just prose.
    """
    inserts: list[Event] = []
    deletes: list[Event] = []
    seq = 0
    for k in range(1, keys + 1):
        ins = Event("I", k, rnd.randint(1000, 9999), _BASE_TS + seq, seq)
        seq += 1
        dele = Event("D", k, 0, _BASE_TS + seq, seq)
        seq += 1
        inserts.append(ins)
        deletes.append(dele)
    arrival = deletes + inserts  # all deletes first (batch 1), all inserts second (batch 2)
    expected = {ins.account_id: (ins.balance, ins.seq) for ins in inserts}
    return Scenario(
        name="resurrection-demo",
        events=arrival,
        expected_state=expected,
        batch=keys,  # exactly one batch of deletes, then one batch of inserts
        note="stale insert after delete resurrects the row (documented physical-delete limitation)",
        meta={"has_deletes": True, "demonstrates_limitation": True},
    )


#: Registry of implemented scenarios: name -> builder(rnd, keys, events_per_key).
_BUILDERS = {
    "append-only": lambda rnd, keys, epk: _append_only(rnd, keys),
    "cdc-ordered": lambda rnd, keys, epk: _cdc_ordered(rnd, keys, epk),
    "cdc-out-of-order": lambda rnd, keys, epk: _cdc_out_of_order(rnd, keys, epk),
    "resurrection-demo": lambda rnd, keys, epk: _resurrection_demo(rnd, keys),
    # v2 vs v3 use the identical in-order workload; only the target format-version differs. Assert the
    # final state is identical and let the runner report the delete-encoding metadata difference.
    "mor-v2": lambda rnd, keys, epk: _cdc_ordered(rnd, keys, epk, fv="2"),
    "mor-v3": lambda rnd, keys, epk: _cdc_ordered(rnd, keys, epk, fv="3"),
}

SCENARIO_NAMES = tuple(_BUILDERS.keys())


def build(name: str, seed: int = 42, keys: int = 50, events_per_key: int = 8) -> Scenario:
    """Build a named scenario deterministically from ``seed``.

    :raises KeyError: if ``name`` is not one of :data:`SCENARIO_NAMES`.
    """
    if name not in _BUILDERS:
        raise KeyError(f"unknown scenario '{name}'. Available: {', '.join(SCENARIO_NAMES)}")
    rnd = random.Random(seed)
    scenario = _BUILDERS[name](rnd, keys, events_per_key)
    # mor-v2/mor-v3 keep their registry name rather than the internal cdc-ordered label.
    if name in ("mor-v2", "mor-v3"):
        object.__setattr__(scenario, "name", name)
    return scenario

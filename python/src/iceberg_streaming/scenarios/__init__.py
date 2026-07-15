"""Deterministic end-to-end scenario harness for the CDC mirror patterns.

:mod:`events` is pure Python (the seeded dataset + expected-final-state oracle) and is unit-tested in
CI. :mod:`runner` drives a real local Spark + Iceberg (+ optional Kafka) run and asserts the final
table state against the oracle.
"""

from iceberg_streaming.scenarios.events import SCENARIO_NAMES, Event, Scenario, build

__all__ = ["build", "SCENARIO_NAMES", "Event", "Scenario"]

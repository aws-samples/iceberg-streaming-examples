"""Kafka producer simulating a DMS-like CDC feed of bank-account changes.

Python counterpart of ``com.aws.emr.kafka.KafkaCDCSimulator``, consumed by the CDC examples
(``cdc-log-change``, ``cdc-streaming-mirror``, ...).

Wire format (CSV): ``operation,account_id,balance,last_updated(epoch millis),seq``. ``operation`` is
``I`` (first change seen for a key), ``U`` or ``D``. The trailing ``seq`` is a **monotonic source
sequence** (an LSN surrogate) that gives the downstream MERGE a deterministic total order across
keys, partitions and retries (see :mod:`iceberg_streaming.cdc._sql`). ``balance`` is in minor units
(cents), ``bigint`` end to end - money never touches a float.

Records are sent **without a Kafka key by default**, so changes for one account scatter across
partitions and arrive genuinely out of order - the condition the ``seq``-guarded MERGE exists for.
Pass ``keyed=true`` to key by account id (per-key ordering preserved).

Workload shape: 80% of the changes hit a small "hot" key set (row-level delete churn on the same
data files - the deletion-vector workload), the rest spread across a large long tail; ~85% updates /
~15% deletes after a key's first change.

Usage::

    uv run cdc-simulator [bootstrap=host:port] [count=<n|0=forever>] [rate=<msgs/sec|0=unthrottled>]
        [hot=<n>] [accounts=<n>] [keyed=bool] [topic=<name>]
"""

from __future__ import annotations

import random
import sys
import time

from kafka import KafkaProducer

from iceberg_streaming.kafka.telemetry_producer import _bool, _kv

TOPIC = "streaming-cdc-log-ingest"


def main(argv: list[str] | None = None) -> None:
    kv = _kv(argv if argv is not None else sys.argv[1:])
    bootstrap = kv.get("bootstrap", "localhost:9092")
    topic = kv.get("topic", TOPIC)
    count = int(kv.get("count", "0"))
    rate = int(kv.get("rate", "20000"))
    hot_keys = int(kv.get("hot", "100000"))
    total_keys = int(kv.get("accounts", "2000000"))
    keyed = _bool(kv, "keyed", False)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode() if k is not None else None,
        value_serializer=lambda v: v.encode(),
        # --- high-throughput producer tuning; acks=1 disables idempotence on purpose: retries may
        # duplicate records (same seq re-delivered), which the seq-guarded MERGE absorbs.
        acks=1,
        compression_type="zstd",
        batch_size=262144,
        linger_ms=50,
        buffer_memory=268435456,
        max_request_size=10485760,
    )
    print(
        f"KafkaCDCSimulator -> topic={topic} bootstrap={bootstrap} count={count or 'unbounded'} "
        f"rate={rate or 'unthrottled'} hot={hot_keys} accounts={total_keys} keyed={keyed}"
    )

    seen: set[int] = set()
    seq = 0
    sent = 0
    try:
        while count == 0 or sent < count:
            # 80% of the changes hit the small hot key set, 20% the long tail.
            account_id = random.randrange(hot_keys) if random.randrange(100) < 80 else random.randrange(total_keys)
            if account_id not in seen:
                seen.add(account_id)
                operation = "I"
            else:
                # ~85% updates, ~15% deletes after the first change.
                operation = "D" if random.randrange(100) < 15 else "U"
            balance_cents = random.randrange(1_000, 100_000_000)  # minor units, bigint end to end
            value = f"{operation},{account_id},{balance_cents},{int(time.time() * 1000)},{seq}"
            producer.send(topic, key=str(account_id) if keyed else None, value=value)
            seq += 1
            sent += 1
            if sent % 1_000_000 == 0:
                print(f"{sent} records produced...")
            if rate >= 10 and sent % (rate // 10) == 0:
                time.sleep(0.1)
    except KeyboardInterrupt:
        pass
    producer.flush()
    producer.close()
    print(f"Done, {sent} records produced to {topic}.")


if __name__ == "__main__":
    main()

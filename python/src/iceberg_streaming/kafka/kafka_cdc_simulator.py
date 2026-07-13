"""Kafka CDC simulator.

Counterpart of ``com.aws.emr.spark.cdc.simulator.KafkaCDCSimulator``: produces DMS-like CDC records
as comma separated strings ``operation,account_id,balance,last_updated_millis`` to the
``streaming-cdc-log-ingest`` topic, which the ``cdc-log-change`` Spark job consumes.

Operations: ``I`` (insert), ``U`` (update), ``D`` (delete).

Usage: ``uv run cdc-simulator [bootstrap=host:port] [count=1000] [accounts=100]``
"""

from __future__ import annotations

import random
import sys
import time

from kafka import KafkaProducer

TOPIC = "streaming-cdc-log-ingest"


def main(argv: list[str] | None = None) -> None:
    args = {a.split("=", 1)[0]: a.split("=", 1)[1] for a in (argv or sys.argv[1:]) if "=" in a}
    bootstrap = args.get("bootstrap", "localhost:9092")
    count = int(args.get("count", "1000"))
    accounts = int(args.get("accounts", "100"))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        # --- high-throughput producer tuning ---
        acks=1,
        compression_type="zstd",
        batch_size=262144,  # 256 KiB
        linger_ms=50,
        buffer_memory=268435456,  # 256 MiB
        max_request_size=10485760,
    )
    print(f"Producing {count} CDC records to {TOPIC} on {bootstrap} ...")
    seen: set[int] = set()
    for _ in range(count):
        account_id = random.randint(1, accounts)
        if account_id not in seen:
            operation = "I"
            seen.add(account_id)
        else:
            operation = random.choices(["U", "D"], weights=[9, 1])[0]
        balance = random.randint(0, 1_000_000)
        millis = int(time.time() * 1000)
        value = f"{operation},{account_id},{balance},{millis}"
        producer.send(TOPIC, key=str(account_id), value=value)
    producer.flush()
    producer.close()
    print("Done.")


if __name__ == "__main__":
    main()

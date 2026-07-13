"""Native Kafka JSON producer.

Simplified counterpart of ``com.aws.emr.json.kafka.producer.JsonProducerSchemaRegistry`` -- the Java
version used the Glue Schema Registry, this one just produces plain JSON ``Employee`` records to
``json-demo-topic-pure`` (there is no first-class Python Glue Schema Registry JSON serde).

Usage: ``uv run json-producer [bootstrap=host:port] [count=1000]``
"""

from __future__ import annotations

import json
import random
import sys
import time

from kafka import KafkaProducer

TOPIC = "json-demo-topic-pure"


def main(argv: list[str] | None = None) -> None:
    args = {a.split("=", 1)[0]: a.split("=", 1)[1] for a in (argv or sys.argv[1:]) if "=" in a}
    bootstrap = args.get("bootstrap", "localhost:9092")
    count = int(args.get("count", "1000"))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # --- high-throughput producer tuning ---
        acks=1,
        compression_type="zstd",
        batch_size=262144,  # 256 KiB
        linger_ms=50,
        buffer_memory=268435456,  # 256 MiB
        max_request_size=10485760,
    )
    print(f"Producing {count} json records to {TOPIC} on {bootstrap} ...")
    for employee_id in range(count):
        record = {
            "employee_id": random.randint(0, 100000),
            "age": random.randint(0, 99),
            "start_date": int(time.time() * 1000),
            "team": "Solutions Architects",
            "role": "ARCHITECT",
            "address": "Melbourne, Australia",
            "name": f"Dummy{random.randint(0, 99)}",
        }
        producer.send(TOPIC, key=f"key-{employee_id}", value=record)
    producer.flush()
    producer.close()
    print("Done.")


if __name__ == "__main__":
    main()

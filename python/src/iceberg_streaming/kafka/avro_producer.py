"""Native Kafka Avro producer.

Counterpart of ``com.aws.emr.avro.kafka.producer.AvroProducer``: produces schemaless (no registry)
Avro-encoded ``Employee`` records to ``avro-demo-topic-pure`` using the ``Employee.avsc`` schema.
This matches what the Spark ``avro-native`` job decodes with ``from_avro``.

Usage: ``uv run avro-producer [bootstrap=host:port] [count=1000] [avro=path.avsc]``
"""

from __future__ import annotations

import io
import json
import random
import sys
import time
from pathlib import Path

import fastavro
from kafka import KafkaProducer

TOPIC = "avro-demo-topic-pure"
_DEFAULT_SCHEMA = "../src/main/avro/Employee.avsc"
_TEAMS = ["Solutions Architects", "Developers", "Managers"]
_ROLES = ["MANAGER", "DEVELOPER", "ARCHITECT"]


def _kv(argv: list[str]) -> dict[str, str]:
    return {a.split("=", 1)[0].strip().lower(): a.split("=", 1)[1].strip() for a in argv if "=" in a}


def _encode(parsed_schema, record: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


def main(argv: list[str] | None = None) -> None:
    args = _kv(argv if argv is not None else sys.argv[1:])
    bootstrap = args.get("bootstrap", "localhost:9092")
    count = int(args.get("count", "1000"))
    schema_path = args.get("avro", _DEFAULT_SCHEMA)

    parsed_schema = fastavro.parse_schema(json.loads(Path(schema_path).read_text(encoding="utf-8")))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        # --- high-throughput producer tuning ---
        acks=1,
        compression_type="zstd",
        batch_size=262144,  # 256 KiB
        linger_ms=50,
        buffer_memory=268435456,  # 256 MiB
        max_request_size=10485760,
    )
    print(f"Producing {count} avro records to {TOPIC} on {bootstrap} ...")
    for employee_id in range(count):
        record = {
            "employee_id": random.randint(0, 100000),
            "age": random.randint(0, 99),
            "start_date": int(time.time() * 1000),
            "team": random.choice(_TEAMS),
            "role": random.choice(_ROLES),
            "address": "Melbourne, Australia",
            "name": f"Dummy{random.randint(0, 99)}",
        }
        producer.send(TOPIC, key=f"key-{employee_id}", value=_encode(parsed_schema, record))
    producer.flush()
    producer.close()
    print("Done.")


if __name__ == "__main__":
    main()

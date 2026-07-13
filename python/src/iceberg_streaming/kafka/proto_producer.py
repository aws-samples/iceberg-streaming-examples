"""Native Kafka Protocol Buffers producer.

PySpark-project counterpart of ``com.aws.emr.proto.kafka.producer.ProtoProducer``: produces raw
protobuf-serialized ``Employee`` messages (no schema registry) to ``protobuf-demo-topic-pure``.

Requires the generated bindings (``scripts/gen_proto.sh``).

Usage: ``uv run proto-producer [bootstrap=host:port] [count=1000]``
"""

from __future__ import annotations

import random
import sys

from kafka import KafkaProducer

TOPIC = "protobuf-demo-topic-pure"


def _kv(argv: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for a in argv:
        if "=" in a:
            k, _, v = a.partition("=")
            out[k.strip().lower()] = v.strip()
    return out


def _make_employee(pb2, employee_id: int):
    emp = pb2.Employee()
    emp.id = random.randint(0, 100000)
    emp.name = f"Dummy{random.randint(0, 99)}"
    emp.address = "Melbourne, Australia"
    emp.employee_age.value = random.randint(0, 99)
    emp.start_date.GetCurrentTime()
    emp.role = pb2.Role.ARCHITECT
    emp.team.name = "Solutions Architects"
    emp.team.location = "Australia"
    return emp


def main(argv: list[str] | None = None) -> None:
    args = _kv(argv if argv is not None else sys.argv[1:])
    bootstrap = args.get("bootstrap", "localhost:9092")
    count = int(args.get("count", "1000"))

    from iceberg_streaming.proto_gen import Employee_pb2  # generated bindings

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda emp: emp.SerializeToString(),
        # --- high-throughput producer tuning ---
        acks=1,
        compression_type="zstd",
        batch_size=262144,  # 256 KiB
        linger_ms=50,
        buffer_memory=268435456,  # 256 MiB
        max_request_size=10485760,
    )
    print(f"Producing {count} protobuf records to {TOPIC} on {bootstrap} ...")
    for employee_id in range(count):
        producer.send(TOPIC, key=f"key-{employee_id}", value=_make_employee(Employee_pb2, employee_id))
    producer.flush()
    producer.close()
    print("Done.")


if __name__ == "__main__":
    main()

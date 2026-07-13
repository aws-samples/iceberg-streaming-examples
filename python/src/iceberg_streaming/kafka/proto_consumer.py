"""Native Kafka Protocol Buffers consumer.

Counterpart of ``com.aws.emr.proto.kakfa.consumer.ProtoConsumer``: consumes raw protobuf ``Employee``
messages from ``protobuf-demo-topic-pure`` and logs them. Requires the generated bindings.

Usage: ``uv run proto-consumer [bootstrap=host:port]``
"""

from __future__ import annotations

import sys

from kafka import KafkaConsumer

TOPIC = "protobuf-demo-topic-pure"


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    bootstrap = "localhost:9092"
    for a in argv:
        if a.startswith("bootstrap="):
            bootstrap = a.split("=", 1)[1]

    from iceberg_streaming.proto_gen import Employee_pb2  # generated bindings

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=bootstrap,
        group_id="protobuf-py",
        auto_offset_reset="latest",
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    print(f"Consuming from {TOPIC} on {bootstrap} ... Ctrl-C to stop")
    for record in consumer:
        emp = Employee_pb2.Employee()
        emp.ParseFromString(record.value)
        print(
            f"Employee Id: {emp.id} | Name: {emp.name} | Address: {emp.address} "
            f"| Age: {emp.employee_age.value} | Startdate: {emp.start_date.seconds}"
        )


if __name__ == "__main__":
    main()

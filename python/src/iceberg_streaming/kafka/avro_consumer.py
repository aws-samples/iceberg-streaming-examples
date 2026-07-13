"""Native Kafka Avro consumer.

Counterpart of ``com.aws.emr.avro.kafka.consumer.AvroConsumer``: consumes schemaless Avro
``Employee`` records from ``avro-demo-topic-pure`` and logs them.

Usage: ``uv run avro-consumer [bootstrap=host:port] [avro=path.avsc]``
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import fastavro
from kafka import KafkaConsumer

TOPIC = "avro-demo-topic-pure"
_DEFAULT_SCHEMA = "../src/main/avro/Employee.avsc"


def main(argv: list[str] | None = None) -> None:
    args = {a.split("=", 1)[0]: a.split("=", 1)[1] for a in (argv or sys.argv[1:]) if "=" in a}
    bootstrap = args.get("bootstrap", "localhost:9092")
    parsed_schema = fastavro.parse_schema(
        json.loads(Path(args.get("avro", _DEFAULT_SCHEMA)).read_text(encoding="utf-8"))
    )

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=bootstrap,
        group_id="avro-py",
        auto_offset_reset="latest",
    )
    print(f"Consuming from {TOPIC} on {bootstrap} ... Ctrl-C to stop")
    for record in consumer:
        emp = fastavro.schemaless_reader(io.BytesIO(record.value), parsed_schema)
        print(
            f"Employee Id: {emp['employee_id']} | Name: {emp['name']} | Address: {emp['address']} "
            f"| Age: {emp['age']} | Startdate: {emp['start_date']}"
        )


if __name__ == "__main__":
    main()

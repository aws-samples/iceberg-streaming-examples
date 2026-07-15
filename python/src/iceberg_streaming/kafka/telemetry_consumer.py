"""Plain Kafka consumer that decodes and prints the EV telemetry topics - a debugging tool for
checking what ``telemetry-producer`` actually put on the wire (``format=proto|avro|json``).

Python counterpart of ``com.aws.emr.kafka.TelemetryConsumer``.

Usage::

    uv run telemetry-consumer [format=proto|avro|json] [bootstrap=host:port] [topic=<name>]
        [group=<id>] [count=<n|0=forever>]
"""

from __future__ import annotations

import io
import sys

from kafka import KafkaConsumer

from iceberg_streaming.kafka.telemetry_producer import _AVRO_SCHEMA, _kv


def main(argv: list[str] | None = None) -> None:
    kv = _kv(argv if argv is not None else sys.argv[1:])
    bootstrap = kv.get("bootstrap", "localhost:9092")
    fmt = kv.get("format", kv.get("source", "proto")).lower()
    if fmt not in ("proto", "avro", "json"):
        raise SystemExit(f"format must be proto, avro or json, got: {fmt}")
    topic = kv.get("topic", f"telemetry-{fmt}")
    group = kv.get("group", "telemetry-console-py")
    count = int(kv.get("count", "0"))

    if fmt == "proto":
        from iceberg_streaming.proto_gen import VehicleTelemetry_pb2  # generated bindings

        def decode(value: bytes) -> str:
            t = VehicleTelemetry_pb2.VehicleTelemetry()
            t.ParseFromString(value)
            return (
                f"vehicle={t.vehicle_id} time={t.event_time.seconds} model={t.model} "
                f"speed={t.speed_kmh}km/h soc={t.soc_pct}% odo={t.odometer_km}km charging={t.charging}"
            )

    elif fmt == "avro":
        from fastavro import parse_schema, schemaless_reader

        parsed = parse_schema(_AVRO_SCHEMA)

        def decode(value: bytes) -> str:
            record = schemaless_reader(io.BytesIO(value), parsed)
            return (
                f"vehicle={record['vehicle_id']} time={record['event_time']} model={record['model']} "
                f"speed={record['speed_kmh']}km/h soc={record['soc_pct']}% "
                f"odo={record['odometer_km']}km charging={record['charging']}"
            )

    else:

        def decode(value: bytes) -> str:
            return value.decode(errors="replace")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="earliest",
    )
    print(f"TelemetryConsumer -> topic={topic} format={fmt} group={group}")
    seen = 0
    try:
        for message in consumer:
            print(decode(message.value))
            seen += 1
            if count and seen >= count:
                break
    except KeyboardInterrupt:
        pass
    consumer.close()


if __name__ == "__main__":
    main()

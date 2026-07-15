"""Kafka producer of EV vehicle telemetry, in any of the three payload formats the Spark jobs
consume (``format=proto|avro|json``).

Python counterpart of ``com.aws.emr.kafka.TelemetryProducer``. One producer covers all formats so
the only difference between the format examples is the serializer, not a copy/pasted loop. It
deliberately produces the two data-quality warts the ingest examples exist to handle - **late
events** (0.1% of readings stamped one hour in the past) and **duplicates** (0.2% of records re-sent
verbatim) - plus optional **corrupt** JSON lines (~0.1%, ``corrupt=true``) to feed the dead-letter
example. Records carry no Kafka key on purpose, so arrivals scatter across partitions out of order.

The protobuf format needs the generated bindings (``scripts/gen_proto.sh``); Avro uses fastavro's
plain binary encoding (symmetric with Spark's ``from_avro``); JSON is a hand-built line.

Durability note: ``acks=1`` favours throughput and disables idempotence, so retries can duplicate
and reorder - the at-least-once behaviour the dedup examples are built for.

Usage::

    uv run telemetry-producer [format=proto|avro|json] [bootstrap=host:port] [count=<n|0=forever>]
        [rate=<msgs/sec|0=unthrottled>] [vehicles=<n>] [late=bool] [duplicates=bool] [corrupt=bool]
        [topic=<name>]
"""

from __future__ import annotations

import io
import json
import random
import sys
import time

from kafka import KafkaProducer

#: Low-cardinality model dimension; stable per vehicle id.
MODELS = ("Falcon-1", "Falcon-3", "Aquila-S", "Aquila-X", "Vulcan-7")

_AVRO_SCHEMA = {
    "namespace": "telemetry.ev.avro",
    "type": "record",
    "name": "VehicleTelemetry",
    "fields": [
        {"name": "vehicle_id", "type": "long"},
        {"name": "event_time", "type": "long"},
        {"name": "model", "type": "string"},
        {"name": "speed_kmh", "type": "int"},
        {"name": "soc_pct", "type": "int"},
        {"name": "odometer_km", "type": "long"},
        {"name": "charging", "type": "boolean"},
    ],
}


def _kv(argv: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for a in argv:
        if "=" in a:
            k, _, v = a.partition("=")
            out[k.strip().lower()] = v.strip()
    return out


def _bool(kv: dict[str, str], key: str, default: bool) -> bool:
    v = kv.get(key)
    return default if v is None else v.strip().lower() in {"1", "true", "yes", "on"}


def _next_reading(vehicles: int, late: bool) -> dict:
    vehicle_id = random.randrange(vehicles)
    event_ms = int(time.time() * 1000)
    if late and random.randrange(1000) == 0:
        event_ms -= 3_600_000  # a reading that arrives one hour late
    charging = random.randrange(10) == 0
    return {
        "vehicle_id": vehicle_id,
        "event_time": event_ms,
        "model": MODELS[vehicle_id % len(MODELS)],
        "speed_kmh": 0 if charging else random.randrange(201),
        "soc_pct": random.randrange(101),
        "odometer_km": random.randrange(500_000),
        "charging": charging,
    }


def main(argv: list[str] | None = None) -> None:
    kv = _kv(argv if argv is not None else sys.argv[1:])
    bootstrap = kv.get("bootstrap", "localhost:9092")
    fmt = kv.get("format", kv.get("source", "proto")).lower()
    if fmt not in ("proto", "avro", "json"):
        raise SystemExit(f"format must be proto, avro or json, got: {fmt}")
    topic = kv.get("topic", f"telemetry-{fmt}")
    count = int(kv.get("count", "0"))
    rate = int(kv.get("rate", "0"))
    vehicles = int(kv.get("vehicles", "100000"))
    late = _bool(kv, "late", True)
    duplicates = _bool(kv, "duplicates", True)
    corrupt = _bool(kv, "corrupt", False)

    # Format-specific serializers, resolved once outside the hot loop.
    if fmt == "proto":
        from google.protobuf.timestamp_pb2 import Timestamp

        from iceberg_streaming.proto_gen import VehicleTelemetry_pb2  # generated bindings

        def serialize(r: dict) -> bytes:
            t = VehicleTelemetry_pb2.VehicleTelemetry()
            t.vehicle_id = r["vehicle_id"]
            ts = Timestamp()
            ts.FromMilliseconds(r["event_time"])
            t.event_time.CopyFrom(ts)
            t.model = r["model"]
            t.speed_kmh = r["speed_kmh"]
            t.soc_pct = r["soc_pct"]
            t.odometer_km = r["odometer_km"]
            t.charging = r["charging"]
            return t.SerializeToString()

    elif fmt == "avro":
        from fastavro import parse_schema, schemaless_writer

        parsed = parse_schema(_AVRO_SCHEMA)

        def serialize(r: dict) -> bytes:
            # Plain Avro binary (no single-object header), symmetric with Spark's from_avro.
            buf = io.BytesIO()
            schemaless_writer(buf, parsed, r)
            return buf.getvalue()

    else:

        def serialize(r: dict) -> bytes:
            return json.dumps(r, separators=(",", ":")).encode()

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        # --- high-throughput producer tuning; acks=1 disables idempotence on purpose (see module doc)
        acks=1,
        compression_type="zstd",
        batch_size=262144,  # 256 KiB
        linger_ms=50,
        buffer_memory=268435456,  # 256 MiB
        max_request_size=10485760,
    )
    print(
        f"TelemetryProducer -> topic={topic} format={fmt} bootstrap={bootstrap} "
        f"count={count or 'unbounded'} rate={rate or 'unthrottled'} vehicles={vehicles} "
        f"late={late} duplicates={duplicates} corrupt={corrupt}"
    )
    sent = 0
    try:
        while count == 0 or sent < count:
            reading = _next_reading(vehicles, late)
            if fmt == "json" and corrupt and random.randrange(1000) == 0:
                # truncated line -> lands in the dead-letter table of the JSON ingest example
                payload = f'{{"vehicle_id":{reading["vehicle_id"]},"event_time":'.encode()
            else:
                payload = serialize(reading)
            # No key on purpose: records scatter across partitions (out-of-order arrivals).
            producer.send(topic, value=payload)
            if duplicates and random.randrange(500) == 0:
                producer.send(topic, value=payload)  # verbatim re-send
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

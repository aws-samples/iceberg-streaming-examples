"""End-to-end scenario runner: seed a fixed dataset, run bounded micro-batches through the shared
guarded CDC MERGE, then assert the final Iceberg table state against the deterministic oracle in
:mod:`iceberg_streaming.scenarios.events`.

It runs entirely locally (Hadoop file catalog under a throwaway temp warehouse), so it needs no AWS.
Two sources are supported:

* ``source=memory`` (default) -- feeds the events straight into ``foreachBatch``-style chunks via
  ``createDataFrame``. No Kafka broker required; still exercises the real shared MERGE, the real
  Iceberg table, cross-batch guards and the final-state assertion.
* ``source=kafka`` -- produces the events to a throwaway Kafka topic and consumes them with a bounded
  ``Trigger.AvailableNow`` streaming query (``maxOffsetsPerTrigger`` forces several micro-batches).
  Needs a broker (``bootstrap=host:port``, e.g. the ``docker-compose.yml`` broker on localhost:9092).

Both paths need a local Spark + Iceberg runtime; the jars are resolved via Ivy on first run (network).

Usage::

    uv run scenario cdc-out-of-order
    uv run scenario cdc-ordered source=kafka bootstrap=localhost:9092
    uv run scenario mor-v2 && uv run scenario mor-v3
    uv run scenario all                       # run every scenario, memory source
    uv run scenario cdc-out-of-order keep=true # keep the temp table/warehouse for inspection

Exit code is non-zero if any scenario's final state does not match the oracle, so it doubles as a
smoke test.
"""

from __future__ import annotations

import logging
import shutil
import sys
import tempfile
import uuid

from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from iceberg_streaming.cdc._sql import mirror_merge
from iceberg_streaming.common import DATABASE, JobConfig
from iceberg_streaming.scenarios.events import SCENARIO_NAMES, Scenario, build

log = logging.getLogger("iceberg_streaming.scenarios.runner")

_BATCH_SCHEMA = StructType(
    [
        StructField("operation", StringType()),
        StructField("account_id", LongType()),
        StructField("balance", DoubleType()),
        StructField("last_updated_ms", LongType()),
        StructField("seq", LongType()),
    ]
)


def _parse_args(argv: list[str]) -> tuple[str, dict[str, str]]:
    positional = [a for a in argv if "=" not in a]
    kv = {a.split("=", 1)[0].strip().lower(): a.split("=", 1)[1].strip() for a in argv if "=" in a}
    name = positional[0] if positional else "all"
    return name, kv


def _create_table_sql(fqn: str, fv: str) -> str:
    return f"""
        CREATE TABLE {fqn}
              (account_id bigint, balance float, last_updated timestamp, seq bigint)
              PARTITIONED BY (bucket(8, account_id))
              TBLPROPERTIES (
                        'table_type'='ICEBERG',
                        'format-version'='{fv}',
                        'format'='parquet',
                        'write.delete.mode'='merge-on-read',
                        'write.update.mode'='merge-on-read',
                        'write.merge.mode'='merge-on-read',
                        'write.parquet.compression-codec'='zstd',
                        'compatibility.snapshot-id-inheritance.enabled'='true' )
    """


def _chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _run_memory(spark, scenario: Scenario, fqn: str, merge_sql: str) -> int:
    """Feed the events in bounded chunks via Spark's JVM CSV reader.

    We deliberately avoid ``createDataFrame`` from a Python list here: on some platforms (notably
    macOS) that spawns Python workers that can crash on fork. Writing each micro-batch to a CSV file
    and reading it with the JVM reader keeps the whole path off Python workers, so the harness is
    robust everywhere. It still exercises the real shared MERGE, the real table and the cross-batch
    guards.
    """
    batch_dir = tempfile.mkdtemp(prefix="scenario-batches-")
    batches = 0
    try:
        for i, chunk in enumerate(_chunks(scenario.events, scenario.batch)):
            path = f"{batch_dir}/batch_{i:05d}.csv"
            with open(path, "w", encoding="utf-8") as fh:
                for e in chunk:
                    fh.write(f"{e.operation},{e.account_id},{e.balance},{e.last_updated},{e.seq}\n")
            (
                spark.read.schema(_BATCH_SCHEMA)
                .csv(path)
                .selectExpr(
                    "operation",
                    "account_id",
                    "balance",
                    "cast(last_updated_ms/1000 as timestamp) as last_updated",
                    "seq",
                )
                .createOrReplaceTempView("accounts_batch")
            )
            spark.sql(merge_sql)
            batches += 1
    finally:
        shutil.rmtree(batch_dir, ignore_errors=True)
    return batches


def _run_kafka(spark, scenario: Scenario, fqn: str, merge_sql: str, bootstrap: str, checkpoint: str) -> int:
    from kafka import KafkaProducer  # kafka-python is a project dependency

    topic = f"scenario-{scenario.name}-{uuid.uuid4().hex[:8]}"
    log.warning("[scenario] producing %s events to kafka topic %s", len(scenario.events), topic)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
    )
    for e in scenario.events:
        producer.send(topic, key=str(e.account_id), value=e.to_csv())
    producer.flush()
    producer.close()

    from pyspark.sql import functions as F

    schema = ["operation", "account_id", "balance", "last_updated", "seq"]
    src = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", str(scenario.batch))
        .load()
        .selectExpr("CAST(value AS STRING) as value")
    )
    split_col = F.split(F.col("value"), ",")
    for i, name in enumerate(schema):
        src = src.withColumn(name, split_col.getItem(i))
    src = (
        src.drop("value")
        .withColumn("account_id", F.col("account_id").cast("bigint"))
        .withColumn("balance", F.col("balance").cast("float"))
        .withColumn("last_updated", (F.col("last_updated") / 1000).cast("timestamp"))
        .withColumn("seq", F.col("seq").cast("long"))
    )

    counter = {"batches": 0}

    def _foreach(bdf, batch_id):
        counter["batches"] += 1
        bdf.createOrReplaceTempView("accounts_batch")
        bdf.sparkSession.sql(merge_sql)

    query = (
        src.writeStream.queryName(f"scenario-{scenario.name}")
        .foreachBatch(_foreach)
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint)
        .start()
    )
    query.awaitTermination()
    return counter["batches"]


def _actual_state(spark, fqn: str) -> dict[int, tuple[int, int]]:
    rows = spark.sql(f"SELECT account_id, balance, seq FROM {fqn}").collect()
    return {int(r["account_id"]): (int(round(r["balance"])), int(r["seq"])) for r in rows}


def _delete_encoding_report(spark, fqn: str) -> dict:
    report: dict = {}
    try:
        report["snapshots"] = spark.sql(f"SELECT * FROM {fqn}.snapshots").count()
    except Exception:  # noqa: BLE001
        report["snapshots"] = "n/a"
    try:
        rows = spark.sql(f"SELECT content, count(*) AS c FROM {fqn}.files GROUP BY content").collect()
        by_content = {int(r["content"]): int(r["c"]) for r in rows}
        report["data_files"] = by_content.get(0, 0)
        report["delete_files"] = by_content.get(1, 0) + by_content.get(2, 0)
    except Exception:  # noqa: BLE001
        report["data_files"] = report["delete_files"] = "n/a"
    return report


def _assert_state(expected: dict, actual: dict) -> tuple[bool, list[str]]:
    problems: list[str] = []
    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    if missing:
        problems.append(f"{len(missing)} expected key(s) missing, e.g. {sorted(missing)[:5]}")
    if extra:
        problems.append(f"{len(extra)} unexpected key(s) present, e.g. {sorted(extra)[:5]}")
    mismatched = [k for k in expected.keys() & actual.keys() if expected[k] != actual[k]]
    if mismatched:
        sample = {k: {"expected": expected[k], "actual": actual[k]} for k in sorted(mismatched)[:5]}
        problems.append(f"{len(mismatched)} value mismatch(es), e.g. {sample}")
    return (not problems), problems


def _run_one(spark, cfg: JobConfig, scenario: Scenario, source: str, bootstrap: str) -> bool:
    table = f"scenario_{scenario.name.replace('-', '_')}_v{scenario.format_version}"
    fqn = f"{DATABASE}.{table}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"DROP TABLE IF EXISTS {fqn} PURGE")
    spark.sql(_create_table_sql(fqn, scenario.format_version))
    merge_sql = mirror_merge(fqn, "accounts_batch")

    if source == "kafka":
        batches = _run_kafka(spark, scenario, fqn, merge_sql, bootstrap, cfg.checkpoint_for(scenario.name))
    else:
        batches = _run_memory(spark, scenario, fqn, merge_sql)

    actual = _actual_state(spark, fqn)
    ok, problems = _assert_state(scenario.expected_state, actual)
    report = _delete_encoding_report(spark, fqn)

    status = "PASS" if ok else "FAIL"
    print(
        f"[{status}] {scenario.name} (fv={scenario.format_version}, source={source}) "
        f"events={len(scenario.events)} batches={batches} "
        f"expected_rows={len(scenario.expected_state)} actual_rows={len(actual)} "
        f"snapshots={report['snapshots']} data_files={report['data_files']} "
        f"delete_files={report['delete_files']}"
    )
    if scenario.note:
        print(f"        note: {scenario.note}")
    for p in problems:
        print(f"        - {p}")
    return ok


def _run_single(name: str, kv: dict[str, str]) -> bool:
    source = kv.get("source", "memory").lower()
    bootstrap = kv.get("bootstrap", "localhost:9092")
    seed = int(kv.get("seed", "42"))
    keys = int(kv.get("keys", "50"))
    events_per_key = int(kv.get("events", "8"))
    keep = kv.get("keep", "false").lower() in {"1", "true", "yes", "on"}

    tmp = tempfile.mkdtemp(prefix="iceberg-scenario-")
    cfg = JobConfig.from_args(
        [
            "catalog=local",
            f"warehouse={tmp}/warehouse",
            f"checkpoint={tmp}/checkpoint",
            f"bootstrap={bootstrap}",
            "startingOffsets=earliest",
        ]
    )
    spark = cfg.build_session("ScenarioHarness")
    spark.sparkContext.setLogLevel("WARN")
    try:
        scenario = build(name, seed=seed, keys=keys, events_per_key=events_per_key)
        return _run_one(spark, cfg, scenario, source, bootstrap)
    finally:
        spark.stop()
        if keep:
            print(f"[scenario] kept temp warehouse/checkpoint at {tmp}")
        else:
            shutil.rmtree(tmp, ignore_errors=True)


def _run_all(kv: dict[str, str]) -> None:
    """Run every scenario, each in its own subprocess.

    A single local Spark JVM running all scenarios back-to-back is flaky (accumulated state /
    executor instability), and a user would run them one at a time anyway. Isolating each in a fresh
    process is robust and mirrors real usage.
    """
    import subprocess

    passthrough = [f"{k}={v}" for k, v in kv.items()]
    results: dict[str, int] = {}
    for name in SCENARIO_NAMES:
        print(f"\n=== scenario: {name} ===", flush=True)
        proc = subprocess.run(
            [sys.executable, "-m", "iceberg_streaming.scenarios.runner", name, *passthrough],
            check=False,
        )
        results[name] = proc.returncode
        print(f"[scenario] {name}: {'ok' if proc.returncode == 0 else f'exit {proc.returncode}'}", flush=True)
    passed = sum(1 for rc in results.values() if rc == 0)
    print(f"\n[scenario] {passed}/{len(results)} scenario(s) passed", flush=True)
    for name, rc in results.items():
        if rc != 0:
            print(f"  - FAILED: {name} (exit {rc})", flush=True)
    if passed != len(results):
        raise SystemExit(1)


def main(argv: list[str] | None = None) -> None:
    name, kv = _parse_args(argv if argv is not None else sys.argv[1:])
    if name == "all":
        _run_all(kv)
        return
    if name not in SCENARIO_NAMES:
        raise SystemExit(f"unknown scenario '{name}'. Available: {', '.join(SCENARIO_NAMES)}, or 'all'")
    ok = _run_single(name, kv)
    print(f"\n[scenario] {'1/1' if ok else '0/1'} scenario(s) passed")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

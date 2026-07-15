# AGENTS.md - guide for coding agents

This file orients AI agents (and new contributors) working on this repository: what it is, how it
is structured, the architectural decisions you must preserve, and how to build, test and extend it.
Read this before changing code. The README is user-facing documentation; this file is about the
*rules of the codebase*.

## What this repository is

High-throughput **Kafka -> Spark Structured Streaming -> Apache Iceberg** examples, implemented
twice (Java and PySpark) around two example domains:

* **IoT / EV vehicle telemetry** - append-heavy ingestion of a flat `VehicleTelemetry` record
  produced in Protocol Buffers, Avro or JSON. Tables: `bigdata.vehicle_telemetry` (+ optional
  `_dead_letter`).
* **CDC / bank accounts** - a DMS-like change feed (`I`/`U`/`D` + monotonic `seq`) mirrored into
  Iceberg with a guarded MERGE. Tables: `bigdata.accounts_changelog`, `bigdata.accounts_mirror`.

It is a *teaching/showcase* repo aiming at ~400k msg/sec, not a product. Correctness semantics
(dedup, ordering guards, maintenance policies) are the actual content - treat them as the API.

## Repository layout

```
pom.xml                          Maven build; profiles: emr (default, Spark/Iceberg provided)
                                 XOR dev (everything shaded into the uber-jar). Enforcer checks XOR.
docker-compose.yml               Single-node Kafka (KRaft, apache/kafka image), tuned for throughput.
scripts/run-local.sh             One-command local pipeline; forwards every key=value arg to
                                 producer AND Spark job.
src/main/protobuf/               VehicleTelemetry.proto + checked-in VehicleTelemetry.desc
src/main/avro/                   VehicleTelemetry.avsc (epoch-millis long event_time)
src/main/java/com/aws/emr/
  common/JobConfig.java          THE parameter utility: key=value parsing, SparkSession factory,
                                 Kafka source factory, table-knob accessors, createTableDdl recipe,
                                 per-query checkpoints. Every knob lives here, nowhere else.
  common/StreamingProgressListener.java   [stream-progress] key=value per-batch metrics line.
  kafka/                         Plain Kafka clients: TelemetryProducer (format=proto|avro|json),
                                 TelemetryConsumer, KafkaCDCSimulator. No Spark dependency in logic.
  gsr/                           AWS Glue Schema Registry variants (JVM-only, not ported to Python):
                                 TelemetryRegistryProducer/Consumer, SparkProtoRegistry.
  spark/iot/                     Telemetry ingest jobs + shared helpers:
                                 Telemetry.java        (schema constants + payload decode per source=)
                                 TelemetrySql.java     (dedup MERGE + compaction SQL templates)
                                 SparkNativeIcebergIngest, SparkCustomIcebergIngest,
                                 SparkProtoUDF, SparkIcebergIngestSpj, SparkS3TablesTwoQuerySpj,
                                 SparkIcebergUtils
  spark/cdc/                     CDC jobs + shared SQL:
                                 CdcSql.java           (guarded mirror MERGE + mirror DDL constants)
                                 SparkLogChange, SparkCDCMirror, SparkIncrementalPipeline,
                                 SparkStreamingCDCMirror, SparkCDCReadBenchmark
  spark/maintenance/IcebergMaintenance.java   Standalone maintenance driver (the recommended baseline).
src/test/java/                   Pure-JVM tests (no Spark session): JobConfigTest, CdcSqlTest,
                                 TelemetrySqlTest.
python/                          Full PySpark mirror, managed with uv (Python 3.12, PySpark 4.0.2):
  pyproject.toml                 [project.scripts] = the console entry points (parity-tested).
  src/iceberg_streaming/
    common/jobconfig.py          Mirror of JobConfig (same knobs, same recipe).
    common/observability.py      attach_progress_listener(spark).
    iot/_telemetry.py, iot/_sql.py   Mirrors of Telemetry / TelemetrySql.
    iot/spark_native_iceberg_ingest.py, iot/spark_custom_iceberg_ingest.py,
    iot/spark_proto_udf.py, iot/spark_iceberg_utils.py
    cdc/_sql.py + cdc jobs       Mirrors of CdcSql + the CDC jobs.
    kafka/                       telemetry_producer/consumer, kafka_cdc_simulator (kafka-python).
    maintenance/                 iceberg_maintenance.py.
    scenarios/                   Deterministic end-to-end harness: events.py (pure-Python oracle,
                                 unit-tested) + runner.py (real Spark+Iceberg, memory or Kafka source).
    proto_gen/                   GENERATED protobuf bindings (scripts/gen_proto.sh). Never hand-edit.
  tests/                         pytest suite incl. test_entrypoints_parity (pyproject <-> README sync).
.github/workflows/ci.yml         3 jobs: mvn test, pytest, and `uv run scenario all` (integration smoke).
```

Runtime-only directories (git-ignored, safe to delete): `warehouse/`, `tmp/`, `target/`,
`dependency-reduced-pom.xml`, `python/.venv/`.

## Architectural decisions (preserve these)

### 1. One class per *approach*; knobs per *variant*

The repo used to have one class per combination (MoR vs CoW, parquet vs ORC vs Avro, v2 vs v3,
per-payload-format...). That was deliberately collapsed. A job class exists only if it demonstrates
a genuinely different **approach** (native writer vs foreachBatch vs UDF decode vs GSR vs SPJ).
Everything else is a `key=value` knob parsed by `JobConfig`:

`mode=cow|mor`, `fv=2|3`, `fileformat=parquet|orc|avro`, `objectstorage=`, `source=proto|avro|json`,
`topic=`, `dedup=none|batch|merge|watermark`, `compaction=none|inline|scheduled`,
`trigger=<seconds>|availablenow`, `watermark=`, `fanout=`, `manifestmerge=`, `table=`, plus the
environment knobs (`runtime=`, `catalog=local|glue|s3tables`, `warehouse=`, `checkpoint=`,
`bootstrap=`, `startingOffsets=`, `maxOffsetsPerTrigger=`, `failOnDataLoss=`, `shuffle=`, `region=`).

**Do not** add a new class for a variant expressible as a knob. **Do not** parse `args` by hand in a
job - add a typed accessor to `JobConfig` (both languages) instead.

### 2. One CREATE TABLE recipe

All table DDL goes through `JobConfig.createTableDdl(table, columnsDdl, partitionDdl, defaultMode,
overrides)` / `create_table_ddl(...)`. The recipe is format-aware (parquet tuning only on parquet
tables, `write.orc.*` on ORC, etc.), sets MoR hash distribution, bounded metadata
(`write.metadata.delete-after-commit.enabled` + `previous-versions-max`), and generous commit
retries (streaming writers, MERGEs and compaction all race optimistic commits). Job-specific needs
go in the `overrides` map (see `SparkStreamingCDCMirror` for an example), never in a bespoke inline
TBLPROPERTIES block.

### 3. Java and Python are mirrors - parity is enforced

Every pattern exists in both languages with the same knobs, same table DDL, same SQL, same defaults,
unless explicitly listed as JVM-only (Glue Schema Registry, SPJ jobs, read benchmark - see the
README parity matrix). If you change one side, change the other. Mechanical enforcement:
`python/tests/test_entrypoints_parity.py` fails if a console script in `pyproject.toml` is not
importable or not documented in `python/README.md`. There is no automated Java<->Python semantic
diff - *you* are the enforcement for SQL/knob parity, and the shared-SQL unit tests exist on both
sides for exactly this reason.

### 4. Correctness-critical SQL lives in templates and is unit-tested

* `TelemetrySql` / `iot/_sql.py` - telemetry replay-suppression MERGE + compaction calls.
* `CdcSql` / `cdc/_sql.py` - the guarded CDC mirror MERGE + mirror DDL constants.

Jobs interpolate only table/view names into these templates. The invariants are pinned by
`TelemetrySqlTest`/`test_iot_sql.py` and `CdcSqlTest`/`test_cdc_sql.py`:

* **Telemetry event identity is `(vehicle_id, event_time)`.** In-batch dedup MUST partition by both
  columns - partitioning by `vehicle_id` alone collapses distinct readings and silently loses data
  (this was a real bug once; the tests exist so it cannot come back). Ties break on
  `kafka_offset DESC` (deterministic).
* The telemetry MERGE is **insert-only** (bounded replay suppression, ON clause pruned to the last
  2 hours). It is not an upsert; upserts belong to the CDC mirror / SPJ jobs.
* **CDC dedup orders by `seq DESC`, never by `last_updated`** (timestamps are not unique). Matched
  UPDATE/DELETE branches are guarded by `c.seq >= a.seq`; the winning `seq` is persisted on the
  target. The DELETE branch precedes the UPDATE branch. Inserts skip tombstones
  (`c.operation != 'D'`).
* The known "resurrection" limitation (stale insert after a physical delete) is deliberate and
  documented; `resurrection-demo` in the scenario harness asserts it. Do not "fix" it casually -
  it is part of the v2/v3 delete-encoding story.
* **Compaction never touches the hot partition**: `rewrite_data_files` is bounded to closed hours
  (`>= now()-3h AND < date_trunc('hour', now())`). Manifest rewrites run less often than data
  rewrites. In-job maintenance is always wrapped so a failure can never kill the ingest query or a
  `scheduleAtFixedRate` schedule.

### 5. Data type and schema rules

* **Money is `bigint` minor units (cents) end to end.** Never float/double for balances.
* Telemetry tables carry Kafka lineage columns `kafka_partition int, kafka_offset bigint` -
  debugging aid and the dedup tiebreaker. Keep them in every telemetry projection and DDL.
* Timestamps: protobuf uses `google.protobuf.Timestamp` (spark-protobuf maps it natively);
  Avro/JSON carry epoch-millis `long`, converted with `timestamp_millis(...)` (never
  `cast(x/1000)` maths).
* Schema changes touch, in lockstep: `VehicleTelemetry.proto` (+ regenerate the checked-in `.desc`
  and `python/proto_gen` via `python/scripts/gen_proto.sh`), `VehicleTelemetry.avsc`, the JSON
  struct in `Telemetry.java`/`_telemetry.py`, `COLUMNS_DDL` in both languages, both producers, and
  the SQL templates.

### 6. Streaming query hygiene

* Every streaming query uses `cfg.checkpointFor("<stable-query-name>")` - never the raw checkpoint
  base. Different `table=` values must yield different checkpoint paths.
* `StreamingProgressListener.attach(spark)` / `attach_progress_listener(spark)` is called **before**
  `query.start()` so batch 0 is captured.
* `foreachBatch` runs on a **cloned session that does not inherit `USE bigdata`** - reference sinks
  and MERGE targets by fully-qualified `catalog.database.table` inside the batch function (the
  `catalogName()` accessor exists for this).
* Writes inside `foreachBatch` use by-name `writeTo(fqn).append()` (never positional `insertInto`).
* Skip empty micro-batches early; when a JSON batch is consumed twice (dead-letter split + ingest),
  persist/unpersist it.

### 7. Producers are at-least-once *on purpose*

`acks=1` (idempotence off), no Kafka key, deliberate late events (0.1%, one hour), verbatim
duplicates (0.2%) and optional corrupt JSON. This misbehaviour is the input the dedup strategies
exist to handle - do not "harden" the producers without understanding that the examples then stop
demonstrating anything. `keyed=true` on the CDC simulator is the opt-in for per-key ordering.

### 8. Build decisions

* Maven profiles: `emr` (default; Spark/Iceberg/S3Tables `provided`) XOR `dev` (uber-jar via shade;
  ServicesResourceTransformer keeps DataSource registrations, `reference.conf` appended, protobuf
  relocated to `org.sparkproject.spark_protobuf.protobuf` and excluded from the jar). The enforcer
  plugin fails the build if both/neither profile is active.
* `kafka-clients` is pinned to 3.9.x because Spark 4.0.2's Kafka connector is built against it (a
  4.x client throws `NoSuchMethodError`); a 3.9 client talks to 4.x brokers fine.
* `protobuf-java-util` must stay on `${protobuf.version}` (never mix 3.x core with 4.x util).
* Generated code (protobuf via protoc-jar, Avro via avro-maven-plugin with `stringType=String`)
  goes to `target/generated-sources/` and is **never committed**. The only committed generated
  artifacts are `VehicleTelemetry.desc` and `python/.../proto_gen/*_pb2.py` (needed for
  out-of-the-box runs); regenerate them when the proto changes.

## Build and test

```bash
# Java: compile + all unit tests (pure JVM, fast; emr profile default)
mvn -B -ntp test

# Java: local uber-jar (Spark/Iceberg bundled)
mvn -Pdev clean package -DskipTests

# Python: env + unit tests (from python/)
uv sync && uv run pytest -q

# Integration smoke test (real local Spark+Iceberg, no broker needed; ~minutes, needs Ivy/network
# on first run). Non-zero exit on any oracle mismatch. CI runs this.
uv run scenario all

# Full local pipeline (broker + build + producer + job)
scripts/run-local.sh dedup=merge compaction=inline
```

Definition of done for any change: `mvn test` green, `pytest` green, and - if you touched the CDC
MERGE, the scenario model or JobConfig session/table logic - `uv run scenario all` green. Update
both READMEs when you add/rename an entry point (the parity test checks `python/README.md`).

## How to extend (checklists)

**Add a new knob:** typed accessor + validation + `usage()` entry in `JobConfig.java` AND
`jobconfig.py` -> wire into `tablePropertiesMap`/jobs as needed -> tests in `JobConfigTest` and
`test_jobconfig.py` -> document in both READMEs. Invalid values must throw, never silently default.

**Add a new job/pattern:** first confirm it is a new *approach*, not a knob. Java class under
`spark/iot` or `spark/cdc` + Python module + `[project.scripts]` entry + row in the README parity
matrix + entry-point table in `python/README.md`. Use `JobConfig` for everything configurable,
`createTableDdl` for DDL, `checkpointFor` for state, the shared decode/SQL helpers for logic.

**Add a new payload format:** extend `Source` enum (both languages), decode branch in
`Telemetry`/`_telemetry.py`, serializer branch in both producers and consumers, default topic
mapping (`telemetry-<format>`), README format section.

**Change dedup/merge semantics:** update the SQL template (both languages), the invariant tests
(both languages), the scenario oracle (`events.py`) if CDC semantics changed, and the README
sections that document the semantics.

## Gotchas

* The **scenario harness memory source writes CSV files and reads them with the JVM reader** on
  purpose (Python-worker fork crashes on macOS) - do not "simplify" it to `createDataFrame`.
* `SparkS3TablesTwoQuerySpj` **seeds the staging table with a sentinel row** (`vehicle_id = -1`)
  because an Iceberg streaming read of an empty table fails; the MERGE filters `vehicle_id >= 0`.
* `CREATE TABLE IF NOT EXISTS` does not update properties on existing tables - jobs that must
  enforce a knob on resume issue an explicit `ALTER TABLE ... SET TBLPROPERTIES` (see the streaming
  CDC mirror's fanout handling).
* `dedup=watermark` (native job only) **drops events older than the watermark entirely** - with the
  default 120s delay, the producer's 1-hour-late events are discarded on that path. This trade-off
  is documented behaviour, not a bug.
* Java `SparkIncrementalPipeline` commits its watermark atomically via the JVM-only
  `CommitMetadata` thread-local; the Python port uses a separate `ALTER TABLE` commit (idempotent
  reprocessing on crash). This asymmetry is documented - keep it.
* Ivy resolution for PySpark local runs can fail if `~/.m2` holds a pom-only artifact entry (see
  `python/README.md` troubleshooting).
* On EMR the runtime provides Spark/Iceberg - the effective Iceberg version (and therefore v3
  behaviour) is decided by the release label, not this build.
* First runs from a stale workspace: `mvn clean` if you see duplicate-class errors from
  `target/generated-sources` after schema renames.

## Conventions

* Arguments are always order-independent lowercase `key=value`; unknown keys are logged and ignored,
  invalid values throw.
* Conventional Commits (`feat(java): ...`, `docs: ...`); build before committing; never push to
  main - feature branches + PRs.
* Comments explain *why* (trade-offs, failure modes), not *what*. The class/module docstring of
  every job states which approach it demonstrates and its knobs.
* Logging via Log4j2 (`log.warn` for the demo-visible lines - Spark's default level hides info) /
  Python `logging`; never `printStackTrace`, never bare `print` in library code (producers' CLI
  feedback excepted).
* Database is always `bigdata` (`JobConfig.DATABASE`); default telemetry table `vehicle_telemetry`;
  topics `telemetry-<format>` and `streaming-cdc-log-ingest`.

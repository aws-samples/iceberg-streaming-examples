#!/usr/bin/env bash
#
# Local end-to-end EV telemetry demo pipeline:
#   1. Ensures the dockerized Kafka (KRaft, apache/kafka) is up.
#   2. Builds the local (dev-profile) uber-jar if needed (Spark/Iceberg bundled).
#   3. Starts the telemetry producer -> Kafka topic 'telemetry-<source>'.
#   4. Runs SparkCustomIcebergIngest in local mode against the same source format, writing the
#      Iceberg table 'bigdata.vehicle_telemetry' under ./warehouse (local Hadoop catalog).
#
# Every key=value argument you pass is forwarded to BOTH the producer and the Spark job, so the
# whole knob matrix is available from here, e.g.:
#
#   scripts/run-local.sh                                    # protobuf, CoW parquet v3, no dedup
#   scripts/run-local.sh source=avro mode=mor fileformat=orc dedup=merge
#   scripts/run-local.sh source=json corrupt=true dedup=batch      # feeds the dead-letter table
#   scripts/run-local.sh dedup=merge compaction=inline fv=2
#
# The Spark UI is available at http://localhost:4040 while the streaming query runs.
#
# Environment overrides:
#   SKIP_BUILD=1        reuse the existing target/*.jar instead of rebuilding
#   BOOTSTRAP=host:port Kafka bootstrap servers (default: localhost:9092)
#   KEEP_DATA=1         do not wipe ./warehouse and ./tmp before starting
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

JAR="target/streaming-iceberg-ingest-1.0-SNAPSHOT.jar"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
PRODUCER_CLASS="com.aws.emr.kafka.TelemetryProducer"
JOB_CLASS="com.aws.emr.spark.iot.SparkCustomIcebergIngest"

# Default source; overridden when the caller passes source=...
SOURCE="proto"
for arg in "$@"; do
  case "$arg" in
    source=*) SOURCE="${arg#source=}" ;;
  esac
done

# --- Java 17 (Apache Spark 4.0 requires JDK 17 or 21) --------------------------------------------
if [ -z "${JAVA_HOME:-}" ] || ! "${JAVA_HOME}/bin/java" -version 2>&1 | grep -q 'version "17'; then
  if command -v /usr/libexec/java_home >/dev/null 2>&1; then
    JAVA_HOME="$(/usr/libexec/java_home -v 17)"
  fi
fi
export JAVA_HOME
JAVA="${JAVA_HOME:+${JAVA_HOME}/bin/}java"
echo "==> Using java: $("$JAVA" -version 2>&1 | head -1)"

# --- Spark 4.0 JVM module options (avoid InaccessibleObjectException on Java 17) ------------------
MODULE_OPTS=(
  -XX:+IgnoreUnrecognizedVMOptions
  --add-modules=jdk.incubator.vector
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
  --add-opens=java.base/sun.security.action=ALL-UNNAMED
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
  -Djdk.reflect.useDirectMethodHandle=false
  -Dio.netty.tryReflectionSetAccessible=true
)

echo "==> Ensuring Kafka is up (docker compose)"
docker compose up -d

if [ "${SKIP_BUILD:-0}" = "1" ] && [ -f "$JAR" ]; then
  echo "==> SKIP_BUILD=1, reusing $JAR"
else
  echo "==> Building local (dev profile) jar (Spark + Iceberg bundled) ..."
  mvn -Pdev -q clean package -DskipTests
fi

if [ "${KEEP_DATA:-0}" != "1" ]; then
  echo "==> Cleaning ./warehouse and ./tmp for a fresh start (set KEEP_DATA=1 to keep)"
  rm -rf warehouse tmp
fi

echo "==> Starting telemetry producer -> topic telemetry-$SOURCE (bootstrap=$BOOTSTRAP)"
"$JAVA" -cp "$JAR" "$PRODUCER_CLASS" bootstrap="$BOOTSTRAP" "$@" &
PRODUCER_PID=$!

cleanup() {
  echo ""
  echo "==> Stopping producer (pid $PRODUCER_PID)"
  kill "$PRODUCER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

sleep 3

echo "==> Running $JOB_CLASS (local, hadoop catalog, source=$SOURCE)"
echo "    Target table: bigdata.vehicle_telemetry  (warehouse=./warehouse)"
echo "    Spark UI:     http://localhost:4040"
echo "    Press Ctrl-C to stop the whole pipeline."
"$JAVA" "${MODULE_OPTS[@]}" -cp "$JAR" "$JOB_CLASS" \
  runtime=local catalog=local warehouse=warehouse checkpoint=tmp/ \
  bootstrap="$BOOTSTRAP" "$@"

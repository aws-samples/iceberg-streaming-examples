#!/usr/bin/env bash
#
# Local end-to-end demo pipeline (Protocol Buffers source, PARQUET storage):
#   1. Ensures the dockerized Kafka (KRaft, apache/kafka) is up.
#   2. Builds the local (dev-profile) uber-jar if needed (Spark/Iceberg bundled).
#   3. Starts the native Protocol Buffers producer -> Kafka topic 'protobuf-demo-topic-pure'.
#   4. Runs SparkCustomIcebergIngest in local mode: it consumes the protobuf stream and writes an
#      Iceberg format-version 3 (v3) table 'bigdata.employee' (Parquet), under ./warehouse.
#
# The Spark UI is available at http://localhost:4040 while the streaming query runs.
#
# Usage:
#   scripts/run-local-proto.sh
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
DESCRIPTOR="src/main/protobuf/Employee.desc"
PRODUCER_CLASS="com.aws.emr.proto.kafka.producer.ProtoProducer"
JOB_CLASS="com.aws.emr.spark.iot.SparkCustomIcebergIngest"

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

echo "==> Starting protobuf producer -> topic protobuf-demo-topic-pure (bootstrap=$BOOTSTRAP)"
"$JAVA" -cp "$JAR" "$PRODUCER_CLASS" "$BOOTSTRAP" &
PRODUCER_PID=$!

cleanup() {
  echo ""
  echo "==> Stopping producer (pid $PRODUCER_PID)"
  kill "$PRODUCER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

sleep 3

echo "==> Running $JOB_CLASS (local, hadoop catalog, Parquet v3 table)"
echo "    Target table: bigdata.employee  (warehouse=./warehouse)"
echo "    Spark UI:     http://localhost:4040"
echo "    Press Ctrl-C to stop the whole pipeline."
"$JAVA" "${MODULE_OPTS[@]}" -cp "$JAR" "$JOB_CLASS" \
  runtime=local catalog=local warehouse=warehouse checkpoint=tmp/ \
  bootstrap="$BOOTSTRAP" descriptor="$DESCRIPTOR" dedup=false compaction=false

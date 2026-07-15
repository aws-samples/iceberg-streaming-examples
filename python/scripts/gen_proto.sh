#!/usr/bin/env bash
#
# Generate the Python Protocol Buffers bindings used by the telemetry producer/consumer and the
# proto UDF Spark job, from the shared VehicleTelemetry.proto in the parent (Java) project.
#
# Requires the dev dependency group (installed by `uv sync`). Run from anywhere:
#
#   ./scripts/gen_proto.sh
#
set -euo pipefail

# Resolve paths relative to this script so it works from any CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROTO_DIR="$(cd "$PYTHON_DIR/../src/main/protobuf" && pwd)"
OUT_DIR="$PYTHON_DIR/src/iceberg_streaming/proto_gen"

echo "Proto source : $PROTO_DIR/VehicleTelemetry.proto"
echo "Output dir   : $OUT_DIR"

mkdir -p "$OUT_DIR"

# grpc_tools.protoc bundles the google/protobuf well-known types (timestamp) on its default include
# path, so we only need to add our own proto directory.
uv run --project "$PYTHON_DIR" python -m grpc_tools.protoc \
  -I "$PROTO_DIR" \
  --python_out="$OUT_DIR" \
  "$PROTO_DIR/VehicleTelemetry.proto"

echo "Generated $OUT_DIR/VehicleTelemetry_pb2.py"

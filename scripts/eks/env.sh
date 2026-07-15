#!/usr/bin/env bash
#
# Shared configuration, state tracking and helpers for the EKS scripts.
# Every numbered script sources this file. Override anything with environment
# variables before running the scripts, e.g.:
#
#   AWS_REGION=eu-west-1 CLUSTER_NAME=my-cluster ./01-vpc.sh
#
# Reuse hooks (set these to adopt existing resources instead of creating new ones;
# reused resources are recorded as such and NEVER touched by 99-teardown.sh):
#
#   EXISTING_VPC_ID=vpc-...                  reuse a VPC (also set the subnet lists below)
#   EXISTING_PRIVATE_SUBNET_IDS=subnet-a,subnet-b   private subnets in >=2 AZs (required with EXISTING_VPC_ID)
#   EXISTING_PUBLIC_SUBNET_IDS=subnet-c,subnet-d    optional public subnets
#   EXISTING_CLUSTER_NAME=my-eks             reuse an EKS cluster (skips VPC + cluster creation)
#   EXISTING_BUCKET=my-bucket                reuse an S3 bucket (only our prefixes are written)
#   KAFKA_BOOTSTRAP=broker:9092              use an external Kafka/MSK instead of the in-cluster demo broker
#
set -euo pipefail

# ---------------------------------------------------------------- basic config
export AWS_REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || true)}"
if [ -z "${AWS_REGION}" ]; then
  echo "ERROR: AWS_REGION is not set and no default region is configured." >&2
  exit 1
fi

export CLUSTER_NAME="${EXISTING_CLUSTER_NAME:-${CLUSTER_NAME:-iceberg-streaming}}"
export K8S_VERSION="${K8S_VERSION:-1.32}"
export NAMESPACE="${NAMESPACE:-spark}"
export SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-spark}"

# Graviton by default (AWS price/performance best practice); ARCH=amd64 switches
# both the nodegroup instance type and the docker build platform.
export ARCH="${ARCH:-arm64}"
if [ "$ARCH" = "arm64" ]; then
  export NODE_TYPE="${NODE_TYPE:-m7g.xlarge}"
else
  export NODE_TYPE="${NODE_TYPE:-m6i.xlarge}"
fi

export ECR_REPO_NAME="${ECR_REPO_NAME:-iceberg-streaming-examples}"
export IMAGE_TAG="${IMAGE_TAG:-latest}"
export GLUE_DATABASE="bigdata"   # fixed by JobConfig.DATABASE

# Tags applied to every AWS resource these scripts create.
export TAG_PROJECT="iceberg-streaming-examples"
export TAG_MANAGED_BY="scripts-eks"
export CLI_TAGS="Key=Project,Value=${TAG_PROJECT} Key=ManagedBy,Value=${TAG_MANAGED_BY}"

# ---------------------------------------------------------------- derived values
export ACCOUNT_ID="${ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
export BUCKET="${EXISTING_BUCKET:-${BUCKET:-iceberg-streaming-${ACCOUNT_ID}-${AWS_REGION}}}"
export ECR_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------- state tracking
# Everything the scripts create is recorded in .state/state.env (git-ignored) as
# KEY=VALUE plus a KEY_CREATED=true|false flag. 99-teardown.sh only deletes
# resources whose *_CREATED flag is true - reused resources are protected.
STATE_DIR="$SCRIPT_DIR/.state"
STATE_FILE="$STATE_DIR/state.env"
mkdir -p "$STATE_DIR"
touch "$STATE_FILE"

state_set() { # state_set KEY VALUE
  local key="$1" value="$2"
  grep -v "^${key}=" "$STATE_FILE" > "$STATE_FILE.tmp" || true
  echo "${key}=${value}" >> "$STATE_FILE.tmp"
  mv "$STATE_FILE.tmp" "$STATE_FILE"
}

state_get() { # state_get KEY [default]
  local value
  value="$(grep "^${1}=" "$STATE_FILE" | tail -1 | cut -d= -f2- || true)"
  echo "${value:-${2:-}}"
}

# ---------------------------------------------------------------- helpers
log()  { echo "==> $*"; }
warn() { echo "WARN: $*" >&2; }
die()  { echo "ERROR: $*" >&2; exit 1; }

require() { # require <tool> [hint]
  command -v "$1" >/dev/null 2>&1 || die "'$1' is required. ${2:-}"
}

confirm() { # confirm <prompt>  (bypass with YES=1)
  if [ "${YES:-0}" = "1" ]; then return 0; fi
  read -r -p "$1 [y/N] " reply
  [ "$reply" = "y" ] || [ "$reply" = "Y" ]
}

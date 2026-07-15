#!/usr/bin/env bash
#
# 00 - Preflight: verify tools and credentials, then print the execution plan.
# Read-only: creates nothing.
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

log "Checking required tools"
require aws     "Install the AWS CLI v2: https://docs.aws.amazon.com/cli/"
require eksctl  "Install eksctl: https://eksctl.io/installation/"
require kubectl "Install kubectl: https://kubernetes.io/docs/tasks/tools/"
require docker  "Install Docker (used to build/push the Spark image)."
require mvn     "Install Maven 3.9+ (used to build the application jar)."
require jq      "Install jq (used to parse AWS CLI responses)."

log "Checking AWS credentials"
IDENTITY=$(aws sts get-caller-identity --output json)
echo "    Account: $(echo "$IDENTITY" | jq -r .Account)"
echo "    Caller:  $(echo "$IDENTITY" | jq -r .Arn)"
echo "    Region:  $AWS_REGION"

log "Checking Docker daemon"
docker info >/dev/null 2>&1 || die "Docker daemon is not running."

log "Execution plan"
cat <<EOF
    Cluster:        $CLUSTER_NAME (Kubernetes $K8S_VERSION)$( [ -n "${EXISTING_CLUSTER_NAME:-}" ] && echo "  [REUSED - will not be deleted]" )
    Nodes:          $NODE_TYPE ($ARCH), managed nodegroup, private subnets
    VPC:            $( [ -n "${EXISTING_VPC_ID:-}" ] && echo "$EXISTING_VPC_ID  [REUSED - will not be deleted]" || echo "new 10.42.0.0/16, 3 AZs, public+private subnets, single NAT (demo cost profile)" )
    S3 bucket:      $BUCKET$( [ -n "${EXISTING_BUCKET:-}" ] && echo "  [REUSED - only warehouse/, checkpoint/, artifacts/ prefixes are written]" )
    ECR repo:       $ECR_URI
    Glue database:  $GLUE_DATABASE
    Kafka:          $( [ -n "${KAFKA_BOOTSTRAP:-}" ] && echo "external at $KAFKA_BOOTSTRAP  [REUSED]" || echo "single-node demo broker inside the cluster (PLAINTEXT; not for production)" )

    COST WARNING: an EKS cluster (~\$0.10/h control plane) + $NODE_TYPE nodes + NAT gateway
    accrue charges while they exist. Run 99-teardown.sh when you are done.
EOF

log "Preflight OK. Continue with 01-vpc.sh (or 02-cluster.sh when reusing a cluster)."

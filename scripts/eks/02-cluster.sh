#!/usr/bin/env bash
#
# 02 - EKS cluster: create it with eksctl inside the VPC from 01-vpc.sh, or
# adopt an existing cluster (EXISTING_CLUSTER_NAME=...). Either way the script
# ends with a working kubeconfig context and an OIDC provider for IRSA.
#
# Created (when not reusing): an EKS control plane + one managed nodegroup of
# $NODE_TYPE instances on private subnets, OIDC provider enabled.
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

# ------------------------------------------------------------------ reuse path
if [ -n "${EXISTING_CLUSTER_NAME:-}" ]; then
  log "Adopting existing EKS cluster $CLUSTER_NAME (will NOT be deleted by teardown)"
  aws eks describe-cluster --name "$CLUSTER_NAME" >/dev/null || die "Cluster $CLUSTER_NAME not found in $AWS_REGION"
  state_set CLUSTER_NAME "$CLUSTER_NAME"; state_set CLUSTER_CREATED false
  aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$AWS_REGION"
  # IRSA needs an OIDC provider; associating is idempotent and non-destructive.
  eksctl utils associate-iam-oidc-provider --cluster "$CLUSTER_NAME" --region "$AWS_REGION" --approve
  kubectl get nodes
  log "Cluster adopted. Continue with 03-artifacts.sh."
  exit 0
fi

if [ "$(state_get CLUSTER_CREATED)" = "true" ]; then
  log "Cluster $CLUSTER_NAME already created by these scripts. Refreshing kubeconfig."
  aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$AWS_REGION"
  exit 0
fi

# ------------------------------------------------------------------ create path
VPC_ID="$(state_get VPC_ID)"
[ -n "$VPC_ID" ] || die "No VPC in state. Run 01-vpc.sh first (or set EXISTING_CLUSTER_NAME)."

# eksctl wants subnets grouped by AZ; look the AZs up from the subnet ids.
subnet_yaml() { # subnet_yaml <comma-separated-ids>
  local ids="$1"
  [ -n "$ids" ] || return 0
  aws ec2 describe-subnets --subnet-ids ${ids//,/ } \
    --query 'Subnets[].[AvailabilityZone,SubnetId]' --output text |
    while read -r az id; do printf '      %s: { id: %s }\n' "$az" "$id"; done
}

CONFIG="$STATE_DIR/cluster.yaml"
log "Generating eksctl config at $CONFIG"
{
cat <<EOF
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: ${CLUSTER_NAME}
  region: ${AWS_REGION}
  version: "${K8S_VERSION}"
  tags:
    Project: ${TAG_PROJECT}
    ManagedBy: ${TAG_MANAGED_BY}
vpc:
  id: ${VPC_ID}
  subnets:
    private:
$(subnet_yaml "$(state_get PRIVATE_SUBNET_IDS)")
EOF
if [ -n "$(state_get PUBLIC_SUBNET_IDS)" ]; then
cat <<EOF
    public:
$(subnet_yaml "$(state_get PUBLIC_SUBNET_IDS)")
EOF
fi
cat <<EOF
iam:
  withOIDC: true   # required for IRSA (pod-level AWS permissions, no node-wide creds)
managedNodeGroups:
  - name: spark-workers
    instanceType: ${NODE_TYPE}
    desiredCapacity: 3
    minSize: 2
    maxSize: 4
    volumeSize: 100
    privateNetworking: true   # nodes on private subnets; egress via NAT
    labels: { role: spark }
    tags:
      Project: ${TAG_PROJECT}
      ManagedBy: ${TAG_MANAGED_BY}
EOF
} > "$CONFIG"

log "Creating EKS cluster $CLUSTER_NAME (this takes ~15-20 minutes)"
eksctl create cluster -f "$CONFIG"
state_set CLUSTER_NAME "$CLUSTER_NAME"; state_set CLUSTER_CREATED true

kubectl get nodes
log "Cluster ready. Continue with 03-artifacts.sh."

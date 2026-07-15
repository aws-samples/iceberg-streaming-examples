#!/usr/bin/env bash
#
# 01 - VPC: create a well-architected VPC for EKS, or adopt an existing one.
#
# Created layout (EKS best practice):
#   * 10.42.0.0/16 with DNS support + hostnames
#   * 3 AZs (falls back to 2 if the region has fewer available)
#   * public subnets  (/20, tagged kubernetes.io/role/elb=1, auto-assign public IP)
#   * private subnets (/19, tagged kubernetes.io/role/internal-elb=1) - nodes live here
#   * one internet gateway; ONE NAT gateway (demo cost profile - production wants
#     one NAT per AZ for availability; see the README)
#
# Reuse instead of create:
#   EXISTING_VPC_ID=vpc-... EXISTING_PRIVATE_SUBNET_IDS=subnet-a,subnet-b \
#     [EXISTING_PUBLIC_SUBNET_IDS=subnet-c,subnet-d] ./01-vpc.sh
# Reused resources are recorded and protected from teardown.
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

if [ -n "${EXISTING_CLUSTER_NAME:-}" ]; then
  log "EXISTING_CLUSTER_NAME is set - the cluster brings its own VPC. Skipping."
  exit 0
fi

# ------------------------------------------------------------------ reuse path
if [ -n "${EXISTING_VPC_ID:-}" ]; then
  log "Adopting existing VPC $EXISTING_VPC_ID (will NOT be deleted by teardown)"
  aws ec2 describe-vpcs --vpc-ids "$EXISTING_VPC_ID" >/dev/null || die "VPC $EXISTING_VPC_ID not found"
  [ -n "${EXISTING_PRIVATE_SUBNET_IDS:-}" ] \
    || die "EXISTING_PRIVATE_SUBNET_IDS (comma-separated, >=2 AZs) is required when reusing a VPC"
  # Validate the subnets exist, belong to the VPC and span >=2 AZs.
  AZS=$(aws ec2 describe-subnets --subnet-ids ${EXISTING_PRIVATE_SUBNET_IDS//,/ } \
        --query 'Subnets[?VpcId==`'"$EXISTING_VPC_ID"'`].AvailabilityZone' --output text | tr '\t' '\n' | sort -u)
  [ "$(echo "$AZS" | wc -l | tr -d ' ')" -ge 2 ] || die "Private subnets must span at least 2 AZs (got: $AZS)"
  state_set VPC_ID "$EXISTING_VPC_ID";                    state_set VPC_CREATED false
  state_set PRIVATE_SUBNET_IDS "$EXISTING_PRIVATE_SUBNET_IDS"
  state_set PUBLIC_SUBNET_IDS  "${EXISTING_PUBLIC_SUBNET_IDS:-}"
  log "Recorded reused VPC. Continue with 02-cluster.sh."
  exit 0
fi

if [ "$(state_get VPC_CREATED)" = "true" ] && [ -n "$(state_get VPC_ID)" ]; then
  log "VPC $(state_get VPC_ID) already created by these scripts. Skipping."
  exit 0
fi

# ------------------------------------------------------------------ create path
CIDR="10.42.0.0/16"
PUBLIC_CIDRS=(10.42.0.0/20 10.42.16.0/20 10.42.32.0/20)
PRIVATE_CIDRS=(10.42.64.0/19 10.42.96.0/19 10.42.128.0/19)

log "Selecting availability zones"
ZONES=()
while read -r z; do [ -n "$z" ] && ZONES+=("$z"); done < <(aws ec2 describe-availability-zones \
  --filters Name=state,Values=available Name=zone-type,Values=availability-zone \
  --query 'AvailabilityZones[].ZoneName' --output text | tr '\t' '\n' | head -3)
[ "${#ZONES[@]}" -ge 2 ] || die "Need at least 2 available AZs in $AWS_REGION"
log "Using AZs: ${ZONES[*]}"

log "Creating VPC $CIDR"
VPC_ID=$(aws ec2 create-vpc --cidr-block "$CIDR" \
  --tag-specifications "ResourceType=vpc,Tags=[{Key=Name,Value=${CLUSTER_NAME}-vpc},{Key=Project,Value=${TAG_PROJECT}},{Key=ManagedBy,Value=${TAG_MANAGED_BY}}]" \
  --query Vpc.VpcId --output text)
state_set VPC_ID "$VPC_ID"; state_set VPC_CREATED true
aws ec2 modify-vpc-attribute --vpc-id "$VPC_ID" --enable-dns-support   '{"Value":true}'
aws ec2 modify-vpc-attribute --vpc-id "$VPC_ID" --enable-dns-hostnames '{"Value":true}'

log "Creating and attaching internet gateway"
IGW_ID=$(aws ec2 create-internet-gateway \
  --tag-specifications "ResourceType=internet-gateway,Tags=[{Key=Name,Value=${CLUSTER_NAME}-igw},{Key=Project,Value=${TAG_PROJECT}},{Key=ManagedBy,Value=${TAG_MANAGED_BY}}]" \
  --query InternetGateway.InternetGatewayId --output text)
aws ec2 attach-internet-gateway --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID"
state_set IGW_ID "$IGW_ID"

PUBLIC_IDS=(); PRIVATE_IDS=()
for i in "${!ZONES[@]}"; do
  az="${ZONES[$i]}"
  log "Creating subnets in $az"
  pub=$(aws ec2 create-subnet --vpc-id "$VPC_ID" --availability-zone "$az" --cidr-block "${PUBLIC_CIDRS[$i]}" \
    --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=${CLUSTER_NAME}-public-${az}},{Key=kubernetes.io/role/elb,Value=1},{Key=Project,Value=${TAG_PROJECT}},{Key=ManagedBy,Value=${TAG_MANAGED_BY}}]" \
    --query Subnet.SubnetId --output text)
  aws ec2 modify-subnet-attribute --subnet-id "$pub" --map-public-ip-on-launch
  PUBLIC_IDS+=("$pub")
  prv=$(aws ec2 create-subnet --vpc-id "$VPC_ID" --availability-zone "$az" --cidr-block "${PRIVATE_CIDRS[$i]}" \
    --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=${CLUSTER_NAME}-private-${az}},{Key=kubernetes.io/role/internal-elb,Value=1},{Key=Project,Value=${TAG_PROJECT}},{Key=ManagedBy,Value=${TAG_MANAGED_BY}}]" \
    --query Subnet.SubnetId --output text)
  PRIVATE_IDS+=("$prv")
done
state_set PUBLIC_SUBNET_IDS  "$(IFS=,; echo "${PUBLIC_IDS[*]}")"
state_set PRIVATE_SUBNET_IDS "$(IFS=,; echo "${PRIVATE_IDS[*]}")"

log "Allocating EIP and creating NAT gateway (single NAT: demo cost profile)"
EIP_ALLOC=$(aws ec2 allocate-address --domain vpc \
  --tag-specifications "ResourceType=elastic-ip,Tags=[{Key=Name,Value=${CLUSTER_NAME}-nat-eip},{Key=Project,Value=${TAG_PROJECT}},{Key=ManagedBy,Value=${TAG_MANAGED_BY}}]" \
  --query AllocationId --output text)
state_set EIP_ALLOC "$EIP_ALLOC"
NAT_ID=$(aws ec2 create-nat-gateway --subnet-id "${PUBLIC_IDS[0]}" --allocation-id "$EIP_ALLOC" \
  --tag-specifications "ResourceType=natgateway,Tags=[{Key=Name,Value=${CLUSTER_NAME}-nat},{Key=Project,Value=${TAG_PROJECT}},{Key=ManagedBy,Value=${TAG_MANAGED_BY}}]" \
  --query NatGateway.NatGatewayId --output text)
state_set NAT_ID "$NAT_ID"
log "Waiting for NAT gateway $NAT_ID to become available..."
aws ec2 wait nat-gateway-available --nat-gateway-ids "$NAT_ID"

log "Creating route tables"
PUB_RT=$(aws ec2 create-route-table --vpc-id "$VPC_ID" \
  --tag-specifications "ResourceType=route-table,Tags=[{Key=Name,Value=${CLUSTER_NAME}-public},{Key=Project,Value=${TAG_PROJECT}},{Key=ManagedBy,Value=${TAG_MANAGED_BY}}]" \
  --query RouteTable.RouteTableId --output text)
aws ec2 create-route --route-table-id "$PUB_RT" --destination-cidr-block 0.0.0.0/0 --gateway-id "$IGW_ID" >/dev/null
for s in "${PUBLIC_IDS[@]}"; do aws ec2 associate-route-table --route-table-id "$PUB_RT" --subnet-id "$s" >/dev/null; done
state_set PUBLIC_RT "$PUB_RT"

PRV_RT=$(aws ec2 create-route-table --vpc-id "$VPC_ID" \
  --tag-specifications "ResourceType=route-table,Tags=[{Key=Name,Value=${CLUSTER_NAME}-private},{Key=Project,Value=${TAG_PROJECT}},{Key=ManagedBy,Value=${TAG_MANAGED_BY}}]" \
  --query RouteTable.RouteTableId --output text)
aws ec2 create-route --route-table-id "$PRV_RT" --destination-cidr-block 0.0.0.0/0 --nat-gateway-id "$NAT_ID" >/dev/null
for s in "${PRIVATE_IDS[@]}"; do aws ec2 associate-route-table --route-table-id "$PRV_RT" --subnet-id "$s" >/dev/null; done
state_set PRIVATE_RT "$PRV_RT"

log "VPC ready: $VPC_ID"
log "  public:  $(state_get PUBLIC_SUBNET_IDS)"
log "  private: $(state_get PRIVATE_SUBNET_IDS)"
log "Continue with 02-cluster.sh."

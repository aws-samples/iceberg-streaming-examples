#!/usr/bin/env bash
#
# 99 - Teardown: delete everything these scripts CREATED, in reverse order, and
# nothing else. Resources recorded as reused (EXISTING_* / *_CREATED=false in
# .state/state.env) are protected and left untouched.
#
#   YES=1 ./99-teardown.sh          # non-interactive
#   PURGE_PREFIXES=1 ./99-teardown.sh   # on a REUSED bucket, also delete our
#                                       # warehouse/, checkpoint/, artifacts/ prefixes
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

created() { [ "$(state_get "${1}_CREATED")" = "true" ]; }

log "Teardown plan (only resources created by these scripts are deleted):"
echo "    cluster $CLUSTER_NAME:        $(created CLUSTER && echo "DELETE" || echo "keep - reused/not created")"
echo "    IRSA + IAM policy:           $(created IRSA && echo "DELETE" || echo "keep")"
echo "    ECR repo $ECR_REPO_NAME:     $(created ECR && echo "DELETE" || echo "keep")"
echo "    S3 bucket $(state_get BUCKET "$BUCKET"):  $(created BUCKET && echo "DELETE" || echo "keep - PURGE_PREFIXES=1 removes only our prefixes")"
echo "    Glue database $GLUE_DATABASE: $(created GLUE_DB && echo "DELETE" || echo "keep")"
echo "    VPC $(state_get VPC_ID):     $(created VPC && echo "DELETE" || echo "keep")"
confirm "Proceed?" || { log "Aborted."; exit 0; }

# ---------------------------------------------------------------- k8s workloads
# Anything inside the cluster disappears with the cluster; clean explicitly so
# reused clusters are left tidy and LB/ENI-backed objects release before VPC delete.
if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
  log "Deleting workloads in namespace $NAMESPACE"
  kubectl -n "$NAMESPACE" delete jobs --all --ignore-not-found --timeout=120s || true
  kubectl -n "$NAMESPACE" delete pods -l spark-role=driver --ignore-not-found --timeout=120s || true
  if created KAFKA; then
    kubectl -n "$NAMESPACE" delete deployment/kafka service/kafka --ignore-not-found --timeout=120s || true
  fi
  if created RBAC; then
    kubectl -n "$NAMESPACE" delete rolebinding/spark-driver role/spark-driver --ignore-not-found || true
  fi
fi

# ---------------------------------------------------------------- IRSA + policy
if created IRSA; then
  log "Deleting IRSA service account (eksctl-managed role + CloudFormation stack)"
  eksctl delete iamserviceaccount --cluster "$CLUSTER_NAME" --region "$AWS_REGION" \
    --namespace "$NAMESPACE" --name "$SERVICE_ACCOUNT" || warn "iamserviceaccount deletion failed (continuing)"
fi
if created POLICY; then
  log "Deleting IAM policy $(state_get POLICY_ARN)"
  aws iam delete-policy --policy-arn "$(state_get POLICY_ARN)" || warn "policy deletion failed (continuing)"
fi

# ---------------------------------------------------------------- namespace
if created NAMESPACE && ! created CLUSTER; then
  # Only needed on a reused cluster; a created cluster takes the namespace with it.
  log "Deleting namespace $NAMESPACE (created by these scripts on a reused cluster)"
  kubectl delete namespace "$NAMESPACE" --ignore-not-found --timeout=180s || true
fi

# ---------------------------------------------------------------- cluster
if created CLUSTER; then
  log "Deleting EKS cluster $CLUSTER_NAME (this takes ~10-15 minutes)"
  eksctl delete cluster --name "$CLUSTER_NAME" --region "$AWS_REGION" --wait
fi

# ---------------------------------------------------------------- ECR
if created ECR; then
  log "Deleting ECR repository $ECR_REPO_NAME (including images)"
  aws ecr delete-repository --repository-name "$ECR_REPO_NAME" --force >/dev/null || warn "ECR deletion failed"
fi

# ---------------------------------------------------------------- S3
B="$(state_get BUCKET "$BUCKET")"
if created BUCKET; then
  log "Emptying and deleting S3 bucket $B"
  aws s3 rm "s3://$B" --recursive --only-show-errors || true
  aws s3api delete-bucket --bucket "$B" || warn "bucket deletion failed"
elif [ "${PURGE_PREFIXES:-0}" = "1" ]; then
  log "Reused bucket $B: deleting ONLY our prefixes (warehouse/, checkpoint/, artifacts/)"
  for p in warehouse checkpoint artifacts; do
    aws s3 rm "s3://$B/$p/" --recursive --only-show-errors || true
  done
else
  log "Keeping bucket $B (reused). Set PURGE_PREFIXES=1 to remove our prefixes."
fi

# ---------------------------------------------------------------- Glue
if created GLUE_DB; then
  log "Deleting Glue database $GLUE_DATABASE (metadata only; data lived in the bucket)"
  aws glue delete-database --name "$GLUE_DATABASE" || warn "Glue database deletion failed"
fi

# ---------------------------------------------------------------- VPC
if created VPC; then
  VPC_ID="$(state_get VPC_ID)"
  log "Deleting VPC $VPC_ID and its components"
  NAT_ID="$(state_get NAT_ID)"
  if [ -n "$NAT_ID" ]; then
    log "  deleting NAT gateway $NAT_ID (waiting for release)"
    aws ec2 delete-nat-gateway --nat-gateway-id "$NAT_ID" >/dev/null || true
    aws ec2 wait nat-gateway-deleted --nat-gateway-ids "$NAT_ID" || true
  fi
  [ -n "$(state_get EIP_ALLOC)" ] && { aws ec2 release-address --allocation-id "$(state_get EIP_ALLOC)" || warn "EIP release failed"; }
  IGW_ID="$(state_get IGW_ID)"
  if [ -n "$IGW_ID" ]; then
    aws ec2 detach-internet-gateway --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID" || true
    aws ec2 delete-internet-gateway --internet-gateway-id "$IGW_ID" || true
  fi
  for s in $(state_get PUBLIC_SUBNET_IDS | tr ',' ' ') $(state_get PRIVATE_SUBNET_IDS | tr ',' ' '); do
    [ -n "$s" ] && { aws ec2 delete-subnet --subnet-id "$s" || warn "subnet $s deletion failed"; }
  done
  for rt in "$(state_get PUBLIC_RT)" "$(state_get PRIVATE_RT)"; do
    [ -n "$rt" ] && { aws ec2 delete-route-table --route-table-id "$rt" || warn "route table $rt deletion failed"; }
  done
  aws ec2 delete-vpc --vpc-id "$VPC_ID" || warn "VPC deletion failed (check for leftover ENIs/SGs and retry)"
fi

log "Teardown finished. Removing local state."
rm -f "$STATE_FILE" "$STATE_DIR/cluster.yaml"
log "Done. Reused resources (if any) were left untouched."

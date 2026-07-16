#!/usr/bin/env bash
#
# 04 - Identity: Glue database, least-privilege IAM policy, IRSA service account
# and the Kubernetes RBAC the Spark driver needs.
#
# AWS best practice applied here: pods get AWS permissions through IRSA (IAM
# Roles for Service Accounts) - a role scoped to ONE service account in ONE
# namespace - instead of node-instance-profile credentials shared by every pod.
# The policy is scoped to exactly our bucket and the bigdata Glue database.
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

BUCKET="$(state_get BUCKET "$BUCKET")"
POLICY_NAME="${CLUSTER_NAME}-spark-s3-glue"

# Reused buckets may be SSE-KMS encrypted: detect the key so the policy grants
# the pods GenerateDataKey/Decrypt on exactly that key (nothing else). The bucket
# config may reference the key by ARN, key id or alias/ - resolve to the key ARN.
BUCKET_KMS_KEY_ID="$(aws s3api get-bucket-encryption --bucket "$BUCKET" \
  --query "ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.KMSMasterKeyID" \
  --output text 2>/dev/null || true)"
BUCKET_KMS_KEY_ARN=""
if [ -n "$BUCKET_KMS_KEY_ID" ] && [ "$BUCKET_KMS_KEY_ID" != "None" ]; then
  BUCKET_KMS_KEY_ARN="$(aws kms describe-key --key-id "$BUCKET_KMS_KEY_ID" \
    --query "KeyMetadata.Arn" --output text 2>/dev/null || true)"
fi
[ -n "$BUCKET_KMS_KEY_ARN" ] && log "Bucket $BUCKET uses SSE-KMS key $BUCKET_KMS_KEY_ARN (policy will include scoped KMS access)"

# ------------------------------------------------------------------ Glue database
if aws glue get-database --name "$GLUE_DATABASE" >/dev/null 2>&1; then
  log "Glue database $GLUE_DATABASE already exists (reusing; teardown will keep it)"
  [ -n "$(state_get GLUE_DB_CREATED)" ] || state_set GLUE_DB_CREATED false
else
  log "Creating Glue database $GLUE_DATABASE"
  aws glue create-database --database-input "{\"Name\":\"$GLUE_DATABASE\",\"Description\":\"iceberg-streaming-examples\"}"
  state_set GLUE_DB_CREATED true
fi

# ------------------------------------------------------------------ IAM policy (least privilege)
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"
if aws iam get-policy --policy-arn "$POLICY_ARN" >/dev/null 2>&1; then
  log "IAM policy $POLICY_NAME already exists"
  [ -n "$(state_get POLICY_CREATED)" ] || state_set POLICY_CREATED false
else
  log "Creating IAM policy $POLICY_NAME (S3 scoped to $BUCKET, Glue scoped to $GLUE_DATABASE)"
  aws iam create-policy --policy-name "$POLICY_NAME" \
    --tags Key=Project,Value="$TAG_PROJECT" Key=ManagedBy,Value="$TAG_MANAGED_BY" \
    --policy-document "$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Bucket",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::${BUCKET}"
    },
    {
      "Sid": "S3Objects",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
      "Resource": "arn:aws:s3:::${BUCKET}/*"
    },
    {
      "Sid": "GlueCatalog",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:GetTable", "glue:GetTables",
        "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable"
      ],
      "Resource": [
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:catalog",
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:database/${GLUE_DATABASE}",
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:table/${GLUE_DATABASE}/*"
      ]
    }$( if [ -n "$BUCKET_KMS_KEY_ARN" ]; then cat <<KMS
,
    {
      "Sid": "KmsForBucketSse",
      "Effect": "Allow",
      "Action": ["kms:GenerateDataKey", "kms:Decrypt", "kms:DescribeKey"],
      "Resource": "${BUCKET_KMS_KEY_ARN}"
    }
KMS
fi )
  ]
}
EOF
)" >/dev/null
  state_set POLICY_CREATED true
fi
state_set POLICY_ARN "$POLICY_ARN"

# ------------------------------------------------------------------ namespace + IRSA
if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
  log "Namespace $NAMESPACE already exists"
  [ -n "$(state_get NAMESPACE_CREATED)" ] || state_set NAMESPACE_CREATED false
else
  log "Creating namespace $NAMESPACE"
  kubectl create namespace "$NAMESPACE"
  state_set NAMESPACE_CREATED true
fi

log "Creating IRSA service account $NAMESPACE/$SERVICE_ACCOUNT (eksctl manages the role via CloudFormation)"
eksctl create iamserviceaccount \
  --cluster "$CLUSTER_NAME" --region "$AWS_REGION" \
  --namespace "$NAMESPACE" --name "$SERVICE_ACCOUNT" \
  --attach-policy-arn "$POLICY_ARN" \
  --override-existing-serviceaccounts \
  --tags "Project=${TAG_PROJECT},ManagedBy=${TAG_MANAGED_BY}" \
  --approve
state_set IRSA_CREATED true

# ------------------------------------------------------------------ Spark RBAC
# The submitter creates the driver pod; the driver creates executor pods,
# services and configmaps. Namespace-scoped role, nothing cluster-wide.
log "Applying Spark RBAC in namespace $NAMESPACE"
kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-driver
  namespace: ${NAMESPACE}
rules:
  # patch/update included: Spark 4 manages the driver service via server-side apply (PATCH)
  - apiGroups: [""]
    resources: ["pods", "pods/log", "services", "configmaps", "persistentvolumeclaims"]
    verbs: ["create", "get", "list", "watch", "patch", "update", "delete", "deletecollection"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-driver
  namespace: ${NAMESPACE}
subjects:
  - kind: ServiceAccount
    name: ${SERVICE_ACCOUNT}
    namespace: ${NAMESPACE}
roleRef:
  kind: Role
  name: spark-driver
  apiGroup: rbac.authorization.k8s.io
EOF
state_set RBAC_CREATED true

log "Identity ready. Continue with 05-kafka.sh (or skip it when KAFKA_BOOTSTRAP is set)."

#!/usr/bin/env bash
#
# 03 - Artifacts: ECR repository + S3 bucket, application build, image push,
# and jar/descriptor upload to S3.
#
# Created (unless reused): an ECR repo (scan-on-push enabled) and an S3 bucket
# (public access blocked by default, SSE-S3 default encryption). Reuse the
# bucket with EXISTING_BUCKET=...; the scripts then only write under the
# artifacts/, warehouse/ and checkpoint/ prefixes and teardown never deletes it.
#
# The image bakes the code in (primary path). The jar is ALSO uploaded to
# s3://$BUCKET/artifacts/ so you can alternatively run the stock apache/spark
# image and pull the application from S3 (see the README, "Option B").
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

# ------------------------------------------------------------------ ECR repo
if aws ecr describe-repositories --repository-names "$ECR_REPO_NAME" >/dev/null 2>&1; then
  log "ECR repository $ECR_REPO_NAME already exists (reusing; teardown will keep it unless created here earlier)"
  [ -n "$(state_get ECR_CREATED)" ] || state_set ECR_CREATED false
else
  log "Creating ECR repository $ECR_REPO_NAME (scan on push enabled)"
  aws ecr create-repository --repository-name "$ECR_REPO_NAME" \
    --image-scanning-configuration scanOnPush=true \
    --encryption-configuration encryptionType=AES256 \
    --tags Key=Project,Value="$TAG_PROJECT" Key=ManagedBy,Value="$TAG_MANAGED_BY" >/dev/null
  state_set ECR_CREATED true
fi
state_set ECR_REPO "$ECR_REPO_NAME"

# ------------------------------------------------------------------ S3 bucket
if aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
  log "S3 bucket $BUCKET already exists ($( [ -n "${EXISTING_BUCKET:-}" ] && echo reused || echo found ); teardown protects reused buckets)"
  [ -n "$(state_get BUCKET_CREATED)" ] || state_set BUCKET_CREATED false
else
  log "Creating S3 bucket $BUCKET"
  if [ "$AWS_REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$BUCKET" >/dev/null
  else
    aws s3api create-bucket --bucket "$BUCKET" \
      --create-bucket-configuration LocationConstraint="$AWS_REGION" >/dev/null
  fi
  # New buckets block public access and use SSE-S3 by default; make it explicit anyway.
  aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  aws s3api put-bucket-tagging --bucket "$BUCKET" \
    --tagging "TagSet=[{Key=Project,Value=${TAG_PROJECT}},{Key=ManagedBy,Value=${TAG_MANAGED_BY}}]"
  state_set BUCKET_CREATED true
fi
state_set BUCKET "$BUCKET"

# ------------------------------------------------------------------ build the app jar
log "Building the application jar (emr profile: Spark/Iceberg provided by the image)"
(cd "$REPO_ROOT" && mvn -B -ntp -Pemr clean package -DskipTests -q)
APP_JAR="$REPO_ROOT/target/streaming-iceberg-ingest-1.0-SNAPSHOT.jar"
DESC="$REPO_ROOT/src/main/protobuf/VehicleTelemetry.desc"
AVSC="$REPO_ROOT/src/main/avro/VehicleTelemetry.avsc"
[ -f "$APP_JAR" ] || die "Build produced no jar at $APP_JAR"

# ------------------------------------------------------------------ upload to S3
log "Uploading jar + schemas to s3://$BUCKET/artifacts/"
aws s3 cp "$APP_JAR" "s3://$BUCKET/artifacts/app.jar" --only-show-errors
aws s3 cp "$DESC"    "s3://$BUCKET/artifacts/VehicleTelemetry.desc" --only-show-errors
aws s3 cp "$AVSC"    "s3://$BUCKET/artifacts/VehicleTelemetry.avsc" --only-show-errors

# ------------------------------------------------------------------ build + push the image
GIT_SHA="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo dev)"
CTX="$SCRIPT_DIR"
cp "$APP_JAR" "$CTX/app.jar"
cp "$DESC"    "$CTX/VehicleTelemetry.desc"
cp "$AVSC"    "$CTX/VehicleTelemetry.avsc"
trap 'rm -f "$CTX/app.jar" "$CTX/VehicleTelemetry.desc" "$CTX/VehicleTelemetry.avsc"' EXIT

log "Logging in to ECR"
aws ecr get-login-password | docker login --username AWS --password-stdin \
  "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

log "Building image for linux/$ARCH (override the base with SPARK_BASE_IMAGE=...)"
docker build --platform "linux/$ARCH" \
  ${SPARK_BASE_IMAGE:+--build-arg SPARK_BASE_IMAGE="$SPARK_BASE_IMAGE"} \
  -t "$ECR_URI:$IMAGE_TAG" -t "$ECR_URI:$GIT_SHA" \
  -f "$SCRIPT_DIR/Dockerfile" "$CTX"

log "Pushing $ECR_URI:{$IMAGE_TAG,$GIT_SHA}"
docker push "$ECR_URI:$IMAGE_TAG"
docker push "$ECR_URI:$GIT_SHA"
state_set IMAGE "$ECR_URI:$IMAGE_TAG"

log "Artifacts ready. Continue with 04-identity.sh."

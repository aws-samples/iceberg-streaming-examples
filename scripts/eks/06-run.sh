#!/usr/bin/env bash
#
# 06 - Run the pipeline on EKS.
#
#   ./06-run.sh producer [key=value ...]   # Kafka telemetry producer as a k8s Job
#   ./06-run.sh ingest   [key=value ...]   # Spark ingest via spark-submit (cluster mode)
#   ./06-run.sh status                     # pods in the spark namespace
#   ./06-run.sh logs                       # follow the newest Spark driver's logs
#   ./06-run.sh stop                       # delete producer jobs + spark driver pods
#
# Examples:
#   ./06-run.sh producer count=5000000 rate=100000
#   ./06-run.sh ingest dedup=merge compaction=inline
#   ./06-run.sh ingest mode=mor fileformat=orc fv=2 table=vehicle_telemetry_v2
#   JOB_CLASS=com.aws.emr.spark.maintenance.IcebergMaintenance ./06-run.sh ingest table=vehicle_telemetry dry-run=true trigger=availablenow
#
# How it works: a short-lived "submit" Job runs spark-submit inside the cluster
# (in-cluster API auth, no local Spark needed). spark-submit creates the driver
# pod, which creates the executors; all of them run the IRSA-enabled 'spark'
# service account, so S3/Glue access is pod-scoped (no node-wide credentials).
#
# Tunables (env): EXECUTORS=2 EXECUTOR_CORES=2 EXECUTOR_MEMORY=6g DRIVER_MEMORY=2g
#                 JOB_CLASS=com.aws.emr.spark.iot.SparkCustomIcebergIngest
#                 PRODUCER_CLASS=com.aws.emr.kafka.TelemetryProducer  (e.g. the CDC feed:
#                 PRODUCER_CLASS=com.aws.emr.kafka.KafkaCDCSimulator ./06-run.sh producer rate=100000)
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

CMD="${1:-}"; shift || true
IMAGE="$(state_get IMAGE "$ECR_URI:$IMAGE_TAG")"
BUCKET="$(state_get BUCKET "$BUCKET")"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-$(state_get KAFKA_BOOTSTRAP)}"
JOB_CLASS="${JOB_CLASS:-com.aws.emr.spark.iot.SparkCustomIcebergIngest}"
PRODUCER_CLASS="${PRODUCER_CLASS:-com.aws.emr.kafka.TelemetryProducer}"
EXECUTORS="${EXECUTORS:-2}"
EXECUTOR_CORES="${EXECUTOR_CORES:-2}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-6g}"
DRIVER_MEMORY="${DRIVER_MEMORY:-2g}"
RUN_ID="$(date +%Y%m%d%H%M%S)"

yaml_args() { # emit "$@" as YAML list items (safe passthrough into the Job manifest)
  for a in "$@"; do printf '            - "%s"\n' "$a"; done
}

case "$CMD" in
  # ---------------------------------------------------------------- producer
  producer)
    [ -n "$BOOTSTRAP" ] || die "No Kafka bootstrap known. Run 05-kafka.sh or export KAFKA_BOOTSTRAP."
    NAME="producer-$RUN_ID"
    log "Starting producer Job $NAME ($PRODUCER_CLASS, image $IMAGE, bootstrap $BOOTSTRAP)"
    kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${NAME}
  namespace: ${NAMESPACE}
  labels: { app: kafka-producer, project: ${TAG_PROJECT} }
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    metadata: { labels: { app: kafka-producer } }
    spec:
      restartPolicy: Never
      serviceAccountName: ${SERVICE_ACCOUNT}
      containers:
        - name: producer
          image: ${IMAGE}
          imagePullPolicy: Always
          command: ["java", "-cp", "/opt/spark/app/app.jar", "${PRODUCER_CLASS}"]
          args:
            - "bootstrap=${BOOTSTRAP}"
$(yaml_args "$@")
          resources:
            requests: { cpu: "1", memory: 1Gi }
            limits: { memory: 2Gi }
EOF
    log "Producer started. Follow it with: kubectl -n $NAMESPACE logs -f job/$NAME"
    ;;

  # ---------------------------------------------------------------- spark ingest
  ingest)
    [ -n "$BOOTSTRAP" ] || die "No Kafka bootstrap known. Run 05-kafka.sh or export KAFKA_BOOTSTRAP."
    APP_NAME="telemetry-ingest-$RUN_ID"
    NAME="spark-submit-$RUN_ID"
    log "Submitting $JOB_CLASS as $APP_NAME (image $IMAGE)"
    kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${NAME}
  namespace: ${NAMESPACE}
  labels: { app: spark-submit, project: ${TAG_PROJECT} }
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    metadata: { labels: { app: spark-submit } }
    spec:
      restartPolicy: Never
      serviceAccountName: ${SERVICE_ACCOUNT}
      containers:
        - name: spark-submit
          image: ${IMAGE}
          imagePullPolicy: Always
          command: ["/opt/spark/bin/spark-submit"]
          args:
            - "--master"
            - "k8s://https://kubernetes.default.svc"
            - "--deploy-mode"
            - "cluster"
            - "--name"
            - "${APP_NAME}"
            - "--class"
            - "${JOB_CLASS}"
            - "--conf"
            - "spark.kubernetes.namespace=${NAMESPACE}"
            - "--conf"
            - "spark.kubernetes.container.image=${IMAGE}"
            - "--conf"
            - "spark.kubernetes.container.image.pullPolicy=Always"
            - "--conf"
            - "spark.kubernetes.authenticate.driver.serviceAccountName=${SERVICE_ACCOUNT}"
            - "--conf"
            - "spark.kubernetes.authenticate.executor.serviceAccountName=${SERVICE_ACCOUNT}"
            # submit-and-exit: the driver pod keeps the (long-running) streaming query alive
            - "--conf"
            - "spark.kubernetes.submission.waitAppCompletion=false"
            - "--conf"
            - "spark.executor.instances=${EXECUTORS}"
            - "--conf"
            - "spark.executor.cores=${EXECUTOR_CORES}"
            - "--conf"
            - "spark.executor.memory=${EXECUTOR_MEMORY}"
            - "--conf"
            - "spark.driver.memory=${DRIVER_MEMORY}"
            # s3a:// (checkpoints) resolves credentials through the IRSA web identity
            - "--conf"
            - "spark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
            # explicit SDK region on driver + executors (pods must not depend on IMDS hop limits)
            - "--conf"
            - "spark.kubernetes.driverEnv.AWS_REGION=${AWS_REGION}"
            - "--conf"
            - "spark.executorEnv.AWS_REGION=${AWS_REGION}"
            - "local:///opt/spark/app/app.jar"
            # unified key=value application arguments (see JobConfig.usage())
            - "runtime=emr"
            - "catalog=glue"
            - "warehouse=s3://${BUCKET}/warehouse"
            - "checkpoint=s3a://${BUCKET}/checkpoint"
            - "bootstrap=${BOOTSTRAP}"
            - "descriptor=/opt/spark/app/VehicleTelemetry.desc"
            - "avro=/opt/spark/app/VehicleTelemetry.avsc"
$(yaml_args "$@")
          resources:
            requests: { cpu: 500m, memory: 512Mi }
            limits: { memory: 1Gi }
EOF
    log "Submitted. The driver pod appears as '${APP_NAME}-*-driver'."
    log "  status: ./06-run.sh status      logs: ./06-run.sh logs      stop: ./06-run.sh stop"
    ;;

  # ---------------------------------------------------------------- utilities
  status)
    kubectl -n "$NAMESPACE" get pods -o wide
    ;;
  logs)
    DRIVER=$(kubectl -n "$NAMESPACE" get pods -l spark-role=driver \
      --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null || true)
    [ -n "$DRIVER" ] || die "No Spark driver pod found in $NAMESPACE."
    log "Following logs of $DRIVER (grep for [stream-progress])"
    kubectl -n "$NAMESPACE" logs -f "$DRIVER"
    ;;
  stop)
    log "Deleting producer/submit jobs and Spark driver pods in $NAMESPACE"
    kubectl -n "$NAMESPACE" delete jobs -l project="$TAG_PROJECT" --ignore-not-found
    kubectl -n "$NAMESPACE" delete pods -l spark-role=driver --ignore-not-found
    ;;
  *)
    die "Usage: $0 {producer|ingest|status|logs|stop} [key=value ...]"
    ;;
esac

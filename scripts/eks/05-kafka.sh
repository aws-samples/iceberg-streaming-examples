#!/usr/bin/env bash
#
# 05 - Kafka: deploy a single-node demo broker (KRaft, official apache/kafka
# image - the in-cluster twin of docker-compose.yml) into the spark namespace.
#
# DEMO ONLY: one replica, PLAINTEXT, ephemeral storage (emptyDir). For anything
# real, point the jobs at Amazon MSK instead by exporting
#   KAFKA_BOOTSTRAP=<broker:9092>
# before running this script (it then skips deployment entirely) - the MSK
# cluster must be reachable from the EKS VPC (peering/same VPC + security groups).
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

if [ -n "${KAFKA_BOOTSTRAP:-}" ]; then
  log "KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP is set - using the external broker, deploying nothing."
  state_set KAFKA_BOOTSTRAP "$KAFKA_BOOTSTRAP"
  state_set KAFKA_CREATED false
  exit 0
fi

BROKER_FQDN="kafka.${NAMESPACE}.svc.cluster.local"

log "Deploying single-node demo Kafka (apache/kafka:4.3.1, KRaft) to namespace $NAMESPACE"
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka
  namespace: ${NAMESPACE}
  labels: { app: kafka, project: ${TAG_PROJECT} }
spec:
  replicas: 1
  strategy: { type: Recreate }   # single-node KRaft: never two brokers with the same node id
  selector: { matchLabels: { app: kafka } }
  template:
    metadata: { labels: { app: kafka } }
    spec:
      containers:
        - name: kafka
          image: apache/kafka:4.3.1
          ports:
            - { containerPort: 9092, name: broker }
            - { containerPort: 9093, name: controller }
          env:
            - { name: KAFKA_NODE_ID, value: "1" }
            - { name: KAFKA_PROCESS_ROLES, value: "controller,broker" }
            - { name: CLUSTER_ID, value: "iceberg-streaming-eks-demo00" }
            - { name: KAFKA_LISTENERS, value: "CONTROLLER://:9093,PLAINTEXT://:9092" }
            - { name: KAFKA_ADVERTISED_LISTENERS, value: "PLAINTEXT://${BROKER_FQDN}:9092" }
            - { name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, value: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT" }
            - { name: KAFKA_CONTROLLER_QUORUM_VOTERS, value: "1@localhost:9093" }
            - { name: KAFKA_CONTROLLER_LISTENER_NAMES, value: "CONTROLLER" }
            - { name: KAFKA_INTER_BROKER_LISTENER_NAME, value: "PLAINTEXT" }
            # high-throughput defaults mirroring docker-compose.yml
            - { name: KAFKA_NUM_PARTITIONS, value: "32" }
            - { name: KAFKA_AUTO_CREATE_TOPICS_ENABLE, value: "true" }
            - { name: KAFKA_COMPRESSION_TYPE, value: "producer" }
            - { name: KAFKA_MESSAGE_MAX_BYTES, value: "10485760" }
            - { name: KAFKA_SOCKET_REQUEST_MAX_BYTES, value: "104857600" }
            - { name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, value: "1" }
            - { name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, value: "1" }
            - { name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR, value: "1" }
            - { name: KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR, value: "1" }
            - { name: KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR, value: "1" }
            - { name: KAFKA_LOG_RETENTION_MS, value: "1200000" }
          resources:
            requests: { cpu: "1", memory: 2Gi }
            limits: { memory: 4Gi }
          volumeMounts:
            - { name: data, mountPath: /tmp/kraft-combined-logs }
      volumes:
        - name: data
          emptyDir: {}   # ephemeral by design: a demo feed, not durable storage
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
  namespace: ${NAMESPACE}
  labels: { app: kafka, project: ${TAG_PROJECT} }
spec:
  selector: { app: kafka }
  ports:
    - { port: 9092, targetPort: 9092, name: broker }
EOF

log "Waiting for the broker to become ready"
kubectl -n "$NAMESPACE" rollout status deployment/kafka --timeout=180s

state_set KAFKA_BOOTSTRAP "${BROKER_FQDN}:9092"
state_set KAFKA_CREATED true
log "Kafka ready at ${BROKER_FQDN}:9092. Continue with 06-run.sh."

# Spark-on-EKS image for the iceberg-streaming-examples jobs.
#
# Base: the official Apache Spark image (Spark provides itself - the app jar is
# built with the `emr` Maven profile, so Spark/Iceberg are NOT shaded into it).
# On top of the base we add exactly the runtime jars the jobs need:
#   * Iceberg Spark runtime + AWS bundle (Glue catalog + S3FileIO)
#   * the Spark Kafka connector and its dependencies (kafka-clients pinned to
#     3.9.x - the line Spark 4.0.2 is built against)
#   * hadoop-aws + AWS SDK v2 bundle (s3a:// for streaming checkpoints; the
#     stock image does not ship them)
# plus the application jar and the protobuf descriptor.
#
# Build (from scripts/eks, after 03-artifacts.sh stages app.jar + the .desc):
#   docker build --platform linux/arm64 -t <ecr>/iceberg-streaming-examples .
#
# All versions are pinned and overridable with --build-arg.

ARG SPARK_BASE_IMAGE=apache/spark:4.0.2

# ---------------------------------------------------------------- deps stage
FROM curlimages/curl:8.10.1 AS deps
ARG SPARK_VERSION=4.0.2
ARG SCALA_BINARY=2.13
ARG ICEBERG_VERSION=1.11.0
ARG KAFKA_CLIENTS_VERSION=3.9.1
ARG COMMONS_POOL2_VERSION=2.12.0
ARG HADOOP_AWS_VERSION=3.4.1
ARG AWS_SDK_BUNDLE_VERSION=2.24.6

USER root
WORKDIR /tmp/jars
RUN M2=https://repo1.maven.org/maven2 && \
    curl -fsSLO $M2/org/apache/iceberg/iceberg-spark-runtime-4.0_${SCALA_BINARY}/${ICEBERG_VERSION}/iceberg-spark-runtime-4.0_${SCALA_BINARY}-${ICEBERG_VERSION}.jar && \
    curl -fsSLO $M2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar && \
    curl -fsSLO $M2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar && \
    curl -fsSLO $M2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar && \
    curl -fsSLO $M2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar && \
    curl -fsSLO $M2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar && \
    curl -fsSLO $M2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSLO $M2/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar

# ---------------------------------------------------------------- final image
FROM ${SPARK_BASE_IMAGE}
USER root
COPY --from=deps /tmp/jars/*.jar /opt/spark/jars/
# Staged by 03-artifacts.sh next to this Dockerfile:
COPY app.jar /opt/spark/work-dir/app.jar
COPY VehicleTelemetry.desc /opt/spark/work-dir/VehicleTelemetry.desc
COPY VehicleTelemetry.avsc /opt/spark/work-dir/VehicleTelemetry.avsc
RUN chown -R spark:spark /opt/spark/work-dir
USER spark

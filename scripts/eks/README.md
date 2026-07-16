# Running the examples on Amazon EKS

Numbered scripts that stand up (or adopt) everything needed to run the EV telemetry pipeline on
**Spark on Kubernetes**: VPC, EKS cluster, ECR image with the code baked in, S3 bucket for the
Iceberg warehouse + checkpoints, Glue database, IRSA identity, an in-cluster demo Kafka, and a
runner. A teardown script removes **only what the scripts created** - adopted resources are
recorded as reused and never touched.

> **Cost warning.** An EKS control plane (~$0.10/h), three `m7g.xlarge` nodes, a NAT gateway and
> S3/ECR storage accrue charges while they exist. Run `99-teardown.sh` when you are done.

## Prerequisites

`aws` CLI v2 (with credentials for an account where you may create these resources), `eksctl`,
`kubectl`, `docker` (daemon running), `mvn`, `jq`. Verify everything with `./00-preflight.sh`.

## Quick start (create everything)

```bash
cd scripts/eks
./00-preflight.sh          # tools + credentials + plan (read-only)
./01-vpc.sh                # VPC 10.42.0.0/16, 3 AZs, public+private subnets, single NAT
./02-cluster.sh            # eksctl cluster on the private subnets, OIDC enabled (~15-20 min)
./03-artifacts.sh          # ECR repo + S3 bucket; build jar; push image; upload jar+schemas to S3
./04-identity.sh           # Glue db 'bigdata', least-privilege IAM policy, IRSA SA, Spark RBAC
./05-kafka.sh              # single-node demo Kafka in-cluster (skip with KAFKA_BOOTSTRAP=...)
./06-run.sh producer count=5000000 rate=100000
./06-run.sh ingest dedup=merge compaction=inline
./06-run.sh logs           # follow the driver; grep for [stream-progress]
```

The ingest lands in the Iceberg v3 table `bigdata.vehicle_telemetry` (Glue catalog, data under
`s3://<bucket>/warehouse`, checkpoints under `s3a://<bucket>/checkpoint`). All the repo's
`key=value` knobs pass straight through, so the whole matrix works here too:

```bash
./06-run.sh ingest mode=mor fileformat=orc fv=2 table=vehicle_telemetry_v2
./06-run.sh producer format=json corrupt=true count=1000000
./06-run.sh ingest source=json dedup=batch          # exercises the dead-letter table
JOB_CLASS=com.aws.emr.spark.maintenance.IcebergMaintenance \
  ./06-run.sh ingest table=vehicle_telemetry dry-run=true
```

## Reusing existing resources

Set these before running the scripts; adopted resources are validated, recorded as *reused* in
`.state/state.env`, and `99-teardown.sh` will not delete them:

| Variable | Effect |
|---|---|
| `EXISTING_VPC_ID` + `EXISTING_PRIVATE_SUBNET_IDS` (+ `EXISTING_PUBLIC_SUBNET_IDS`) | skip VPC creation; private subnets must span >=2 AZs |
| `EXISTING_CLUSTER_NAME` | skip VPC + cluster creation; the cluster only gets an OIDC provider association (idempotent), a namespace, an IRSA SA and RBAC |
| `EXISTING_BUCKET` | skip bucket creation; the scripts only write the `artifacts/`, `warehouse/` and `checkpoint/` prefixes |
| `KAFKA_BOOTSTRAP` | skip the demo broker; point the jobs at MSK or any reachable Kafka (network access from the EKS VPC is on you) |

Other knobs: `AWS_REGION`, `CLUSTER_NAME`, `K8S_VERSION`, `ARCH=arm64|amd64` (switches nodegroup
type m7g/m6i **and** the docker build platform), `NODE_TYPE`, `NAMESPACE`, `SERVICE_ACCOUNT`,
`ECR_REPO_NAME`, `SPARK_BASE_IMAGE`, `BUCKET_KMS_KEY` (see *Encryption*), and for `06-run.sh`:
`EXECUTORS`, `EXECUTOR_CORES`, `EXECUTOR_MEMORY`, `DRIVER_MEMORY`, `JOB_CLASS`, `PRODUCER_CLASS`.

## How it works

* **Image (option A, default).** `03-artifacts.sh` builds the app jar with the `emr` Maven profile
  (Spark/Iceberg *not* shaded in) and bakes it into an image based on the official `apache/spark`
  base, together with the exact runtime jars the jobs need (Iceberg Spark runtime + AWS bundle,
  the Spark Kafka connector with `kafka-clients` 3.9.x, `hadoop-aws` + AWS SDK v2 bundle for
  `s3a://` checkpoints) and the protobuf/Avro schemas. Versions are pinned in the `Dockerfile`.
* **S3 artifacts (option B).** The jar and schemas are also uploaded to
  `s3://<bucket>/artifacts/`, so you can run the *stock* `apache/spark` image and fetch the code
  from S3 instead: point spark-submit at `s3a://<bucket>/artifacts/app.jar` and add the same
  connector jars via `--packages`. Option A is what `06-run.sh` uses - it avoids per-pod Ivy
  downloads and keeps startup fast and reproducible.
* **Submission.** `06-run.sh ingest` creates a short-lived Job that runs `spark-submit` in cluster
  mode against the in-cluster API (`k8s://https://kubernetes.default.svc`), with
  `waitAppCompletion=false`: the submit pod exits, the driver pod keeps the streaming query alive
  and manages its executors. No local Spark installation is needed.
* **Identity.** Driver and executors run the `spark` service account with **IRSA**: an IAM role
  scoped to that one service account, carrying a least-privilege policy (S3 restricted to the demo
  bucket, Glue restricted to the `bigdata` database). No node-wide credentials.
* **Encryption.** Buckets created by `03-artifacts.sh` default to SSE-KMS with the **AWS managed
  key** (`aws/s3`, bucket keys enabled) - nothing to manage and no extra IAM statements. Set
  `BUCKET_KMS_KEY=<key arn>` before `03-artifacts.sh` to use a customer-managed key instead.
  Either way `04-identity.sh` inspects the bucket's default encryption (created *or* reused): a
  customer-managed key gets a scoped `GenerateDataKey`/`Decrypt`/`DescribeKey` statement on
  exactly that key; SSE-S3 and the AWS managed key need none.
* **Networking.** Nodes run on private subnets; egress goes through a single NAT gateway (demo
  cost profile - production wants one NAT per AZ). Public subnets are tagged
  `kubernetes.io/role/elb`, private ones `kubernetes.io/role/internal-elb`, so load balancers
  place correctly if you add any.
* **Kafka.** `05-kafka.sh` deploys a single-node KRaft broker (the in-cluster twin of
  `docker-compose.yml`): one replica, PLAINTEXT, ephemeral storage - a demo feed, not a message
  store. Use MSK via `KAFKA_BOOTSTRAP=` for anything real.
* **State.** Every script records what it created (vs adopted) in `.state/state.env` and tags all
  AWS resources with `Project=iceberg-streaming-examples` / `ManagedBy=scripts-eks`.

## Teardown

```bash
./99-teardown.sh                     # interactive; shows a DELETE/keep plan first
YES=1 ./99-teardown.sh               # non-interactive
PURGE_PREFIXES=1 ./99-teardown.sh    # on a REUSED bucket, also remove our three prefixes
```

Deletion order: k8s workloads -> IRSA + IAM policy -> namespace (only on a reused cluster) ->
cluster -> ECR repo -> S3 bucket (or prefixes) -> Glue database -> NAT/EIP/IGW/subnets/route
tables/VPC. Each step is skipped for resources marked as reused.

## Troubleshooting

* `06-run.sh logs` follows the newest driver pod; `[stream-progress]` lines carry the per-batch
  throughput. `06-run.sh status` lists pods; `06-run.sh stop` kills producers and drivers.
* Driver stuck in `Pending`: the nodegroup is full - lower `EXECUTORS`/memory or raise `maxSize`.
* `Access Denied` on S3/Glue: confirm the pods show the `AWS_WEB_IDENTITY_TOKEN_FILE` env var
  (IRSA webhook) and that `04-identity.sh` ran after the cluster existed.
* Image pull errors on Apple Silicon builds: keep `ARCH=arm64` (Graviton nodes) so the build is
  native; `ARCH=amd64` builds run under emulation and are slow but work.

# Streaming Apache Iceberg examples using Apache Spark
AWS Managed Kafka and Apache Kafka, a distributed event streaming platform, has become the de facto standard for building real-time data pipelines. However, ingesting and storing large amounts of streaming data in a scalable and performant manner can be complex and resource-intensive task, often leading to performance issues and increased costs.

This project covers  how open table formats, such as Apache Iceberg, can help address these challenges. It provides  a solution that combines the power of [Apache Kafka](https://kafka.apache.org/) , [Apache Spark](https://spark.apache.org/), and [Apache Iceberg](https://iceberg.apache.org/) to achieve high-throughput streaming ingestion

The focus in this repository is to go further than the typical poc consuming few messages or small csv files. The aim here is to provide support for around **400,000 msg/seg** on all scenarios. 

The concepts seen here are applicable to PySpark or Scala programs with little effort. Remember that we just program
the transformations and those are converted to a logical plan and then to native code via the Java Virtual Machine (JVM) or to native code using projects such as [Apache Data Fusion Comet](https://github.com/apache/datafusion-comet), [Velox](https://github.com/apache/datafusion-comet) or [Photon](https://www.databricks.com/product/photon).

Why Java? Because why not, remember that this nowadays gets executed by the JVM ( until previous projects arise). Remember that with this approach we can use libraries in an easy way ( without the Scala/Python/Java 'mess'), we can program performant UDFs and there is a friendly local development environment (where you can debug everything with breakpoints) with different options.

The example uses maven profiles to automatically filter required libraries when deployed to [Amazon EMR](https://aws.amazon.com/emr/) ( the Spark and Iceberg libraries will be marked as provided) and therefore you will be using the optimized Spark runtime from EMR. The logging is implemented using [Log4j2](https://logging.apache.org/log4j/2.12.x/) ( where its config can be further tuned using EMR Serverless configs) as Spark uses it behind the scenes. 

**Environment types:** 

All three scenarios run from the same jar and the same class — you just change the order-independent
`key=value` arguments (see [Run configuration and unified arguments](#run-configuration-and-unified-arguments)):

- Local development using a [dockerized Kafka](docker-compose.yml) (the official `apache/kafka`
  image in KRaft mode), a Hadoop file catalog under `./warehouse` (`catalog=local`).
- Local development (still `runtime=local`, great for debugging) writing to Amazon S3 through the
  AWS Glue Data Catalog (`catalog=glue`) or to an Amazon S3 Tables managed bucket
  (`catalog=s3tables`), with the dockerized or a remote Kafka.
- Production on Amazon EMR / EMR Serverless (`runtime=emr`) with `catalog=glue` or
  `catalog=s3tables`, on release label `emr-spark-8.0.0` (Spark 4.0.2, Scala 2.13, Iceberg 1.10.1).

You can run these examples on any Spark compatible runtime too, but that's for a pull request ( if you like to contribute).

In the case of Amazon Web Services on AWS Glue, Amazon EMR or Amazon EMR Serverless.
æ
Remember also that these jobs and code can be adapted for **batch mode** easily (and remember that you can use Kafka as batch source!). A batch job is just a special streaming job with a start and an end anyway.

### Run configuration and unified arguments

Every Spark example shares a single, self-documenting configuration helper
(`com.aws.emr.common.JobConfig`). Instead of relying on the number of positional arguments, all
jobs now take order-independent `key=value` arguments, so the same jar and the same class can run in
any of the three supported scenarios just by changing the arguments:

```
runtime=local|emr           where Spark runs (default: local -> master local[*]; emr -> inferred)
catalog=local|glue|s3tables Iceberg catalog / storage (default: local)
warehouse=<path|s3 uri|arn> catalog warehouse (default 'warehouse' for local;
                            an s3://... URI for glue; the table bucket ARN for s3tables)
checkpoint=<path|s3 uri>    structured streaming checkpoint dir (default: tmp/)
bootstrap=<host:port,...>   Kafka bootstrap servers (default: localhost:9092)
descriptor=<path>           protobuf descriptor file (default: Employee.desc)
avro=<path>                 Avro .avsc schema file (default: ./src/main/avro/Employee.avsc)
dedup=true|false            enable deduplication (default: false)
compaction=true|false       enable periodic/async compaction (default: false)
shuffle=<n>                 spark.sql.shuffle.partitions initial value; AQE coalesces (default: 200 local / 800 cloud)
region=<aws-region>         Glue Schema Registry region (SparkProtoRegistry only, default eu-west-1)
```

The three run scenarios map to arguments as follows:

1. **Local development** (Hadoop file catalog under `./warehouse`, Kafka on `localhost:9092`) — run
   with no arguments, or just toggle behaviour, e.g. `dedup=true compaction=true`.
2. **Local Spark on top of Amazon S3 / S3 Tables** — keep `runtime=local` (great for debugging with
   breakpoints) but store the data in the cloud:
   - S3 via the AWS Glue Data Catalog:
     `catalog=glue warehouse=s3://your-bucket/warehouse checkpoint=s3://your-bucket/checkpoint bootstrap=...`
   - Amazon S3 Tables (managed Iceberg):
     `catalog=s3tables warehouse=arn:aws:s3tables:<region>:<account>:bucket/<table-bucket> checkpoint=s3://your-bucket/checkpoint bootstrap=...`
     For local S3 Tables runs you must also put the S3 Tables catalog client on the classpath, for
     example by adding
     `--conf spark.jars.packages=software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8`
     (it is a `provided` dependency in the build so it is not shaded into the jar).
3. **Amazon EMR on S3 / S3 Tables** — set `runtime=emr` so the master is inferred from the cluster,
   and pick `catalog=glue` or `catalog=s3tables` with the appropriate `warehouse`. On
   `emr-spark-8.0.0` the Iceberg and S3 Tables runtimes are provided by EMR.

You need valid AWS credentials on your machine for the `glue` and `s3tables` catalogs when running
locally (the standard AWS credential chain is used).

### A note on Iceberg v3 tables

All of the examples create their tables as **Apache Iceberg format-version 3 (v3)** tables
(`'format-version'='3'` in the `TBLPROPERTIES`). Iceberg v3 became production ready with Apache
Iceberg 1.11.0 and brings, among other things, deletion vectors (used automatically by the
merge-on-read examples instead of v2 positional delete files), row lineage, the VARIANT type,
default column values, nanosecond timestamps and multi-argument partition transforms. The
merge-on-read jobs therefore write more efficient row-level deletes out of the box on v3.

### A note on performance

Although the code here aims for performance more tuning can be done for achieving specific goals such as improving latency.

Remember that Apache Iceberg have merge-on-read capabilities. In this repo, the default settings for tables are used
but mixing copy-on-write with merge-on-read can lead to some gains as we will write faster.

Remember that this is not a free lunch, you will need to compact if you want good performance.

Another cool thing to test is to use Avro for the ingestion tables and then compact to parquet. 

A good doc to read about these settings and more can be seen on the [Best Practices for Optimizing Apache Iceberg workloads](https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/best-practices.html) from AWS Documentation.

Another good read can be seen on this blog from Cloudera: [Optimization Strategies for Iceberg Tables](https://blog.cloudera.com/optimization-strategies-for-iceberg-tables/)

## PySpark alternative (`python/`)

A complete **PySpark** counterpart of these examples lives in the [`python/`](python/) folder,
managed with [`uv`](https://docs.astral.sh/uv/). Every Spark job here is replicated in Python, uses
the **same unified `key=value` arguments** and the same three run scenarios (local, local on
S3/S3 Tables, and EMR on S3/S3 Tables), and creates the same **Iceberg format-version 3 (v3)**
tables. The shared `com.aws.emr.common.JobConfig` helper is mirrored by
`iceberg_streaming.common.jobconfig`, which additionally wires up `spark.jars.packages` so that
local runs pull the Kafka/Avro/Protobuf connectors and the Iceberg 4.0 / S3 Tables runtimes from
Maven Central (on EMR those are provided by the runtime).

Quick start:

```bash
cd python
uv sync                       # Python 3.12 + PySpark 4.0.2 + deps
uv run iot-custom-ingest      # pure local dev (hadoop catalog, kafka on localhost:9092)
# local Spark on S3 via Glue:
uv run iot-custom-ingest catalog=glue warehouse=s3://your-bucket/warehouse bootstrap=broker:9092 dedup=true
# on EMR (master inferred from the cluster):
#   spark-submit ... iceberg_streaming/iot/spark_custom_iceberg_ingest.py runtime=emr catalog=glue warehouse=s3://...
```

Console entry points exist for every job (`iot-mor`, `proto-native`, `proto-udf`, `avro-native`,
`cdc-log-change`, `cdc-mirror`, `cdc-incremental`, the `iot-s3buckets-*` variants, `iceberg-utils`)
plus native Kafka producers/consumers (`proto-producer`, `avro-producer`, `json-producer`,
`cdc-simulator`, `proto-consumer`, `avro-consumer`). See [`python/README.md`](python/README.md) for
the full table, setup, protobuf-binding generation and troubleshooting.

Differences from the Java project: the AWS Glue Schema Registry clients (including the
`SparkProtoRegistry` Spark consumer) are **not** ported — there is no first-class Python Glue Schema
Registry serde and that deserializer is JVM-only, so use the Java jobs for the Glue Schema Registry
scenarios. The JSON example is a plain native JSON producer/consumer.

## IoT Scenarios

Here we have different approaches and comœmon formats. About the different scenarios the main idea is high throughput streaming
ingestion:
- Native Iceberg writing with deduplication via even-time watermarking.
- Custom process writing with compaction via n-batches and deduplication via merge into.
- Custom process writing with async compaction and Merge-on-read mode.

For the different formats we will have the native use case implemented and the ProtoBuf one will have all the scenarios.

The most advanced example using Protocol Buffers is in ```com.aws.emr.spark.iot``` package.

Later on a job rewriting older partitions to check for duplicates are found and rewrite affected partitions may run. 
An example of such approach can be seen also on the Utils class of ```com.aws.emr.spark.iot``` package.

Remember that exactly once systems are difficult to implement and that for Spark you will need and idempotent sink.

If you want to use the GlueSchemaRegistry you should create in the console a stream registry named ```employee-schema-registry```.

### Protocol Buffers

[Protocol Buffers](https://protobuf.dev/) are language-neutral, platform-neutral extensible mechanisms for serializing structured data.

**Examples**: 
- Native Java Producer/Consumer. 
- AWS Glue Registry based Java Producer/Consumer.
- Native Spark Structured streaming consumer. 
- UDF based Spark Structured streaming consumer.
- AWS Glue Schema Registry based Spark Structured streaming consumer (`SparkProtoRegistry`).

Create a schema for the Glue registry ```Employee.proto``` if you like to use the Registry based producer/consumer:

```
syntax = "proto3";
package gsr.proto.post;

import "google/protobuf/wrappers.proto";
import "google/protobuf/timestamp.proto";

message Employee {
      int32 id = 1;
      string name = 2;
      string address = 3;
      google.protobuf.Int32Value employee_age = 4;
      google.protobuf.Timestamp start_date = 5;
     Team team = 6;
     Role role = 7;

}
message Team {
     string name = 1;
     string location = 2;
}
enum Role {
     MANAGER = 0;
     DEVELOPER = 1;
     ARCHITECT = 2;
}
```

### Apache Avro

[Apache Avro](https://avro.apache.org/) - a data serialization system.

**Examples**: 
- Native Java Producer/Consumer. 
- AWS Glue Registry based Java Producer/Consumer.
- Native Spark Structured streaming consumer. 

Create a schema for the Glue registry ```Employee.avsc``` if you like to use the Registry based producer/consumer:
```
{"namespace": "gsr.avro.post",
 "type": "record",
 "name": "Employee",
 "fields": [
     {"name": "employee_id", "type": "long"},
     {"name": "age",  "type": "int"},
     {"name": "start_date",   "type": "long"},
   {"name": "team", "type": "string"},
   {"name": "role", "type": "string"},
   {"name": "address", "type": "string"},
   {"name": "name", "type": "string"}]
}
```

### Json

There is plenty of literature over the internet on how integrate Spark with Json data, therefore we just implemented one usecase.

**Examples**:
- AWS Glue Registry based Java Producer/Consumer.


Create a schema for the Glue registry ```Employee.json``` if you like to use the Registry based producer/consumer:
```
{
  "$id": "https://example.com/Employee.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Employee",
  "description": "",
  "type": "object",
  "properties": {
    "employeeId": {
      "description": "The unique identifier for a employee",
      "type": "integer"
    },
    "name": {
      "description": "Name of the employee",
      "type": "string"
    }
  }
}

```
## CDC Scenarios

Here the reference is Tabular [Apache Iceberg Cookbook](https://tabular.io/apache-iceberg-cookbook/) and these blogposts:
 - https://tabular.io/blog/hello-world-of-cdc/
 - https://tabular.io/blog/cdc-data-gremlins/#eventual-consistency-causes-data-gremlins
 - https://tabular.io/blog/cdc-merge-pattern/
 - https://tabular.io/blog/cdc-zen-art-of-cdc-performance/

Here we will focus on the Mirror MERGE patter, as stated in the Iceberg Cookbook the first part could be managed by 
the Kafka Connect Tabular connector, but we will implement both processing pipelines using Spark. 

The relevant classes are withing the ```com.aws.emr.spark.cdc``` package.  

 * ```KafkaCDCSimulator``` class is a Java producer simulating CDC data in [AWS Database Migration Service(DMS)](https://aws.amazon.com/es/dms/) format. 
 * ```SparkLogChange```  class is a Structured Streaming consumer that outputs a CDC changelog to an Iceberg table. 
 * ```SparkCDCMirror``` class is a Spark batch pipeline that process the MERGE using the Mirror approach.
 * ```SparkIncrementalPipeline``` class uses Incremental pipeline for consuming the CDC changelog into a target table. 

### Notes on the MERGE pattern

The mirror approach is a `MERGE INTO`. We first deduplicate the changelog keeping the latest change
per key with a windowed `row_number()`, then merge that single row per key into the target table.
The three `WHEN` branches map the CDC operation to a row-level action: a `D` on a matched key becomes
a `DELETE`, any other matched key is an `UPDATE`, and a new key that is not a delete is an `INSERT`.
On the v3 `merge-on-read` target the deletes are written as deletion vectors, so the merge stays
cheap on the write path. This is the core of `SparkCDCMirror`:

```sql
WITH windowed_changes AS (
    SELECT account_id, balance, last_updated, operation,
           row_number() OVER (PARTITION BY account_id ORDER BY last_updated DESC) AS row_num
    FROM accounts_changelog WHERE last_updated > current_timestamp() - INTERVAL 1 DAY
),
accounts_changes AS (SELECT * FROM windowed_changes WHERE row_num = 1)
MERGE INTO accounts_mirror a USING accounts_changes c
ON a.account_id = c.account_id
WHEN MATCHED AND c.operation = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET a.balance = c.balance, a.last_updated = c.last_updated
WHEN NOT MATCHED AND c.operation != 'D' THEN
    INSERT (account_id, balance, last_updated) VALUES (c.account_id, c.balance, c.last_updated)
```

#### Restrict the affected partitions in the `ON` clause

The most important knob for MERGE performance is limiting how much of the *target* table Spark has to
scan and rewrite. A join predicate on the join key alone (`ON a.account_id = c.account_id`) forces
Spark to consider the whole target. If you know your incoming batch only touches recent data (late
events at most a couple of hours old, for example), add a predicate on the partition column to the
`ON` clause so Iceberg can prune to just those partitions. `SparkCustomIcebergIngest` does exactly
this against the `employee` table, which is partitioned by `hours(start_date)`:

```sql
MERGE INTO bigdata.employee as t
USING insert_data as s
ON  `s`.`employee_id` = `t`.`employee_id`
    AND `t`.`start_date` > current_timestamp() - INTERVAL 1 HOURS
    AND `t`.`team` = 'Solutions Architects' AND `t`.`start_date` = `s`.`start_date`
WHEN NOT MATCHED THEN INSERT *
```

Because the table is bucketed hourly, `t.start_date > current_timestamp() - INTERVAL 1 HOURS` prunes
the target down to just the two latest hourly partitions (the current hour plus the previous one)
instead of the entire table. Adapt the interval and the partition predicate to your own late-arrival
window and partitioning. If you partition by bucket, you can restrict the merge to specific buckets
in the same way (`t.employee_id IN (...)` per bucket, or by materialising a bucket column and joining
on it) — see the inline comments in `SparkCustomIcebergIngest` for the trade-offs.

#### Tune commit retries

Streaming writes, periodic compaction and late-arriving MERGEs all commit against the same table with
optimistic concurrency, so commit conflicts are expected under load. Give Iceberg enough retries and
back-off in the table properties so a losing commit is retried instead of failing the job:

```
'commit.retry.num-retries'='10',   -- number of times to retry a commit before failing
'commit.retry.min-wait-ms'='250',  -- minimum back-off before retrying a commit
'commit.retry.max-wait-ms'='60000' -- (1 min) maximum back-off before retrying a commit
```

#### Enable partial progress on compaction

When you compact from inside the streaming job you are competing with the writer for commits. Run
`rewrite_data_files` with `partial-progress.enabled = true` so the rewrite commits in several smaller
batches (bounded by `partial-progress.max-commits`) rather than one big all-or-nothing commit. That
way a conflict only loses the current group, progress made so far is kept, and this is also why the
commit retries above matter. `SparkCustomIcebergIngest` triggers this every 10 batches, scoped to the
recent partitions:

```sql
CALL system.rewrite_data_files(
  table => 'employee',
  strategy => 'sort',
  sort_order => 'start_date',
  where => 'start_date >= current_timestamp() - INTERVAL 1 HOURS', -- only compact recent partitions
  options => map(
    'rewrite-job-order','bytes-asc',
    'target-file-size-bytes','273741824',
    'max-file-group-size-bytes','10737418240',
    'partial-progress.enabled', 'true',
    'partial-progress.max-commits', '10',
    'max-concurrent-file-group-rewrites', '10000'
  ))
```

#### Expire old metadata files automatically

Every commit writes a new table `metadata.json`, and with a fast streaming trigger these pile up
quickly and slow down planning. Let Iceberg clean them up on each commit by setting these table
properties, so you keep only a bounded number of previous metadata versions instead of an
ever-growing list:

```
'write.metadata.delete-after-commit.enabled' = 'true', -- remove old metadata files after each commit
'write.metadata.previous-versions-max' = '50'          -- keep at most 50 previous metadata.json versions
```

Snapshot and orphan-file expiration are a separate concern and are best left to a dedicated
maintenance job over older partitions.

## Requirements

* Java 17 or 21 (Apache Spark 4.0 dropped support for Java 8 and Java 11; Java 17 is the default and recommended runtime)
* Maven 3.9+
* 16GB of RAM and more than 2 cores. 
* Whatever IDE you like ([Intellij](https://www.jetbrains.com/intellij/), [Visual Studio Code](https://code.visualstudio.com/), [NetBeans](https://apache.netbeans.org/), etc)

For local development and testing you can use the provided ```docker-compose.yml``` to spin up a Kafka cluster.

You can generate the description file using the protobuf compiler like this. You need to install the protobuf compiler for your system, for example on MacOs is available on ```brew```. 

```protoc --include_imports --descriptor_set_out=Employee.desc Employee.proto'```

Remember that for simple scenarios you will be better suited using [Kafka Connect Tabular Iceberg Connector](https://github.com/tabular-io/iceberg-kafka-connect/tree/main) or using [Amazon Kinesis Firehose](https://aws.amazon.com/firehose/).

### Running on EMR Serverless:

Create a S3 bucket with the following structure. 

```
s3bucket/
	/jars
	/employee.desc -- or your custom protocol buffers descriptors
	/warehouse
	/checkpoint
```

Package your application using the ```emr``` Maven profile, then upload the jar of the project to the ```jars``` folder. The ```warehouse``` will be the place where the Iceberg Data and Metadata will live and ```checkpoint``` will be used for Structured Streaming checkpointing mechanismn. 
 
Create a Database in the AWS Glue Data Catalog with the name ```bigdata```.

You need to create an EMR Serverless application with ```default settings for batch jobs only```, application type ```Spark``` release version ```emr-spark-8.0.0``` (this release ships Apache Spark 4.0.2, Scala 2.13 and Apache Iceberg 1.10.1; note the release label is ```emr-spark-8.0.0```, not ```emr-8.0.0```) and ```x86_64``` as architecture, enable ```Java 17``` as runtime, enable ```AWS Glue Data Catalog as metastore```
integration and enable ```Cloudwatch logs``` if desired.

Then you can issue a job run using this aws cli command. Remember to change the desired parameters.

```
aws emr-serverless start-job-run     --application-id application-identifier     --name job-run-name     --execution-role-arn arn-of-emrserverless-role --mode 'STREAMING'     --job-driver
	'{
        "sparkSubmit": {
            "entryPoint": "s3://s3bucket/jars/streaming-iceberg-ingest-1.0-SNAPSHOT.jar",
            "entryPointArguments": ["runtime=emr","catalog=glue","warehouse=s3a://s3bucket/warehouse","descriptor=/home/hadoop/Employee.desc","checkpoint=s3a://s3bucket/checkpoint","bootstrap=kafkaBootstrapString","dedup=true","compaction=true"],
            "sparkSubmitParameters": "--class com.aws.emr.spark.iot.SparkCustomIcebergIngest --conf spark.executor.cores=4 --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.executor.memory=16g  --conf spark.driver.cores=2 --conf spark.driver.memory=8g  --files s3a://s3bucket/Employee.desc --conf spark.dynamicAllocation.minExecutors=4 --conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.emr-serverless.executor.disk.type=shuffle_optimized --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2"
        }
    }'
{	
```

Expected performance should be around 450.000 msgs per sec if you use the ```SparkCustomIcebergIngest```.

<img src="imgs/emr_performance.png" align="center" height="450" width="600"/>

You can also see the cluster autoscaling into action:

<img src="imgs/emr_cluster_autoscaling.png" align="center" height="470" width="550"/>

### Running on a local environment.

1. Install a Java SDK 17 like [Amazon Coretto](https://aws.amazon.com/corretto/).
2. Install [Docker](https://www.docker.com/) for your environment. 
3. Open the desired IDE. 
4. Use the IDE to issue the ```package ``` command of maven selecting the local profile.
5. If you wish to use the AWS Glue Data Catalog and S3 remember to have the corresponding permissions (have your AWS credentials avaliable), there are plugins for both [Intellij](https://aws.amazon.com/intellij/?pg=developertools) and [Visual Studio Code](https://aws.amazon.com/visualstudiocode/) that can be helpful here.
6. Start the local Kafka broker via ```docker-compose up``` command.
7. Run the examples with the desired arguments. Apache Spark 4.0 runs on Java 17 (or 21) and, because of the Java Module System, it needs a set of `--add-opens`/`--add-modules` start options to avoid `InaccessibleObjectException` / "unnamed module" access errors. Add the following VM options to your IDE run configuration (this is the exact default set that `spark-submit` injects for you via `org.apache.spark.launcher.JavaModuleOptions`, so you only need them for local runs launched directly from the IDE):
```
-XX:+IgnoreUnrecognizedVMOptions
--add-modules=jdk.incubator.vector
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
-Djdk.reflect.useDirectMethodHandle=false
-Dio.netty.tryReflectionSetAccessible=true
```

### Running the Kafka producer on AWS

Create a Amazon MSK cluster with at leas two brokers using a recent Apache Kafka version in [KRaft](https://kafka.apache.org/documentation/#kraft) mode (Apache Kafka 4.x removed ZooKeeper entirely, so KRaft is the only supported mode) and use as instance type ```kafka.m7g.xlarge```. Do not use public access and choose two private subnets to deploy it. For the security group remember that the EMR cluster and the EC2 based producer will need to reach the cluster and act accordingly. For security, use ```PLAINTEXT``` (in production you should secure access to the cluster). Choose ```200GB``` as storage size for each broker and do not enable ```Tiered storage```. For the cluster configuration use this one:

```
auto.create.topics.enable=true
default.replication.factor=3
min.insync.replicas=2
num.io.threads=8
num.network.threads=5
num.partitions=32
num.replica.fetchers=2
replica.lag.time.max.ms=30000
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
socket.send.buffer.bytes=102400
unclean.leader.election.enable=true
compression.type=zstd
log.retention.hours=2
log.retention.bytes=10073741824
```

Running the Kafka producer on an Amazon EC2 instance, remember to change the bootstrap connection string.

You will need to install Java if you are using and Amazon Linux instance. 
```
sudo yum install java-17-amazon-corretto-devel
```
Then, download the jar to the instance and execute the producer. With the following command you can start the Protocol Buffers Producer.
```
aws s3 cp s3://s3bucket/jars/streaming-iceberg-ingest-1.0-SNAPSHOT.jar .
java -cp streaming-iceberg-ingest-1.0-SNAPSHOT.jar com.aws.emr.proto.kafka.producer.ProtoProducer kafkaBoostrapString
```

Remember that your EC2 instance need to have network access to the MSK cluster, you will need to configure the VPC, Security Groups and Subnet/s. 

## Costs

Remember that this example is for high throughput scenarios and therefore the config may lead to quite big bill if deployed on top of AWS, remember to stop the EMR Serverless application, the used instance for the Kafka producer and delete the Amazon MSK cluster when not in use.

## Security

The code here is not secured in any way, you should secure your Apache Kafka cluster and be aware that some dependencies may have known vulnerabilities. If you deploy any service on top of AWS you should configure the roles using the least permission model
using [IAM roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) and [Amazon Lake Formation](https://aws.amazon.com/lake-formation/) if needed. 

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

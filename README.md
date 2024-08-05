# Benchmarking Glue Iceberg auto-compaction performance.

Ingestion performance for simulated IoT data with duplicates and late events ( around 20 billion events). Data is deduplicated using ```MERGE INTO``` from the latest two (time-based) partitions. The process involves reading those partitions and do a lot of shuffling ( is quite performance intensive). After simulated ingestion we make some queries using Athena to show the performance differences too.

IoT Simulator on EC2 →  Kafka → EMR Serverless Streaming Application → S3 (Iceberg) ← Athena

### Create S3 structure:

Create a S3 bucket with the following structure.

You can pick the descriptor file [here](https://github.com/aws-samples/iceberg-streaming-examples/blob/a997a59909203c5c6603e27105c18e16f271af01/src/main/protobuf/Employee.desc).
```
s3bucket/
	/jars
	/employee.desc 
	/warehouse
	/checkpoint
	/checkpointAuto
```
## Download the application on the releases page

Get the packaged application from the releases tab on the repo [link](https://github.com/aws-samples/iceberg-streaming-examples/releases/tag/auto-compaction-0.1), then upload the `jar` to the ```jars``` directory on the s3 bucket. The ```warehouse``` will be the place where the Iceberg Data and Metadata will live and ```checkpoint``` will be used for Structured Streaming checkpointing mechanism.

Create a Database in the AWS Glue Data Catalog with the name ```bigdata```.

You need to create an EMR Serverless application with ```default settings for batch jobs only```, application type ```Spark``` release version ```7.1.0``` and ```x86_64``` as architecture, enable ```Java 17``` as runtime, enable ```AWS Glue Data Catalog as metastore```
integration and enable ```Cloudwatch logs``` if desired.

Remember to configure the network (VPC and security groups as the application will need to reach the MSK cluster). 

## Create an Amazon MSK AWS and create an EC2 instance that will hold the data producer

Create a Amazon MSK cluster with at leas 4 brokers using ```3.5.1```, [Apache Zookeeper](https://zookeeper.apache.org/) mode version and use as instance type ```kafka.m7g.large```. Do not use public access and choose two private subnets to deploy it. For the security group remember that the EMR cluster and the EC2 based producer will need to reach the cluster and act accordingly. For security, use ```PLAINTEXT``` (in production you should secure access to the cluster). Choose ```200GB``` as storage size for each broker and do not enable ```Tiered storage```. For the cluster configuration use this one:

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
zookeeper.session.timeout.ms=18000
compression.type=zstd
log.retention.hours=2
log.retention.bytes=10073741824
```

Run the Kafka producer on an Amazon EC2 instance (```c5.xlarge```), remember to change the bootstrap connection string.

You will need to install Java if you are using and Amazon Linux instance, we will also download the Kafka binaries.
```
sudo yum install java-17-amazon-corretto-devel wget
wget https://archive.apache.org/dist/kafka/3.5.1/kafka_2.12-3.5.1.tgz
tar xzvf kafka_2.12-3.5.1.tgz 
```
Next we are going to create two topics ( one for the auto-compacted table and one for the un-compacted table)
```
cd kafka_2.12-3.5.1/
cd bin/
./kafka-topics.sh --topic protobuf-demo-topic-pure-auto --bootstrap-server kafkaBoostrapString --create
./kafka-topics.sh --topic protobuf-demo-topic-pure --bootstrap-server kafkaBoostrapString --create   
```


## Launch the job runs for compacted and un-compacted tables 


Then you can issue a job run for the non compacted table using this aws cli command. Remember to change the desired parameters.

```
aws emr-serverless start-job-run     --application-id application-identifier     --name job-run-name     --execution-role-arn arn-of-emrserverless-role --mode 'STREAMING'     --job-driver
	'{
        "sparkSubmit": {
            "entryPoint": "s3://s3bucket/jars/streaming-iceberg-ingest-1.0-SNAPSHOT.jar",
            "entryPointArguments": ["true","s3://s3bucket/warehouse","/home/hadoop/Employee.desc","s3://s3bucket/checkpoint","kafkaBootstrapString","true"],
            "sparkSubmitParameters": "--class com.aws.emr.spark.iot.SparkCustomIcebergIngestMoR --conf spark.executor.cores=16 --conf spark.executor.memory=64g  --conf spark.driver.cores=4 --conf spark.driver.memory=16g --conf spark.dynamicAllocation.minExecutors=3 --conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.dynamicAllocation.maxExecutors=5 --conf spark.sql.catalog.glue_catalog.http-client.apache.max-connections=3000 --conf spark.emr-serverless.executor.disk.type=shuffle_optimized --conf spark.emr-serverless.executor.disk=1000G --files s3://streaming-big-data-ingest/Employee.desc --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        }
    }'
{	
```
Then you can issue a job run for the auto-compacted table using this aws cli command. Remember to change the desired parameters.
```
aws emr-serverless start-job-run     --application-id application-identifier     --name job-run-name     --execution-role-arn arn-of-emrserverless-role --mode 'STREAMING'     --job-driver
	'{
        "sparkSubmit": {
            "entryPoint": "s3://s3bucket/jars/streaming-iceberg-ingest-1.0-SNAPSHOT.jar",
            "entryPointArguments": ["true","s3://s3bucket/warehouse","/home/hadoop/Employee.desc","s3://s3bucket/checkpointAuto","kafkaBootstrapString","true"],
            "sparkSubmitParameters": "--class com.aws.emr.spark.iot.SparkCustomIcebergIngestMoRAuto --conf spark.executor.cores=16 --conf spark.executor.memory=64g  --conf spark.driver.cores=4 --conf spark.driver.memory=16g --conf spark.dynamicAllocation.minExecutors=3 --conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.dynamicAllocation.maxExecutors=5 --conf spark.sql.catalog.glue_catalog.http-client.apache.max-connections=3000 --conf spark.emr-serverless.executor.disk.type=shuffle_optimized --conf spark.emr-serverless.executor.disk=1000G --files s3://streaming-big-data-ingest/Employee.desc --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        }
    }'
{	
```

## Enable auto-compaction for one of the tables

Go to the Glue table ```employeeauto``` and enable ```auto-compaction```. 

## Launch the data simulator

Then, download the jar to the instance and execute the producer.
```
aws s3 cp s3://s3bucket/jars/streaming-iceberg-ingest-1.0-SNAPSHOT.jar .
```
With the following command you can start the Protocol Buffers Producer that will write to ```protobuf-demo-topic-pure``` 20 billion events.

```java -cp streaming-iceberg-ingest-1.0-SNAPSHOT.jar com.aws.emr.proto.kafka.producer.ProtoProducer kafkaBoostrapString```

With the following command you can start the Protocol Buffers Producer that will write to ```protobuf-demo-topic-pure-auto``` 20 billion events.

```java -cp streaming-iceberg-ingest-1.0-SNAPSHOT.jar com.aws.emr.proto.kafka.producer.ProtoProducerAuto kafkaBoostrapString```

Remember that your EC2 instance need to have network access to the MSK cluster, you will need to configure the VPC, Security Groups and Subnet/s.

## Costs

Remember that this benchmark is for high throughput scenarios and therefore the config may lead to quite big bill if deployed on top of AWS, remember to stop the EMR Serverless application, the used instance for the Kafka producer and delete the Amazon MSK cluster after the test.

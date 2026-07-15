"""Shared merge-on-read "S3 buckets" IoT ingest, parameterised by table name and file format.

PySpark counterpart of ``com.aws.emr.spark.iot.S3BucketsMoRJob``. The four thin entry points
(``s3buckets_avro``, ``s3buckets_orc``, ``s3buckets_auto_avro``, ``s3buckets_auto_orc``) only differ
in the target table and the Iceberg data/delete file format, so they all delegate here. All tables
are Iceberg format-version 3 (v3) merge-on-read tables with object-storage layout enabled and higher
throughput Kafka fetch settings.
"""

from __future__ import annotations

import logging
import sys
import threading
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf

from iceberg_streaming.common import DATABASE, JobConfig

log = logging.getLogger("iceberg_streaming.iot.s3buckets_job")

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS {table}
          (employee_id bigint,
          age int,
          start_date timestamp,
          team string,
          role string,
          address string,
          name string
          )
          PARTITIONED BY (hours(start_date), team, bucket(42, employee_id))
          TBLPROPERTIES (
                    'table_type'='ICEBERG',
                    'format-version'='3',
                    'write.format.default'='{fmt}',
                    'write.delete.format.default'='{fmt}',
                    'write.delete.mode'='merge-on-read',
                    'write.update.mode'='merge-on-read',
                    'write.merge.mode'='merge-on-read',
                    'write.parquet.row-group-size-bytes' = '134217728',
                    'write.parquet.page-size-bytes' = '1048576',
                    'write.target-file-size-bytes' = '536870912',
                    'write.distribution-mode' = 'hash',
                    'write.delete.distribution-mode' = 'hash',
                    'write.update.distribution-mode' = 'hash',
                    'write.merge.distribution-mode' = 'hash',
                    'write.object-storage.enabled' = 'true',
                    'write.spark.fanout.enabled' = 'true',
                    'write.metadata.delete-after-commit.enabled' = 'false',
                    'write.metadata.previous-versions-max' = '50',
                    'history.expire.max-snapshot-age-ms' = '259200000',
                    'commit.retry.num-retries'='20',
                    'commit.retry.min-wait-ms'='250',
                    'commit.retry.max-wait-ms'='60000',
                    'write.parquet.compression-codec'='zstd',
                    'compatibility.snapshot-id-inheritance.enabled'='true' )
"""


def _seconds_to_next_hour() -> float:
    now = datetime.now()
    nxt = (now + timedelta(hours=1)).replace(minute=5, second=0, microsecond=0)
    return max((nxt - now).total_seconds(), 0.0)


def _schedule_compaction(spark, table: str) -> None:
    def compact() -> None:
        try:
            log.warning("Compaction in progress for %s", table)
            spark.sql(
                f"""
                CALL system.rewrite_data_files(
                  table => '{table}',
                  strategy => 'sort',
                  sort_order => 'start_date',
                  where => 'start_date >= current_timestamp() - INTERVAL 1 HOURS',
                  options => map(
                    'rewrite-job-order','bytes-asc',
                    'target-file-size-bytes','273741824',
                    'max-file-group-size-bytes','10737418240',
                    'partial-progress.enabled', 'true',
                    'max-concurrent-file-group-rewrites', '1000',
                    'partial-progress.max-commits', '10'))
                """
            ).show()
            spark.sql(f"CALL system.rewrite_manifests(table => '{table}')").show()
        finally:
            timer = threading.Timer(60 * 60, compact)
            timer.daemon = True
            timer.start()

    first = threading.Timer(_seconds_to_next_hour(), compact)
    first.daemon = True
    first.start()


def run(argv: list[str] | None, app_name: str, table: str, file_format: str) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session(app_name)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(_CREATE_TABLE.format(table=table, fmt=file_format))

    qualified = f"{DATABASE}.{table}"
    remove_duplicates = cfg.remove_duplicates

    def process_batch(batch_df, batch_id: int) -> None:
        session = batch_df.sparkSession
        log.warning("Writing batch %s", batch_id)
        # Skip empty micro-batches: no data to merge/insert on an idle trigger.
        if batch_df.isEmpty():
            log.warning("Batch %s is empty, skipping", batch_id)
            return
        if remove_duplicates:
            batch_df.createOrReplaceTempView("insert_data")
            # Deduplicate the micro-batch first: keep only the latest row per employee_id
            # (by start_date) so a key resent within the same batch is not inserted twice.
            session.sql(
                f"""
                MERGE INTO {qualified} AS t
                USING (
                    SELECT employee_id, age, start_date, team, role, address, name
                    FROM (
                        SELECT *, row_number() OVER (
                                   PARTITION BY employee_id ORDER BY start_date DESC) AS row_num
                        FROM insert_data
                    )
                    WHERE row_num = 1
                ) AS s
                ON `s`.`employee_id`=`t`.`employee_id` AND `t`.`start_date` > current_timestamp() - INTERVAL 1 HOURS
                AND `t`.`team`='Solutions Architects' AND `t`.`start_date`=`s`.`start_date`
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        else:
            batch_df.writeTo(qualified).append()

    df = cfg.kafka_stream(spark, "protobuf-demo-topic-pure")

    output = (
        df.select(from_protobuf(F.col("value"), "Employee", cfg.proto_descriptor).alias("Employee"))
        .select(F.col("Employee.*"))
        .select(
            F.col("id").alias("employee_id"),
            F.col("employee_age.value").alias("age"),
            F.col("start_date"),
            F.col("team.name").alias("team"),
            F.col("role"),
            F.col("address"),
            F.col("name"),
        )
    )

    query = (
        output.writeStream.queryName("streaming-protobuf-ingest")
        .format("iceberg")
        .outputMode("append")
        .foreachBatch(process_batch)
        .trigger(processingTime="1 minute")
        .option("fanout-enabled", "true")
        .option("checkpointLocation", cfg.checkpoint_location)
        .start()
    )

    if cfg.compaction:
        _schedule_compaction(spark, table)

    query.awaitTermination()

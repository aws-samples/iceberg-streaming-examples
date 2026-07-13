"""Protocol Buffers -> Iceberg v3 with a custom foreachBatch writer (MERGE dedup + compaction).

PySpark counterpart of ``com.aws.emr.spark.iot.SparkCustomIcebergIngest``. Demonstrates watermark
free MERGE INTO deduplication and periodic compaction from inside ``foreachBatch``.

Run environment and catalog are configured with :class:`iceberg_streaming.common.JobConfig`
``key=value`` arguments; see ``jobconfig.usage()``.
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf

from iceberg_streaming.common import DATABASE, JobConfig

log = logging.getLogger("iceberg_streaming.iot.spark_custom_iceberg_ingest")

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS employee
          (employee_id bigint,
          age int,
          start_date timestamp,
          team string,
          role string,
          address string,
          name string
          )
          PARTITIONED BY (bucket(8, employee_id), hours(start_date), team)
          TBLPROPERTIES (
                    'table_type'='ICEBERG',
                    'format-version'='3',
                    'write.parquet.compression-level'='7',
                    'format'='parquet',
                    'commit.retry.num-retries'='10',
                    'commit.retry.min-wait-ms'='250',
                    'commit.retry.max-wait-ms'='60000',
                    'write.parquet.compression-codec'='zstd',
                    'compatibility.snapshot-id-inheritance.enabled'='true' )
"""

_MERGE = """
    MERGE INTO bigdata.employee AS t
    USING insert_data AS s
    ON `s`.`employee_id`=`t`.`employee_id` AND `t`.`start_date` > current_timestamp() - INTERVAL 1 HOURS
    AND `t`.`team`='Solutions Architects' AND `t`.`start_date`=`s`.`start_date`
    WHEN NOT MATCHED THEN INSERT *
"""


def _make_foreach_batch(remove_duplicates: bool, compaction_enabled: bool):
    def process_batch(batch_df, batch_id: int) -> None:
        session = batch_df.sparkSession
        log.warning("Writing batch %s", batch_id)
        if remove_duplicates:
            batch_df.createOrReplaceTempView("insert_data")
            session.sql(_MERGE)
        else:
            batch_df.writeTo("bigdata.employee").append()

        if compaction_enabled:
            if batch_id % 10 == 0:
                log.warning("Compaction in progress")
                session.sql(
                    """
                    CALL system.rewrite_data_files(
                      table => 'employee',
                      strategy => 'sort',
                      sort_order => 'start_date',
                      where => 'start_date >= current_timestamp() - INTERVAL 1 HOURS',
                      options => map(
                        'rewrite-job-order','bytes-asc',
                        'target-file-size-bytes','273741824',
                        'max-file-group-size-bytes','10737418240',
                        'partial-progress.enabled', 'true',
                        'max-concurrent-file-group-rewrites', '10000',
                        'partial-progress.max-commits', '10'))
                    """
                ).show()
            if batch_id % 30 == 0:
                log.warning("Manifest compaction in progress")
                session.sql("CALL system.rewrite_manifests(table => 'employee')").show()

    return process_batch


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkIoTProtoBufDescriptor2Iceberg")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(_CREATE_TABLE)

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
        .foreachBatch(_make_foreach_batch(cfg.remove_duplicates, cfg.compaction))
        .trigger(processingTime="5 minutes")
        .option("fanout-enabled", "true")
        .option("checkpointLocation", cfg.checkpoint_location)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

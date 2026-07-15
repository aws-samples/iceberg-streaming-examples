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
from iceberg_streaming.common.observability import attach_progress_listener

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

# NOTE ON SEMANTICS: this is *bounded replay suppression*, NOT a global key upsert. The ON clause is
# scoped to the last hour, team='Solutions Architects' and an exact start_date match, and the only
# action is INSERT when NOT MATCHED. It suppresses duplicate re-arrivals of the same
# (employee_id, start_date) event within that recent window; it does not update existing rows and it
# will still insert an older replay outside the window or for another team. For a global upsert keyed
# on the business key see the CDC mirror (spark_cdc_mirror / spark_streaming_cdc_mirror) and the
# README "CDC correctness assumptions". The inner row_number() collapses duplicates of the same key
# within this one micro-batch so INSERT * cannot write them twice.
_MERGE = """
    MERGE INTO bigdata.employee AS t
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


def _make_foreach_batch(remove_duplicates: bool, compaction_enabled: bool):
    def process_batch(batch_df, batch_id: int) -> None:
        session = batch_df.sparkSession
        log.warning("Writing batch %s", batch_id)
        # Skip empty micro-batches: no data to merge/insert and no reason to compact on an idle trigger.
        if batch_df.isEmpty():
            log.warning("Batch %s is empty, skipping", batch_id)
            return
        if remove_duplicates:
            batch_df.createOrReplaceTempView("insert_data")
            session.sql(_MERGE)
        else:
            batch_df.writeTo("bigdata.employee").append()

        if compaction_enabled:
            if batch_id > 0 and batch_id % 10 == 0:
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
            if batch_id > 0 and batch_id % 30 == 0:
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
        .option("checkpointLocation", cfg.checkpoint_for("streaming-protobuf-ingest"))
        .start()
    )
    attach_progress_listener(spark)
    query.awaitTermination()


if __name__ == "__main__":
    main()

"""Avro -> Iceberg v3 merge-on-read table stored as **Parquet** (PySpark).

Counterpart of the Java ``SparkCustomIcebergIngestMoRAvroParquet``: consumes the Avro messages from
``avro-demo-topic-pure`` (produced by the Java ``AvroProducer``) and writes them to the
``employee_avro_parquet`` Iceberg v3 MERGE-ON-READ table with Parquet data/delete files and
object-storage layout.

Wire format / decoding note (the "battle-tested" bit): the Java ``AvroProducer`` uses Avro
*single-object encoding* (a 10-byte header: ``0xC3 0x01`` + 8-byte schema fingerprint, then the Avro
body). The robust production answer is a schema registry (AWS Glue Schema Registry, or Confluent via
the ABRiS library) whose deserializer understands the framing and schema evolution. In this
registry-free PySpark example we strip the fixed 10-byte header and decode the plain Avro body with
``from_avro`` — simple and dependency-free, but it hard-codes the header length, so prefer a registry
for real pipelines.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from iceberg_streaming.common import DATABASE, JobConfig

log = logging.getLogger("iceberg_streaming.avro.spark_avro_ingest_mor_parquet")

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS employee_avro_parquet
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
                    'write.format.default'='parquet',
                    'write.delete.format.default'='parquet',
                    'write.delete.mode'='merge-on-read',
                    'write.update.mode'='merge-on-read',
                    'write.merge.mode'='merge-on-read',
                    'write.parquet.compression-codec'='zstd',
                    'write.target-file-size-bytes' = '536870912',
                    'write.distribution-mode' = 'hash',
                    'write.object-storage.enabled' = 'true',
                    'write.spark.fanout.enabled' = 'true',
                    'compatibility.snapshot-id-inheritance.enabled'='true' )
"""


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    json_format_schema = Path(cfg.avro_schema_file).read_text(encoding="utf-8")
    spark = cfg.build_session("PySparkAvro2IcebergMoRParquet")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(_CREATE_TABLE)

    remove_duplicates = cfg.remove_duplicates

    def process_batch(batch_df, batch_id: int) -> None:
        session = batch_df.sparkSession
        log.warning("Writing batch %s", batch_id)
        if remove_duplicates:
            batch_df.createOrReplaceTempView("insert_data")
            session.sql(
                """
                MERGE INTO bigdata.employee_avro_parquet AS t
                USING insert_data AS s
                ON `s`.`employee_id`=`t`.`employee_id` AND `t`.`start_date` > current_timestamp() - INTERVAL 1 HOURS
                AND `t`.`team`='Solutions Architects' AND `t`.`start_date`=`s`.`start_date`
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        else:
            batch_df.writeTo("bigdata.employee_avro_parquet").append()

    df = cfg.kafka_stream(spark, "avro-demo-topic-pure")

    # Strip the 10-byte Avro single-object-encoding header before decoding the plain Avro body.
    plain_avro = F.expr("substring(value, 11, length(value) - 10)")

    output = (
        df.select(from_avro(plain_avro, json_format_schema, {"mode": "PERMISSIVE"}).alias("Employee"))
        .select(F.col("Employee.*"))
        .select(
            F.col("employee_id"),
            F.col("age"),
            F.expr("timestamp_millis(start_date)").alias("start_date"),  # producer writes epoch millis
            F.col("team"),
            F.col("role"),
            F.col("address"),
            F.col("name"),
        )
    )

    query = (
        output.writeStream.queryName("streaming-avro-parquet-ingest")
        .format("iceberg")
        .outputMode("append")
        .foreachBatch(process_batch)
        .trigger(processingTime="1 minute")
        .option("fanout-enabled", "true")
        .option("checkpointLocation", cfg.checkpoint_location)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

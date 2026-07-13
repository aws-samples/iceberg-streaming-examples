"""Protocol Buffers -> Iceberg v3 using an inline hex protobuf descriptor set.

PySpark counterpart of ``com.aws.emr.proto.SparkCustomIcebergIngestProtoHex``. Instead of shipping a
descriptor file, the protobuf ``FileDescriptorSet`` is embedded as a hex string and passed to
``from_protobuf`` via ``binaryDescriptorSet``.
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf

from iceberg_streaming.common import DATABASE, JobConfig

log = logging.getLogger("iceberg_streaming.iot.spark_custom_iceberg_ingest_proto_hex")

_HEX_DATA = (
    "0A86040A1E676F6F676C652F70726F746F6275662F77726170706572732E70726F746F120F676F6F676C652E70726F746F62756622230A0B446F75626C6556616C756512140A0576616C7565180120012801520576616C756522220A0A466C6F617456616C756512140A0576616C7565180120012802520576616C756522220A0A496E74363456616C756512140A0576616C7565180120012803520576616C756522230A0B55496E74363456616C756512140A0576616C7565180120012804520576616C756522220A0A496E74333256616C756512140A0576616C7565180120012805520576616C756522230A0B55496E74333256616C756512140A0576616C756518012001280D520576616C756522210A09426F6F6C56616C756512140A0576616C7565180120012808520576616C756522230A0B537472696E6756616C756512140A0576616C7565180120012809520576616C756522220A0A427974657356616C756512140A0576616C756518012001280C520576616C75654283010A13636F6D2E676F6F676C652E70726F746F627566420D577261707065727350726F746F50015A31676F6F676C652E676F6C616E672E6F72672F70726F746F6275662F74797065732F6B6E6F776E2F77726170706572737062F80101A20203475042AA021E476F6F676C652E50726F746F6275662E57656C6C4B6E6F776E5479706573620670726F746F330AFF010A1F676F6F676C652F70726F746F6275662F74696D657374616D702E70726F746F120F676F6F676C652E70726F746F627566223B0A0954696D657374616D7012180A077365636F6E647318012001280352077365636F6E647312140A056E616E6F7318022001280552056E616E6F734285010A13636F6D2E676F6F676C652E70726F746F627566420E54696D657374616D7050726F746F50015A32676F6F676C652E676F6C616E672E6F72672F70726F746F6275662F74797065732F6B6E6F776E2F74696D657374616D707062F80101A20203475042AA021E476F6F676C652E50726F746F6275662E57656C6C4B6E6F776E5479706573620670726F746F330AEE030A0E456D706C6F7965652E70726F746F120E6773722E70726F746F2E706F73741A1E676F6F676C652F70726F746F6275662F77726170706572732E70726F746F1A1F676F6F676C652F70726F746F6275662F74696D657374616D702E70726F746F2297020A08456D706C6F796565120E0A0269641801200128055202696412120A046E616D6518022001280952046E616D6512180A0761646472657373180320012809520761646472657373123E0A0C656D706C6F7965655F61676518042001280B321B2E676F6F676C652E70726F746F6275662E496E74333256616C7565520B656D706C6F79656541676512390A0A73746172745F6461746518052001280B321A2E676F6F676C652E70726F746F6275662E54696D657374616D70520973746172744461746512280A047465616D18062001280B32142E6773722E70726F746F2E706F73742E5465616D52047465616D12280A04726F6C6518072001280E32142E6773722E70726F746F2E706F73742E526F6C655204726F6C6522360A045465616D12120A046E616D6518012001280952046E616D65121A0A086C6F636174696F6E18022001280952086C6F636174696F6E2A310A04526F6C65120B0A074D414E414745521000120D0A09444556454C4F5045521001120D0A094152434849544543541002620670726F746F33"
)

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


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkIoTProtoBufHex2Iceberg")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(_CREATE_TABLE)

    descriptor = bytes.fromhex(_HEX_DATA)
    compaction_enabled = cfg.compaction

    def process_batch(batch_df, batch_id: int) -> None:
        session = batch_df.sparkSession
        log.warning("Writing batch %s", batch_id)
        batch_df.writeTo("bigdata.employee").append()
        if compaction_enabled and batch_id % 10 == 0:
            session.sql(
                """
                CALL system.rewrite_data_files(
                  table => 'employee', strategy => 'sort', sort_order => 'start_date',
                  where => 'start_date >= current_timestamp() - INTERVAL 1 HOURS',
                  options => map('rewrite-job-order','bytes-asc','target-file-size-bytes','273741824',
                    'max-file-group-size-bytes','10737418240','partial-progress.enabled','true',
                    'max-concurrent-file-group-rewrites','10000','partial-progress.max-commits','10'))
                """
            ).show()

    df = cfg.kafka_stream(spark, "protobuf-demo-topic-pure")

    output = (
        df.select(
            from_protobuf(F.col("value"), "Employee", binaryDescriptorSet=descriptor).alias("Employee")
        )
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
    query.awaitTermination()


if __name__ == "__main__":
    main()

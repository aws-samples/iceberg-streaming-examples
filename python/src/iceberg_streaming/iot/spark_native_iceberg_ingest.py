"""EV telemetry -> Iceberg with the **native** Spark/Iceberg streaming writer (``toTable``).

PySpark counterpart of ``com.aws.emr.spark.iot.SparkNativeIcebergIngest``: the simplest,
lowest-overhead ingest path - one streaming query, no ``foreachBatch``, appends only. One module
covers what used to be several: the payload format is selected with ``source=proto|avro|json`` and
the table layout with ``mode=``, ``fv=``, ``fileformat=`` and ``objectstorage=``. Corrupt JSON
records are dropped on this path; use ``iot-custom-ingest`` to capture them in a dead-letter table.

Deduplication (``dedup=watermark``): the native writer cannot run a MERGE, so its dedup option is
the stateful ``dropDuplicatesWithinWatermark`` on the event identity ``(vehicle_id, event_time)``.
Know the trade-off: state is bounded by the watermark delay (``watermark=``, default 120 seconds),
so only duplicates arriving within that delay are caught - and events **older than the watermark are
dropped entirely**, not deduplicated. The demo producer emits 0.1% one-hour-late readings: with the
default watermark those are silently discarded here. Widen ``watermark=`` past your late-arrival
window (more state) or use the MERGE dedup of ``iot-custom-ingest``, which keeps late data.
"""

from __future__ import annotations

import logging
import sys

from iceberg_streaming.common import DATABASE, Dedup, JobConfig, Mode
from iceberg_streaming.common.observability import attach_progress_listener
from iceberg_streaming.iot import _telemetry

log = logging.getLogger("iceberg_streaming.iot.spark_native_iceberg_ingest")


def main(argv: list[str] | None = None) -> None:
    cfg = JobConfig.from_args(argv if argv is not None else sys.argv[1:])
    spark = cfg.build_session("PySparkTelemetryNativeIngest")

    table = cfg.table(_telemetry.TABLE)
    dedup = cfg.dedup(Dedup.NONE)
    if dedup not in (Dedup.NONE, Dedup.WATERMARK):
        raise ValueError(
            "The native writer supports dedup=none or dedup=watermark; use iot-custom-ingest for"
            " dedup=batch|merge."
        )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    spark.sql(cfg.create_table_ddl(table, _telemetry.COLUMNS_DDL, _telemetry.PARTITION_DDL, Mode.COW))

    output = _telemetry.decode(cfg.kafka_stream(spark, cfg.topic()), cfg)

    if dedup is Dedup.WATERMARK:
        log.warning(
            "Watermark dedup enabled (delay=%s): duplicates within the delay are dropped, and so are"
            " events older than the watermark - late data beyond %s is DISCARDED on this path.",
            cfg.watermark_delay(),
            cfg.watermark_delay(),
        )
        output = output.withWatermark("event_time", cfg.watermark_delay()).dropDuplicatesWithinWatermark(
            ["vehicle_id", "event_time"]
        )

    # Attach the progress listener before the query starts so batch 0 is captured too.
    attach_progress_listener(spark)

    query = (
        output.writeStream.queryName(f"native-ingest-{table}")
        .format("iceberg")
        .outputMode("append")
        .trigger(**cfg.trigger_kwargs(60))
        .option("fanout-enabled", str(cfg.fanout(True)).lower())
        .option("checkpointLocation", cfg.checkpoint_for(f"native-ingest-{table}"))
        .toTable(table)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

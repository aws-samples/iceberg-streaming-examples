package com.aws.emr.spark.iot;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import com.aws.emr.common.JobConfig;
import com.aws.emr.common.StreamingProgressListener;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import telemetry.ev.VehicleTelemetryOuterClass;

/**
 * EV telemetry ingest where the protobuf decoding is done by a hand-written Spark <b>UDF</b>
 * instead of the native {@code from_protobuf} connector used by the other examples. This is the
 * approach to reach for when the payload needs custom decode logic (a proprietary envelope,
 * conditional parsing, a library the connector cannot express); for a plain protobuf message the
 * native connector is simpler and faster.
 *
 * <p>Table layout, catalog and run environment come from {@link JobConfig} {@code key=value}
 * arguments; see {@link JobConfig#usage()}.
 *
 * @author acmanjon@amazon.com
 */
public class SparkProtoUDF {

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("TelemetryProtoUDF2Iceberg");

    final String table = cfg.table(Telemetry.TABLE);

    // The struct returned by the decoding UDF (the Kafka lineage columns are added outside it).
    StructType telemetrySchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("vehicle_id", DataTypes.LongType, true),
              DataTypes.createStructField("event_time", DataTypes.TimestampType, true),
              DataTypes.createStructField("model", DataTypes.StringType, true),
              DataTypes.createStructField("speed_kmh", DataTypes.IntegerType, true),
              DataTypes.createStructField("soc_pct", DataTypes.IntegerType, true),
              DataTypes.createStructField("odometer_km", DataTypes.LongType, true),
              DataTypes.createStructField("charging", DataTypes.BooleanType, true)
            });

    spark
        .udf()
        .register(
            "decode_telemetry",
            (UDF1<byte[], Row>)
                bytes -> {
                  VehicleTelemetryOuterClass.VehicleTelemetry t =
                      VehicleTelemetryOuterClass.VehicleTelemetry.parseFrom(bytes);
                  // preserve millisecond precision (seconds + nanos), not just whole seconds
                  Timestamp eventTime =
                      new Timestamp(
                          t.getEventTime().getSeconds() * 1000L
                              + t.getEventTime().getNanos() / 1_000_000L);
                  return RowFactory.create(
                      t.getVehicleId(),
                      eventTime,
                      t.getModel(),
                      t.getSpeedKmh(),
                      t.getSocPct(),
                      t.getOdometerKm(),
                      t.getCharging());
                },
            telemetrySchema);

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    spark.sql(
        cfg.createTableDdl(
            table, Telemetry.COLUMNS_DDL, Telemetry.PARTITION_DDL, JobConfig.Mode.COW, Map.of()));

    Dataset<Row> df = cfg.kafkaStream(spark, cfg.topic());

    Dataset<Row> output =
        df.select(
                callUDF("decode_telemetry", col("value")).as("t"),
                col("partition").as("kafka_partition"),
                col("offset").as("kafka_offset"))
            .select(
                col("t.*"),
                col("kafka_partition"),
                col("kafka_offset"));

    StreamingProgressListener.attach(spark);

    StreamingQuery query =
        output
            .writeStream()
            .queryName("proto-udf-ingest-" + table)
            .format("iceberg")
            .trigger(cfg.trigger(60))
            .outputMode("append")
            .option("checkpointLocation", cfg.checkpointFor("proto-udf-ingest-" + table))
            .option("fanout-enabled", Boolean.toString(cfg.fanout(true)))
            .toTable(table);

    query.awaitTermination();
  }
}

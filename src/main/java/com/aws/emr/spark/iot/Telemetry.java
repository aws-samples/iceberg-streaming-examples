package com.aws.emr.spark.iot;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.timestamp_millis;
import static org.apache.spark.sql.protobuf.functions.from_protobuf;

import com.aws.emr.common.JobConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Shared schema and Kafka-payload decoding for the EV vehicle telemetry examples.
 *
 * <p>Every IoT job consumes the same logical record - a {@code VehicleTelemetry} reading - from
 * Kafka, in one of three payload formats selected with the {@code source=proto|avro|json} argument.
 * This class centralises the decode so the jobs only differ in <em>how they write</em>, not in how
 * they parse.
 *
 * <p>Two Kafka lineage columns ({@code kafka_partition}, {@code kafka_offset}) are carried into the
 * table on purpose: they are free to obtain, invaluable when debugging ("which offset produced this
 * row?"), and they give the dedup logic a <b>deterministic tiebreaker</b> - two non-identical rows
 * claiming the same {@code (vehicle_id, event_time)} identity are resolved by the highest offset
 * instead of arbitrarily.
 */
public final class Telemetry {

  private Telemetry() {}

  /** Default telemetry table name (override per run with {@code table=}). */
  public static final String TABLE = "vehicle_telemetry";

  /** Column list shared by every telemetry table the examples create. */
  public static final String COLUMNS_DDL =
      """
      vehicle_id bigint,
                event_time timestamp,
                model string,
                speed_kmh int,
                soc_pct int,
                odometer_km bigint,
                charging boolean,
                kafka_partition int,
                kafka_offset bigint""";

  /**
   * Partition spec: hourly event-time partitions (prunable by the MERGE ON clause and the
   * compaction window) plus a modest bucket count on the vehicle id.
   */
  public static final String PARTITION_DDL = "hours(event_time), bucket(16, vehicle_id)";

  /** Schema of the JSON payload; {@code event_time} is epoch milliseconds. */
  public static final StructType JSON_SCHEMA =
      new StructType()
          .add("vehicle_id", DataTypes.LongType)
          .add("event_time", DataTypes.LongType)
          .add("model", DataTypes.StringType)
          .add("speed_kmh", DataTypes.IntegerType)
          .add("soc_pct", DataTypes.IntegerType)
          .add("odometer_km", DataTypes.LongType)
          .add("charging", DataTypes.BooleanType);

  /**
   * Decode the raw Kafka stream into the uniform telemetry columns for the configured
   * {@code source=}. Corrupt JSON records are silently dropped here; use
   * {@link #decodeJsonWithRaw(Dataset)} when you need to capture them in a dead-letter table.
   */
  public static Dataset<Row> decode(Dataset<Row> kafka, JobConfig cfg) throws IOException {
    switch (cfg.source()) {
      case PROTO:
        return decodeProto(kafka, cfg);
      case AVRO:
        return decodeAvro(kafka, cfg);
      case JSON:
        return decodeJsonWithRaw(kafka).filter(col("vehicle_id").isNotNull()).drop("raw_value");
      default:
        throw new IllegalStateException("Unhandled source " + cfg.source());
    }
  }

  private static Dataset<Row> decodeProto(Dataset<Row> kafka, JobConfig cfg) throws IOException {
    // spark-protobuf maps google.protobuf.Timestamp to a Spark timestamp natively.
    return kafka
        .select(
            from_protobuf(col("value"), "VehicleTelemetry", cfg.protoDescriptor()).as("t"),
            col("partition").as("kafka_partition"),
            col("offset").as("kafka_offset"))
        .select(telemetryColumns(col("t.event_time")));
  }

  private static Dataset<Row> decodeAvro(Dataset<Row> kafka, JobConfig cfg) throws IOException {
    String jsonFormatSchema = new String(Files.readAllBytes(Paths.get(cfg.avroSchemaFile())));
    Map<String, String> options = new HashMap<>();
    options.put("mode", "PERMISSIVE");
    return kafka
        .select(
            from_avro(col("value"), jsonFormatSchema, options).as("t"),
            col("partition").as("kafka_partition"),
            col("offset").as("kafka_offset"))
        // the Avro payload carries event_time as epoch millis
        .select(telemetryColumns(timestamp_millis(col("t.event_time"))));
  }

  /**
   * Decode a JSON payload keeping the raw Kafka value alongside the parsed columns. A record that
   * fails to parse (malformed JSON, missing fields) surfaces with {@code vehicle_id IS NULL} and
   * the original line still available in {@code raw_value} - exactly what a dead-letter table
   * needs. See {@code SparkCustomIcebergIngest} for the dead-letter split.
   */
  public static Dataset<Row> decodeJsonWithRaw(Dataset<Row> kafka) {
    Column[] cols = telemetryColumns(timestamp_millis(col("t.event_time")));
    Column[] withRaw = new Column[cols.length + 1];
    System.arraycopy(cols, 0, withRaw, 0, cols.length);
    withRaw[cols.length] = col("raw_value");
    return kafka
        .select(
            col("value").cast("string").as("raw_value"),
            col("partition").as("kafka_partition"),
            col("offset").as("kafka_offset"))
        .withColumn("t", from_json(col("raw_value"), JSON_SCHEMA))
        .select(withRaw);
  }

  private static Column[] telemetryColumns(Column eventTime) {
    return new Column[] {
      col("t.vehicle_id"),
      eventTime.as("event_time"),
      col("t.model"),
      col("t.speed_kmh"),
      col("t.soc_pct"),
      col("t.odometer_km"),
      col("t.charging"),
      col("kafka_partition"),
      col("kafka_offset")
    };
  }
}

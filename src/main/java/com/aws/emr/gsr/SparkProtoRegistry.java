package com.aws.emr.gsr;

import static org.apache.spark.sql.functions.col;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.aws.emr.common.JobConfig;
import com.aws.emr.common.StreamingProgressListener;
import com.aws.emr.spark.iot.Telemetry;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import telemetry.ev.VehicleTelemetryOuterClass;

/**
 * Spark Structured Streaming consumer for the <b>AWS Glue Schema Registry</b> telemetry feed
 * produced by {@link TelemetryRegistryProducer}, landing the data in Iceberg.
 *
 * <p>Unlike {@code SparkNativeIcebergIngest} (native {@code from_protobuf} against a raw
 * descriptor), the Kafka value bytes here carry the GSR wire header, so they are decoded with the
 * {@link GlueSchemaRegistryKafkaDeserializer}. The deserializer is not serializable, so it is
 * created once per partition inside {@code mapPartitions} rather than shipped from the driver. The
 * Kafka partition/offset lineage columns are carried through the decode like every other telemetry
 * job.
 *
 * <p>Requires the {@code vehicle-telemetry-registry} registry with the
 * {@code VehicleTelemetry.proto} schema, AWS credentials, and the usual {@link JobConfig}
 * {@code key=value} arguments ({@code region=} selects the registry region).
 *
 * @author acmanjon@amazon.com
 */
public class SparkProtoRegistry {

  private static final Logger log = LogManager.getLogger(SparkProtoRegistry.class);

  public static void main(String[] args)
      throws IOException, TimeoutException, StreamingQueryException {

    JobConfig cfg = JobConfig.fromArgs(args);
    SparkSession spark = cfg.buildSession("SparkGlueSchemaRegistryProto2Iceberg");

    final String table = cfg.table(Telemetry.TABLE);
    final String topic = cfg.arg("topic", TelemetryRegistryProducer.DEFAULT_TOPIC);
    final String region = cfg.region();

    spark.sql("CREATE DATABASE IF NOT EXISTS " + JobConfig.DATABASE);
    spark.sql("USE " + JobConfig.DATABASE);
    spark.sql(
        cfg.createTableDdl(
            table, Telemetry.COLUMNS_DDL, Telemetry.PARTITION_DDL, JobConfig.Mode.COW, Map.of()));

    Dataset<Row> raw =
        cfg.kafkaStream(spark, topic).select(col("value"), col("partition"), col("offset"));

    Dataset<TelemetryBean> decoded =
        raw.mapPartitions(new DecodeGsrProtobuf(region, topic), Encoders.bean(TelemetryBean.class));

    // Rename the bean properties to the Iceberg column names.
    Dataset<Row> output =
        decoded.selectExpr(
            "vehicleId as vehicle_id",
            "eventTime as event_time",
            "model",
            "speedKmh as speed_kmh",
            "socPct as soc_pct",
            "odometerKm as odometer_km",
            "charging",
            "kafkaPartition as kafka_partition",
            "kafkaOffset as kafka_offset");

    StreamingProgressListener.attach(spark);

    StreamingQuery query =
        output
            .writeStream()
            .queryName("gsr-proto-ingest-" + table)
            .format("iceberg")
            .trigger(cfg.trigger(60))
            .outputMode("append")
            .option("checkpointLocation", cfg.checkpointFor("gsr-proto-ingest-" + table))
            .option("fanout-enabled", Boolean.toString(cfg.fanout(true)))
            .toTable(table);

    query.awaitTermination();
  }

  /**
   * Decodes a partition of GSR protobuf records into {@link TelemetryBean}s. The
   * {@link GlueSchemaRegistryKafkaDeserializer} is instantiated per partition because it is not
   * serializable and therefore cannot be created on the driver and shipped to the executors.
   */
  private static class DecodeGsrProtobuf implements MapPartitionsFunction<Row, TelemetryBean> {

    private final String region;
    private final String topic;

    DecodeGsrProtobuf(String region, String topic) {
      this.region = region;
      this.topic = topic;
    }

    @Override
    public Iterator<TelemetryBean> call(Iterator<Row> input) {
      Map<String, Object> config = new HashMap<>();
      config.put(AWSSchemaRegistryConstants.AWS_REGION, region);
      config.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, ProtobufMessageType.POJO.getName());

      GlueSchemaRegistryKafkaDeserializer deserializer = new GlueSchemaRegistryKafkaDeserializer();
      deserializer.configure(config, false);

      List<TelemetryBean> out = new ArrayList<>();
      while (input.hasNext()) {
        Row row = input.next();
        byte[] value = row.getAs("value");
        if (value == null) {
          continue;
        }
        Object decoded = deserializer.deserialize(topic, value);
        if (decoded instanceof VehicleTelemetryOuterClass.VehicleTelemetry t) {
          out.add(TelemetryBean.fromProto(t, row.getAs("partition"), row.getAs("offset")));
        } else {
          log.warn("Skipping record of unexpected type {}", decoded == null ? "null" : decoded.getClass());
        }
      }
      deserializer.close();
      return out.iterator();
    }
  }

  /** JavaBean used as the {@code mapPartitions} output type (encoded with {@link Encoders#bean}). */
  public static class TelemetryBean implements Serializable {
    private long vehicleId;
    private Timestamp eventTime;
    private String model;
    private int speedKmh;
    private int socPct;
    private long odometerKm;
    private boolean charging;
    private int kafkaPartition;
    private long kafkaOffset;

    public static TelemetryBean fromProto(
        VehicleTelemetryOuterClass.VehicleTelemetry t, int partition, long offset) {
      TelemetryBean b = new TelemetryBean();
      b.setVehicleId(t.getVehicleId());
      b.setEventTime(
          new Timestamp(t.getEventTime().getSeconds() * 1000L + t.getEventTime().getNanos() / 1_000_000L));
      b.setModel(t.getModel());
      b.setSpeedKmh(t.getSpeedKmh());
      b.setSocPct(t.getSocPct());
      b.setOdometerKm(t.getOdometerKm());
      b.setCharging(t.getCharging());
      b.setKafkaPartition(partition);
      b.setKafkaOffset(offset);
      return b;
    }

    public long getVehicleId() {
      return vehicleId;
    }

    public void setVehicleId(long vehicleId) {
      this.vehicleId = vehicleId;
    }

    public Timestamp getEventTime() {
      return eventTime;
    }

    public void setEventTime(Timestamp eventTime) {
      this.eventTime = eventTime;
    }

    public String getModel() {
      return model;
    }

    public void setModel(String model) {
      this.model = model;
    }

    public int getSpeedKmh() {
      return speedKmh;
    }

    public void setSpeedKmh(int speedKmh) {
      this.speedKmh = speedKmh;
    }

    public int getSocPct() {
      return socPct;
    }

    public void setSocPct(int socPct) {
      this.socPct = socPct;
    }

    public long getOdometerKm() {
      return odometerKm;
    }

    public void setOdometerKm(long odometerKm) {
      this.odometerKm = odometerKm;
    }

    public boolean isCharging() {
      return charging;
    }

    public void setCharging(boolean charging) {
      this.charging = charging;
    }

    public int getKafkaPartition() {
      return kafkaPartition;
    }

    public void setKafkaPartition(int kafkaPartition) {
      this.kafkaPartition = kafkaPartition;
    }

    public long getKafkaOffset() {
      return kafkaOffset;
    }

    public void setKafkaOffset(long kafkaOffset) {
      this.kafkaOffset = kafkaOffset;
    }
  }
}

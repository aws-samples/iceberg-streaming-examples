package com.aws.emr.gsr;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.aws.emr.common.JobConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import telemetry.ev.VehicleTelemetryOuterClass;

/**
 * Plain Java consumer for the Glue Schema Registry telemetry topic produced by
 * {@link TelemetryRegistryProducer}: the GSR deserializer resolves the schema version from the wire
 * header and hands back the protobuf POJO. Requires AWS credentials.
 *
 * <h2>Arguments (order-independent {@code key=value})</h2>
 *
 * <pre>
 *   bootstrap=&lt;host:port,...&gt;  Kafka bootstrap servers (default localhost:9092)
 *   region=&lt;aws-region&gt;        Glue Schema Registry region (default eu-west-1)
 *   topic=&lt;name&gt;               topic to read (default telemetry-proto-gsr)
 * </pre>
 *
 * @author acmanjon@amazon.com
 */
public class TelemetryRegistryConsumer {

  private static final Logger log = LogManager.getLogger(TelemetryRegistryConsumer.class);

  public static void main(String[] args) {
    JobConfig cfg = JobConfig.fromArgs(args);
    String topic = cfg.arg("topic", TelemetryRegistryProducer.DEFAULT_TOPIC);

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "telemetry-gsr-console");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        GlueSchemaRegistryKafkaDeserializer.class.getName());
    props.put(AWSSchemaRegistryConstants.AWS_REGION, cfg.region());
    props.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, ProtobufMessageType.POJO.getName());

    log.warn("TelemetryRegistryConsumer -> topic={} region={}", topic, cfg.region());
    try (KafkaConsumer<String, VehicleTelemetryOuterClass.VehicleTelemetry> consumer =
        new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      while (true) {
        ConsumerRecords<String, VehicleTelemetryOuterClass.VehicleTelemetry> records =
            consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, VehicleTelemetryOuterClass.VehicleTelemetry> record : records) {
          VehicleTelemetryOuterClass.VehicleTelemetry t = record.value();
          log.warn(
              "vehicle={} time={} model={} speed={}km/h soc={}% odo={}km charging={}",
              t.getVehicleId(), t.getEventTime().getSeconds(), t.getModel(), t.getSpeedKmh(),
              t.getSocPct(), t.getOdometerKm(), t.getCharging());
        }
      }
    } catch (Exception e) {
      log.error("Consumer failed", e);
    }
  }
}

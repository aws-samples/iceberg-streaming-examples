package com.aws.emr.kafka;

import com.aws.emr.common.JobConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import telemetry.ev.VehicleTelemetryOuterClass;

/**
 * Plain Java Kafka consumer that decodes and prints the EV telemetry topics - a debugging tool for
 * checking what the {@link TelemetryProducer} actually put on the wire, in any of the three payload
 * formats ({@code format=proto|avro|json}).
 *
 * <h2>Arguments (order-independent {@code key=value})</h2>
 *
 * <pre>
 *   bootstrap=&lt;host:port,...&gt;  Kafka bootstrap servers (default localhost:9092)
 *   format=proto|avro|json     payload format to decode (default proto)
 *   topic=&lt;name&gt;               topic to read (default telemetry-&lt;format&gt;)
 *   group=&lt;id&gt;                 consumer group id (default telemetry-console)
 * </pre>
 *
 * @author acmanjon@amazon.com
 */
public class TelemetryConsumer {

  private static final Logger log = LogManager.getLogger(TelemetryConsumer.class);

  public static void main(String[] args) {
    JobConfig cfg = JobConfig.fromArgs(args);
    String format = cfg.arg("format", cfg.source().name().toLowerCase());
    String topic = cfg.arg("topic", "telemetry-" + format);
    String group = cfg.arg("group", "telemetry-console");

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    SpecificDatumReader<telemetry.ev.avro.VehicleTelemetry> avroReader =
        new SpecificDatumReader<>(telemetry.ev.avro.VehicleTelemetry.class);
    BinaryDecoder[] decoderHolder = new BinaryDecoder[1];

    log.warn("TelemetryConsumer -> topic={} format={} group={}", topic, format, group);
    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      while (true) {
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, byte[]> record : records) {
          switch (format) {
            case "proto" -> {
              VehicleTelemetryOuterClass.VehicleTelemetry t =
                  VehicleTelemetryOuterClass.VehicleTelemetry.parseFrom(record.value());
              log.warn(
                  "vehicle={} time={} model={} speed={}km/h soc={}% odo={}km charging={}",
                  t.getVehicleId(), t.getEventTime().getSeconds(), t.getModel(), t.getSpeedKmh(),
                  t.getSocPct(), t.getOdometerKm(), t.getCharging());
            }
            case "avro" -> {
              decoderHolder[0] =
                  DecoderFactory.get().binaryDecoder(record.value(), decoderHolder[0]);
              telemetry.ev.avro.VehicleTelemetry t = avroReader.read(null, decoderHolder[0]);
              log.warn(
                  "vehicle={} time={} model={} speed={}km/h soc={}% odo={}km charging={}",
                  t.getVehicleId(), t.getEventTime(), t.getModel(), t.getSpeedKmh(),
                  t.getSocPct(), t.getOdometerKm(), t.getCharging());
            }
            default -> log.warn(new String(record.value()));
          }
        }
      }
    } catch (Exception e) {
      log.error("Consumer failed", e);
    }
  }
}

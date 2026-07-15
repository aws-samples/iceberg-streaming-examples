package com.aws.emr.gsr;

import static com.google.protobuf.util.Timestamps.fromMillis;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.aws.emr.common.JobConfig;
import java.util.Properties;
import java.util.SplittableRandom;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.glue.model.DataFormat;
import telemetry.ev.VehicleTelemetryOuterClass;

/**
 * EV telemetry producer that serializes through the <b>AWS Glue Schema Registry</b> (protobuf data
 * format), the schema-governed alternative to the raw {@code com.aws.emr.kafka.TelemetryProducer}.
 * The GSR wire format prepends a header with the registered schema version, so consumers (including
 * {@code SparkProtoRegistry}) can evolve with the schema instead of shipping descriptor files.
 *
 * <p>Before running, create a registry named {@code vehicle-telemetry-registry} and register
 * {@code VehicleTelemetry.proto} in it (see the README). Requires AWS credentials.
 *
 * <h2>Arguments (order-independent {@code key=value})</h2>
 *
 * <pre>
 *   bootstrap=&lt;host:port,...&gt;  Kafka bootstrap servers (default localhost:9092)
 *   region=&lt;aws-region&gt;        Glue Schema Registry region (default eu-west-1)
 *   topic=&lt;name&gt;               target topic (default telemetry-proto-gsr)
 *   count=&lt;n&gt;                  number of records to produce (default 1000)
 * </pre>
 *
 * @author acmanjon@amazon.com
 */
public class TelemetryRegistryProducer {

  private static final Logger log = LogManager.getLogger(TelemetryRegistryProducer.class);

  /** Registry and schema names the README instructs you to create. */
  public static final String REGISTRY_NAME = "vehicle-telemetry-registry";
  public static final String SCHEMA_NAME = "VehicleTelemetry.proto";
  public static final String DEFAULT_TOPIC = "telemetry-proto-gsr";

  private static final SplittableRandom sr = new SplittableRandom();
  private static final String[] MODELS = {"Falcon-1", "Falcon-3", "Aquila-S", "Aquila-X", "Vulcan-7"};

  public static void main(String[] args) throws InterruptedException {
    JobConfig cfg = JobConfig.fromArgs(args);
    String topic = cfg.arg("topic", DEFAULT_TOPIC);
    long count = Long.parseLong(cfg.arg("count", "1000"));

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        GlueSchemaRegistryKafkaSerializer.class.getName());
    props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.PROTOBUF.name());
    props.put(AWSSchemaRegistryConstants.AWS_REGION, cfg.region());
    props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, REGISTRY_NAME);
    props.put(AWSSchemaRegistryConstants.SCHEMA_NAME, SCHEMA_NAME);
    props.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, ProtobufMessageType.POJO.getName());

    log.warn("TelemetryRegistryProducer -> topic={} region={} count={}", topic, cfg.region(), count);
    Callback callback = new LoggingCallback();
    try (KafkaProducer<String, VehicleTelemetryOuterClass.VehicleTelemetry> producer =
        new KafkaProducer<>(props)) {
      for (long i = 0; i < count; i++) {
        producer.send(new ProducerRecord<>(topic, next()), callback);
      }
      producer.flush();
      log.warn("Done, {} records produced to {}.", count, topic);
    }
  }

  private static VehicleTelemetryOuterClass.VehicleTelemetry next() {
    long vehicleId = sr.nextLong(100_000L);
    boolean charging = sr.nextInt(10) == 0;
    return VehicleTelemetryOuterClass.VehicleTelemetry.newBuilder()
        .setVehicleId(vehicleId)
        .setEventTime(fromMillis(System.currentTimeMillis()))
        .setModel(MODELS[(int) (vehicleId % MODELS.length)])
        .setSpeedKmh(charging ? 0 : sr.nextInt(201))
        .setSocPct(sr.nextInt(101))
        .setOdometerKm(sr.nextLong(500_000L))
        .setCharging(charging)
        .build();
  }

  private static class LoggingCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
      if (e != null) {
        log.warn("Producer send failed", e);
      }
    }
  }
}

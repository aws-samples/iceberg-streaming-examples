package com.aws.emr.kafka;

import static com.google.protobuf.util.Timestamps.fromMillis;

import com.aws.emr.common.JobConfig;
import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import telemetry.ev.VehicleTelemetryOuterClass;

/**
 * High-throughput Kafka producer of EV vehicle telemetry, in any of the three payload formats the
 * Spark jobs consume ({@code format=proto|avro|json}). One producer class covers all formats so the
 * only difference between the format examples is the serializer, not a copy/pasted loop.
 *
 * <p>Designed to be <b>fast</b>: a tight loop, {@link SplittableRandom} (much faster than
 * {@code java.util.Random}; we are not doing crypto), reused Avro encoder/buffer, zstd compression
 * and large batches. Unthrottled it saturates a local broker; use {@code rate=} to pace it.
 *
 * <p>It deliberately produces the two data-quality warts the ingest examples exist to handle:
 * <b>late events</b> (0.1% of readings are stamped one hour in the past) and <b>duplicates</b>
 * (0.2% of records are re-sent verbatim, like a device retry). {@code corrupt=true} additionally
 * emits ~0.1% malformed lines on the JSON format to feed the dead-letter example.
 *
 * <p>Durability note: {@code acks=1} favours throughput and implicitly disables producer
 * idempotence, so broker-side retries can duplicate and reorder - which is exactly the at-least-once
 * behaviour the dedup examples are built for. Production feeds usually want the Kafka defaults
 * ({@code acks=all} + idempotence) instead.
 *
 * <h2>Arguments (order-independent {@code key=value})</h2>
 *
 * <pre>
 *   bootstrap=&lt;host:port,...&gt;   Kafka bootstrap servers (default localhost:9092)
 *   format=proto|avro|json      payload format (default proto)
 *   topic=&lt;name&gt;                target topic (default telemetry-&lt;format&gt;)
 *   count=&lt;n&gt;                   number of records, 0 = run until stopped (default 0)
 *   rate=&lt;msgs/sec&gt;             approximate pacing, 0 = unthrottled (default 0)
 *   vehicles=&lt;n&gt;                vehicle id key space (default 100000)
 *   late=true|false             emit 0.1% one-hour-late events (default true)
 *   duplicates=true|false       re-send 0.2% of records verbatim (default true)
 *   corrupt=true|false          emit 0.1% malformed JSON lines, json format only (default false)
 * </pre>
 *
 * @author acmanjon@amazon.com
 */
public class TelemetryProducer {

  private static final Logger log = LogManager.getLogger(TelemetryProducer.class);

  private static final SplittableRandom sr = new SplittableRandom();

  /** Low-cardinality model dimension; stable per vehicle id. */
  private static final String[] MODELS = {"Falcon-1", "Falcon-3", "Aquila-S", "Aquila-X", "Vulcan-7"};

  private static final SpecificDatumWriter<telemetry.ev.avro.VehicleTelemetry> AVRO_WRITER =
      new SpecificDatumWriter<>(telemetry.ev.avro.VehicleTelemetry.class);

  private final String bootstrapServers;
  private final String format;
  private final String topic;
  private final long count;
  private final long rate;
  private final long vehicles;
  private final boolean lateEvents;
  private final boolean duplicates;
  private final boolean corrupt;

  // Reused Avro serialization buffers (one instance, single-threaded loop).
  private final ByteArrayOutputStream avroOut = new ByteArrayOutputStream(64);
  private BinaryEncoder avroEncoder;

  public static void main(String[] args) throws InterruptedException {
    new TelemetryProducer(JobConfig.fromArgs(args)).run();
  }

  TelemetryProducer(JobConfig cfg) {
    this.bootstrapServers = cfg.bootstrapServers();
    // 'format=' is friendlier on a producer than 'source=', but accept both.
    this.format = cfg.arg("format", cfg.source().name().toLowerCase());
    this.topic = cfg.arg("topic", "telemetry-" + format);
    this.count = Long.parseLong(cfg.arg("count", "0"));
    this.rate = Long.parseLong(cfg.arg("rate", "0"));
    this.vehicles = Long.parseLong(cfg.arg("vehicles", "100000"));
    this.lateEvents = cfg.argBool("late", true);
    this.duplicates = cfg.argBool("duplicates", true);
    this.corrupt = cfg.argBool("corrupt", false);
    if (!format.equals("proto") && !format.equals("avro") && !format.equals("json")) {
      throw new IllegalArgumentException("format must be proto, avro or json, got: " + format);
    }
  }

  private Properties producerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    // --- high-throughput producer tuning ---
    // acks=1 favours throughput and turns OFF idempotence: with retries and 5 in-flight requests,
    // re-sends can duplicate and reorder. That at-least-once behaviour is intentional here - it is
    // what the dedup examples exercise. Use the Kafka defaults (acks=all, idempotence) in production.
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd"); // big win on network + storage
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "262144"); // 256 KiB batches
    props.put(ProducerConfig.LINGER_MS_CONFIG, "50"); // let batches fill before sending
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "268435456"); // 256 MiB send buffer
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760"); // 10 MiB
    return props;
  }

  void run() throws InterruptedException {
    log.warn(
        "TelemetryProducer -> topic={} format={} bootstrap={} count={} rate={} vehicles={} late={} duplicates={} corrupt={}",
        topic, format, bootstrapServers, count == 0 ? "unbounded" : count, rate == 0 ? "unthrottled" : rate,
        vehicles, lateEvents, duplicates, corrupt);

    Callback callback = new LoggingCallback();
    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerConfig())) {
      long sent = 0;
      while (count == 0 || sent < count) {
        byte[] payload = nextPayload();
        // No key on purpose: records scatter across partitions, so consumers see out-of-order and
        // duplicated arrivals - the conditions the dedup logic must handle.
        producer.send(new ProducerRecord<>(topic, payload), callback);
        if (duplicates && sr.nextInt(500) == 0) {
          producer.send(new ProducerRecord<>(topic, payload), callback); // verbatim re-send
        }
        sent++;
        if (sent % 1_000_000 == 0) {
          log.warn("{} records produced...", sent);
        }
        // Coarse pacing: sleep 100ms every rate/10 records => ~rate records/second.
        if (rate >= 10 && sent % (rate / 10) == 0) {
          TimeUnit.MILLISECONDS.sleep(100);
        }
      }
      producer.flush();
      log.warn("Done, {} records produced to {}.", sent, topic);
    }
  }

  private byte[] nextPayload() {
    long vehicleId = sr.nextLong(vehicles);
    long eventMs = System.currentTimeMillis();
    if (lateEvents && sr.nextInt(1000) == 0) {
      eventMs -= 3_600_000L; // a reading that arrives one hour late
    }
    String model = MODELS[(int) (vehicleId % MODELS.length)];
    boolean charging = sr.nextInt(10) == 0;
    int speed = charging ? 0 : sr.nextInt(201);
    int soc = sr.nextInt(101);
    long odometer = sr.nextLong(500_000L);

    switch (format) {
      case "proto":
        return VehicleTelemetryOuterClass.VehicleTelemetry.newBuilder()
            .setVehicleId(vehicleId)
            .setEventTime(fromMillis(eventMs))
            .setModel(model)
            .setSpeedKmh(speed)
            .setSocPct(soc)
            .setOdometerKm(odometer)
            .setCharging(charging)
            .build()
            .toByteArray();
      case "avro":
        return serializeAvro(vehicleId, eventMs, model, speed, soc, odometer, charging);
      case "json":
      default:
        if (corrupt && sr.nextInt(1000) == 0) {
          // truncated line -> lands in the dead-letter table of the JSON ingest example
          return ("{\"vehicle_id\":" + vehicleId + ",\"event_time\":").getBytes();
        }
        // hand-built JSON: no serializer allocation churn in the hot loop
        return new StringBuilder(160)
            .append("{\"vehicle_id\":").append(vehicleId)
            .append(",\"event_time\":").append(eventMs)
            .append(",\"model\":\"").append(model)
            .append("\",\"speed_kmh\":").append(speed)
            .append(",\"soc_pct\":").append(soc)
            .append(",\"odometer_km\":").append(odometer)
            .append(",\"charging\":").append(charging)
            .append('}')
            .toString()
            .getBytes();
    }
  }

  /**
   * Plain Avro binary (no single-object header), symmetric with what Spark's {@code from_avro}
   * expects. The output stream and encoder are reused across records.
   */
  private byte[] serializeAvro(
      long vehicleId, long eventMs, String model, int speed, int soc, long odometer, boolean charging) {
    telemetry.ev.avro.VehicleTelemetry record =
        telemetry.ev.avro.VehicleTelemetry.newBuilder()
            .setVehicleId(vehicleId)
            .setEventTime(eventMs)
            .setModel(model)
            .setSpeedKmh(speed)
            .setSocPct(soc)
            .setOdometerKm(odometer)
            .setCharging(charging)
            .build();
    try {
      avroOut.reset();
      avroEncoder = EncoderFactory.get().binaryEncoder(avroOut, avroEncoder);
      AVRO_WRITER.write(record, avroEncoder);
      avroEncoder.flush();
      return avroOut.toByteArray();
    } catch (java.io.IOException e) {
      throw new RuntimeException("Avro serialization failed", e);
    }
  }

  private static class LoggingCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
      if (e != null) {
        log.warn("Producer send failed", e);
      } else if (log.isDebugEnabled()) {
        log.debug(
            "topic={} partition={} offset={} timestamp={}",
            metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
      }
    }
  }
}

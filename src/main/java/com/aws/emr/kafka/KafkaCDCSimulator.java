package com.aws.emr.kafka;

import com.aws.emr.common.JobConfig;
import java.util.BitSet;
import java.util.Properties;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Kafka producer simulating a DMS-like CDC feed of bank-account changes, consumed by the CDC
 * examples ({@code SparkLogChange}, {@code SparkStreamingCDCMirror}, ...).
 *
 * <p>Wire format (CSV): {@code operation,account_id,balance,last_updated(epoch millis),seq}.
 * {@code operation} is {@code I} (first change seen for a key), {@code U} or {@code D}. The
 * trailing {@code seq} is a <b>monotonic source sequence</b> (an LSN surrogate, {@link AtomicLong})
 * that gives the downstream MERGE a deterministic total order across keys, partitions and retries -
 * see {@code com.aws.emr.spark.cdc.CdcSql}. {@code balance} is in minor units (cents), stored as
 * {@code bigint} end to end; money never touches a float.
 *
 * <p>Records are sent <b>without a Kafka key by default</b>, so changes for one account scatter
 * across partitions and arrive genuinely out of order - the condition the {@code seq}-guarded MERGE
 * exists for. Pass {@code keyed=true} to key by account id (per-key ordering preserved) when you
 * want to isolate other effects.
 *
 * <p>Workload shape: 80% of the changes hit a small "hot" key set, so the same data files are
 * rewritten again and again (row-level delete churn - the deletion-vector workload); the other 20%
 * spread across a large long tail that keeps growing the table. ~85% updates / ~15% deletes after a
 * key's first change.
 *
 * <h2>Arguments (order-independent {@code key=value})</h2>
 *
 * <pre>
 *   bootstrap=&lt;host:port,...&gt;  Kafka bootstrap servers (default localhost:9092)
 *   topic=&lt;name&gt;               target topic (default streaming-cdc-log-ingest)
 *   count=&lt;n&gt;                  number of records, 0 = run until stopped (default 0)
 *   rate=&lt;msgs/sec&gt;            approximate pacing, 0 = unthrottled (default 20000)
 *   hot=&lt;n&gt;                    hot key space (default 100000)
 *   accounts=&lt;n&gt;               total key space including the long tail (default 2000000)
 *   keyed=true|false           key records by account_id (default false = out-of-order arrivals)
 * </pre>
 *
 * @author acmanjon@amazon.com
 */
public class KafkaCDCSimulator {

  private static final Logger log = LogManager.getLogger(KafkaCDCSimulator.class);

  private static final SplittableRandom sr = new SplittableRandom();

  /** Monotonic source sequence (LSN surrogate) stamped on every record. */
  private static final AtomicLong SEQ = new AtomicLong();

  private final String bootstrapServers;
  private final String topic;
  private final long count;
  private final long rate;
  private final int hotKeys;
  private final int totalKeys;
  private final boolean keyed;

  /** Tracks which account ids have been emitted before, so their first change is an I. */
  private final BitSet seen;

  public static void main(String[] args) throws InterruptedException {
    new KafkaCDCSimulator(JobConfig.fromArgs(args)).run();
  }

  KafkaCDCSimulator(JobConfig cfg) {
    this.bootstrapServers = cfg.bootstrapServers();
    this.topic = cfg.arg("topic", "streaming-cdc-log-ingest");
    this.count = Long.parseLong(cfg.arg("count", "0"));
    this.rate = Long.parseLong(cfg.arg("rate", "20000"));
    this.hotKeys = Integer.parseInt(cfg.arg("hot", "100000"));
    this.totalKeys = Integer.parseInt(cfg.arg("accounts", "2000000"));
    this.keyed = cfg.argBool("keyed", false);
    this.seen = new BitSet(totalKeys);
  }

  private Properties producerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    // --- high-throughput producer tuning ---
    // acks=1 disables idempotence: retries may duplicate records (same seq re-delivered). That is
    // fine here - the changelog dedup and the seq-guarded MERGE absorb exactly that.
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "262144");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "50");
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "268435456");
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760");
    return props;
  }

  /** Build one CSV change record: {@code operation,account_id,balance,last_updated,seq}. */
  String nextRecord() {
    // 80% of the changes hit the small hot key set, 20% the long tail.
    int id = (sr.nextInt(100) < 80) ? sr.nextInt(hotKeys) : sr.nextInt(totalKeys);
    String operation;
    if (!seen.get(id)) {
      seen.set(id);
      operation = "I";
    } else {
      // ~85% updates, ~15% deletes. A delete removes the row; a later update on the same key
      // re-inserts it via the MERGE -> more delete/insert churn on the hot files.
      operation = (sr.nextInt(100) < 15) ? "D" : "U";
    }
    long balanceCents = sr.nextLong(1_000L, 100_000_000L); // minor units, bigint end to end
    return operation + "," + id + "," + balanceCents + "," + System.currentTimeMillis() + ","
        + SEQ.getAndIncrement();
  }

  void run() throws InterruptedException {
    log.warn(
        "KafkaCDCSimulator -> topic={} bootstrap={} count={} rate={} hot={} accounts={} keyed={}",
        topic, bootstrapServers, count == 0 ? "unbounded" : count, rate == 0 ? "unthrottled" : rate,
        hotKeys, totalKeys, keyed);

    Callback callback = new LoggingCallback();
    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerConfig())) {
      long sent = 0;
      while (count == 0 || sent < count) {
        String cdc = nextRecord();
        String key = keyed ? cdc.split(",")[1] : null;
        producer.send(new ProducerRecord<>(topic, key, cdc.getBytes()), callback);
        sent++;
        if (sent % 1_000_000 == 0) {
          log.warn("{} records produced...", sent);
        }
        if (rate >= 10 && sent % (rate / 10) == 0) {
          TimeUnit.MILLISECONDS.sleep(100);
        }
      }
      producer.flush();
      log.warn("Done, {} records produced to {}.", sent, topic);
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

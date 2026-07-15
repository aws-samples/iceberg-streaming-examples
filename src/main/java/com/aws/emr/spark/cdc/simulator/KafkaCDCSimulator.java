package com.aws.emr.spark.cdc.simulator;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import gsr.avro.post.Employee;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;

/**
 *
 * A Kafka Java Producer implemented in Java producing DMS messages.
 * It uses a SplittableRandom as it is a lot faster than the default implementation, and we are not using it for
 * cryptographic functions
 *
 * @author acmanjon @amazon.com
 */

public class KafkaCDCSimulator{

    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(KafkaCDCSimulator.class);

    private static final SplittableRandom sr = new SplittableRandom();

    /**
     * Monotonically increasing source sequence stamped on every record. It stands in for a database
     * change LSN and gives the downstream MERGE a deterministic total order across keys, partitions
     * and retries (see {@link com.aws.emr.spark.cdc.CdcSql}). AtomicLong so it stays correct if the
     * producer is ever made multi-threaded.
     */
    private static final java.util.concurrent.atomic.AtomicLong SEQ = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Hot key space: a small set of accounts that get updated/deleted over and over, so the same
     * data files are rewritten again and again (heavy row-level delete churn on the same files).
     */
    private static final int HOT_KEYS = 100_000;

    /**
     * Full key space including the long tail. New tail keys keep growing the table and its data-file
     * count, so deletes are scattered across many files rather than the whole table being rewritten.
     */
    private static final int TOTAL_KEYS = 2_000_000;

    /**
     * The constant bootstrapServers.
     */
    protected static String bootstrapServers="localhost:9092"; // by default localhost

    /**
     * Main entry point.
     *
     * @param args the kafkaBootstrapString -- optional defaults to localhost:9092
     * @throws InterruptedException the interrupted exception
     */
    public static void main(String args[]) throws InterruptedException {
        if(args.length == 1) {
            bootstrapServers=args[0];
        }
        log.warn("Kafka bootstrap servers are set to "+bootstrapServers);
        KafkaCDCSimulator producer = new KafkaCDCSimulator();
        producer.startProducer();
    }

    private Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // --- high-throughput producer tuning ---
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "262144");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "50");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "268435456");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760");
        return props;
    }

    /**
     * Create employee record employee outer class . employee.
     *
     * @return the employee outer class . employee
     */

    public String createCDCRecord() {

        // 80% of the changes hit the small "hot" key set (the same accounts -> the same data files
        // are rewritten again and again, so row-level delete files accumulate on them), the other
        // 20% spread across the large long-tail space (keeps growing the table and its data-file
        // count). This scatter + repetition is what makes v2 positional delete files pile up on the
        // hot data files while v3 keeps a single merged deletion vector per file.
        int id = (sr.nextInt(100) < 80) ? sr.nextInt(HOT_KEYS) : sr.nextInt(TOTAL_KEYS);
        // ~85% updates, ~15% deletes. A delete removes the row; a later update on the same key
        // re-inserts it -> even more delete/insert churn on the hot files.
        String operation = (sr.nextInt(100) < 15) ? "D" : "U";
        int balance = sr.nextInt(1000, 10000);
        Instant instant = Instant.now();
        // DMS-like CSV: operation,account_id,balance,last_updated(epoch millis),seq. The trailing seq
        // is the source sequence used downstream to order changes deterministically and to guard
        // against stale updates/deletes overwriting newer state.
        return operation + "," + id + "," + balance + "," + instant.toEpochMilli() + "," + SEQ.getAndIncrement();
    }

    /**
     * Start producer.
     *
     * @throws InterruptedException the interrupted exception
     */
    public void startProducer() throws InterruptedException {
        String topic = "streaming-cdc-log-ingest";

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(getProducerConfig())){
            log.warn("Starting to send records...");
            int count = 1;
            int throttle = 0;
            while (true) {
                if (count % 100000000 == 0) {
                    log.warn("100 million messages produced... ");
                }
                String cdc = createCDCRecord();
                var array= cdc.getBytes();
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, array);
                producer.send(record, new KafkaCDCSimulator.ProducerCallback());
                count++;
                throttle++;
                // if you want to really push just un-comment this block

         if (throttle % 70000 == 0) {
        TimeUnit.MILLISECONDS.sleep(400); //about 20.000 msg/seg
        }
            }
        }
    }

    private class ProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetaData, Exception e) {
            if (e == null) {
                log.debug("Received new metadata. \n"
                        + "Topic:" + recordMetaData.topic() + "\n"
                        + "Partition: " + recordMetaData.partition() + "\n"
                        + "Offset: " + recordMetaData.offset() + "\n"
                        + "Timestamp: " + recordMetaData.timestamp());
            } else {
                log.warn("There's been an error from the Producer side");
                e.printStackTrace();
            }
        }
    }

}

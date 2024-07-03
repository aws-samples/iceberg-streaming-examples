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
        return props;
    }

    /**
     * Create employee record employee outer class . employee.
     *
     * @return the employee outer class . employee
     */

    public String createCDCRecord() {

        // simulate
        int id=sr.nextInt(10000);
        int balance=sr.nextInt(1000,10000);
        Instant instant = Instant.now();
        return "U,"+Integer.toString(id)+","+Integer.toString(balance)+","+Long.toString(instant.toEpochMilli());
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

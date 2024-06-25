package com.aws.emr.json.kafka.producer;

import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
import gsr.proto.post.EmployeeOuterClass;
import static java.lang.System.currentTimeMillis;
import static com.google.protobuf.util.Timestamps.fromMillis;

import java.util.Properties;
import java.util.SplittableRandom;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * A  Kafka Java Producer implemented in Java producing Proto messages.
 * It uses a SplittableRandom as it is a lot faster than the default implementation, and we are not using it for
 * cryptographic functions
 *
 * @author acmanjon@amazon.com
 */

public class ProtoProducer {

    protected static final Logger logger = LoggerFactory.getLogger(ProtoProducer.class);
    private static final SplittableRandom sr = new SplittableRandom();
    protected static String bootstrapServers="localhost:9094";




    private Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
           return props;
    }

    public EmployeeOuterClass.Employee createEmployeeRecord() {
        Timestamp ts = fromMillis(currentTimeMillis());
        EmployeeOuterClass.Employee employee
                = EmployeeOuterClass.Employee.newBuilder()
                .setId((sr.nextInt(100000)))
                .setName("Dummy"+sr.nextInt(100))
                .setAddress("Melbourne, Australia")
                .setEmployeeAge(Int32Value.newBuilder().setValue(sr.nextInt(99)).build())
                .setStartDate((ts))
                .setRole(EmployeeOuterClass.Role.ARCHITECT)
                .setTeam(EmployeeOuterClass.Team.newBuilder()
                        .setName("Solutions Architects")
                        .setLocation("Australia").build()).build();

        return employee;
    }

    public void startProducer() throws InterruptedException {
        String topic = "json-demo-topic-pure";
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(getProducerConfig());
        logger.warn("Starting to send records...");
        while (true) {

            EmployeeOuterClass.Employee person = createEmployeeRecord();
            // for kafka key specification, not used in this example
            // String key = "key-" + employeeId;
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, person.toByteArray());
            producer.send(record, new ProducerCallback());
         //   TimeUnit.NANOSECONDS.sleep(2);
        }
    }

    private class ProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetaData, Exception e) {
            if (e == null) {
                logger.info("Received new metadata. \n"
                        + "Topic:" + recordMetaData.topic() + "\n"
                        + "Partition: " + recordMetaData.partition() + "\n"
                        + "Offset: " + recordMetaData.offset() + "\n"
                        + "Timestamp: " + recordMetaData.timestamp());
            } else {
                logger.warn("There's been an error from the Producer side");
                e.printStackTrace();
            }
        }
    }

    public static void main(String args[]) throws InterruptedException {
        if(args.length == 1) {
            bootstrapServers=args[0];
        }
        logger.warn("Kafka bootstrap servers are set to "+bootstrapServers);
        ProtoProducer producer = new ProtoProducer();
        producer.startProducer();
    }

}

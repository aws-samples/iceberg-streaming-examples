package com.aws.emr.proto.kafka.producer;

import java.util.Properties;
import java.util.SplittableRandom;

import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
import gsr.proto.post.EmployeeOuterClass;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static java.lang.System.currentTimeMillis;

/**
 *
 * A Kafka Java Producer implemented in Java producing Proto messages.
 * It uses a SplittableRandom as it is a lot faster than the default implementation, and we are not using it for
 * cryptographic functions
 *
 * @author acmanjon@amazon.com
 */
public class ProtoProducer {

    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(ProtoProducer.class);

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
        ProtoProducer producer = new ProtoProducer();
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

    /**
     * Start producer.
     *
     * @throws InterruptedException the interrupted exception
     */
public void startProducer() throws InterruptedException {
        String topic = "protobuf-demo-topic-pure";

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(getProducerConfig())){
      log.warn("Starting to send records...");
      int count = 1;
      int throttle = 0;
      while (true) {
        if (count % 100000000 == 0) {
          log.warn("100 million messages produced... ");
        }
        EmployeeOuterClass.Employee person = createEmployeeRecord();
        // for kafka key specification, not used in this example
        // String key = "key-" + employeeId;
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, person.toByteArray());
        producer.send(record, new ProducerCallback());
        count++;
        throttle++;
        // if you want to really push just un-comment this block

        /* if (throttle % 70000 == 0) {
        TimeUnit.MILLISECONDS.sleep(400); //about 20.000 msg/seg
        }*/
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

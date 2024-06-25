package com.aws.emr.proto.kakfa.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import com.google.protobuf.InvalidProtocolBufferException;
import gsr.proto.post.EmployeeOuterClass;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;


public class ProtoConsumer {

    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(ProtoConsumer.class);

    private static String master = "";

    private String bootstrapServers="localhost:9094";

    public static void main(String args[]) throws InvalidProtocolBufferException {
        ProtoConsumer consumer = new ProtoConsumer();
        consumer.startConsumer();
    }

    private Properties getConsumerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "protobuf-pure");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
    }

    public void startConsumer() {
        log.info("starting consumer...");
        String topic = "protobuf-demo-topic-pure";
        try(KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(getConsumerConfig())){
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            final ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
            for (final ConsumerRecord<String, byte[]> record : records) {
                final EmployeeOuterClass.Employee employee = EmployeeOuterClass.Employee.parseFrom(record.value());

                log.warn("Employee Id: " + employee.getId() + " | Name: " + employee.getName() + " | Address: " + employee.getAddress() +
                        " | Age: " + employee.getEmployeeAge().getValue() + " | Startdate: " + employee.getStartDate().getSeconds() +
                        " | Team: " + employee.getTeam().getName() + " | Role: " + employee.getRole().name());
            }
        }}catch (Exception e) {
            e.printStackTrace();
        }
    }


}

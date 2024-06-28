package com.aws.emr.avro.kafka.consumer;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import gsr.avro.post.Employee;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;


/**
 * A Kafka consumer implemented in Java  using the Glue Schema Registry consuming Avro
 *
 * @author acmanjon@amazon.com
 */

public class AvroConsumerSchemaRegistry {

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(AvroConsumerSchemaRegistry.class);

    private String bootstrapServers="localhost:9092";

    public static void main(String args[]){
        AvroConsumerSchemaRegistry consumer = new AvroConsumerSchemaRegistry();
        consumer.startConsumer();
    }

    private Properties getConsumerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class.getName());
        props.put(AWSSchemaRegistryConstants.AWS_REGION,"us-east-1");
        return props;
    }

    public void startConsumer() {
        logger.info("starting consumer...");
        String topic = "avro-demo-topic";
            try (KafkaConsumer<String, Employee> consumer  = new KafkaConsumer<>(getConsumerConfig())){
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            final ConsumerRecords<String, Employee> records = consumer.poll(Duration.ofMillis(1000));
            for (final ConsumerRecord<String, Employee> record : records) {
                final Employee employee = record.value();
                logger.warn("Employee Id: " + employee.getEmployeeId() + " | Name: " + employee.getName() + " | Address: " + employee.getAddress() +
                        " | Age: " + employee.getAge() + " | Startdate: " + employee.getStartDate());
            }
        }
    }catch (Exception e) {
            e.printStackTrace();
        }
    }


}

package com.aws.emr.json.kafka.producer;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.aws.emr.json.kafka.Employee;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import software.amazon.awssdk.services.glue.model.DataFormat;

/**
 *
 * A Kafka Java Producer implemented in Java producing Json messages using Glue Schema Registry
 * It uses a SplittableRandom as it is a lot faster than the default implementation, and we are not using it for
 * cryptographic functions
 *
 * @author acmanjon@amazon.com
 */

public class JsonProducerSchemaRegistry {

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(JsonProducerSchemaRegistry.class);

    private String bootstrapServers="localhost:9092";

    private Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.JSON.name());
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "eu-west-1");
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "employee-schema-registry");
        props.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "Employee.json");
        return props;
    }

    public Employee createEmployeeRecord(int employeeId) {
        Employee employee
                = new Employee();
        employee.setEmployeeId(employeeId);
        employee.setName("Dummy");
        return employee;
    }

    public void startProducer() throws InterruptedException {
        String topic = "json-demo-topic";
        KafkaProducer<String, Employee> producer = new KafkaProducer<>(getProducerConfig());
        logger.warn("Starting to send records...");
        int employeeId = 0;
        while (employeeId < 1000) {
            Employee person = createEmployeeRecord(employeeId);
            String key = "key-" + employeeId;
            ProducerRecord<String, Employee> record = new ProducerRecord<>(topic, key, person);
            producer.send(record, new ProducerCallback());
            employeeId++;
        }
    }

    private class ProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetaData, Exception e) {
            if (e == null) {
                logger.warn("Received new metadata. \n"
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
        JsonProducerSchemaRegistry producer = new JsonProducerSchemaRegistry();
        producer.startProducer();
    }

}

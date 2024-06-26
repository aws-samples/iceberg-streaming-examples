package com.aws.emr.json.kafka.producer;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
import gsr.proto.post.EmployeeOuterClass;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.DataFormat;

/**
 *
 * A Kafka Java Producer implemented in Java producing Proto messages using Glue Schema Registry
 * It uses a SplittableRandom as it is a lot faster than the default implementation, and we are not using it for
 * cryptographic functions
 *
 * @author acmanjon@amazon.com
 */

public class ProtoProducerSchemaRegistry {
    
protected final Logger logger = LoggerFactory.getLogger(getClass());

    private String bootstrapServers="localhost:9092";

    private Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.JSON.name());
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "employee-schema-registry");
        props.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "Employee-json");
        props.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, ProtobufMessageType.POJO.getName());
        return props;
    }

    public EmployeeOuterClass.Employee createEmployeeRecord(int employeeId) {
        EmployeeOuterClass.Employee employee
                = EmployeeOuterClass.Employee.newBuilder()
                        .setId(employeeId)
                        .setName("Dummy")
                        .setAddress("Melbourne, Australia")
                        .setEmployeeAge(Int32Value.newBuilder().setValue(32).build())
                        .setStartDate(Timestamp.newBuilder().setSeconds(235234532434L).build()).build();
        return employee;
    }

    public void startProducer() throws InterruptedException {
        String topic = "json-demo-topic";
        KafkaProducer<String, EmployeeOuterClass.Employee> producer = new KafkaProducer<>(getProducerConfig());
        logger.warn("Starting to send records...");
        int employeeId = 0;
        while (employeeId < 1000) {
            EmployeeOuterClass.Employee person = createEmployeeRecord(employeeId);
            String key = "key-" + employeeId;
            ProducerRecord<String, EmployeeOuterClass.Employee> record = new ProducerRecord<>(topic, key, person);
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
        ProtoProducerSchemaRegistry producer = new ProtoProducerSchemaRegistry();
        producer.startProducer();
    }

}

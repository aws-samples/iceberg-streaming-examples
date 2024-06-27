package com.aws.emr.avro.kafka.producer;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import gsr.avro.post.Employee;

import java.time.Instant;
import java.util.Properties;
import java.util.SplittableRandom;
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
 * A Kafka Java Producer implemented in Java producing Avro messages using Glue Schema Registry
 * It uses a SplittableRandom as it is a lot faster than the default implementation, and we are not using it for
 * cryptographic functions
 *
 * @author acmanjon @amazon.com
 */

public class AvroProducerSchemaRegistry {


    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(AvroProducerSchemaRegistry.class);

    private static String bootstrapServers =  "localhost:9092"; // by default localhost

    private static final SplittableRandom sr = new SplittableRandom();

    private Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "eu-west-1");
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "employee-schema-registry");
        props.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "Employee.avsc");
        props.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, ProtobufMessageType.POJO.getName());
        return props;
    }

    /**
     * Create employee record employee outer class . employee.
     *
     * @param employeeId the employee id
     * @return the employee outer class . employee
     */
    public Employee createEmployeeRecord(int employeeId) {
        Instant instant = Instant.now();
        Employee emp=Employee.newBuilder()
                .setEmployeeId(employeeId)
                .setName("Dummy"+sr.nextInt(100))
                .setAddress("Melbourne, Australia")
                .setAge(sr.nextInt(99))
                .setStartDate(instant.toEpochMilli())
                .setRole("ARCHITECT")
                .setTeam("Solutions Architects")
                .build();
        return emp;
    }

    /**
     * Start producer.
     *
     * @throws InterruptedException the interrupted exception
     */
public void startProducer() throws InterruptedException {
        String topic = "avro-demo-topic";
        try(KafkaProducer<String, Employee> producer = new KafkaProducer<>(getProducerConfig())){
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

    /**
     * Main entry point
     *
     * @param args the kafkaBootstrapString -- optional defaults to localhost:9092
     * @throws InterruptedException the interrupted exception
     */
public static void main(String args[]) throws InterruptedException {
        if(args.length == 1) {
            bootstrapServers=args[0];
        }
        AvroProducerSchemaRegistry producer = new AvroProducerSchemaRegistry();
        producer.startProducer();
    }

}

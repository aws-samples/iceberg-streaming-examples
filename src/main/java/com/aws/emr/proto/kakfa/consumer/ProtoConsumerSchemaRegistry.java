package com.aws.emr.proto.kakfa.consumer;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import gsr.proto.post.EmployeeOuterClass;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;

/**
 *
 *
 * A Kafka consumer implemented in Java  using the Glue Schema Registry consuming Protocol Buffers
 *
 * @author acmanjon@amazon.com
 */
public class ProtoConsumerSchemaRegistry {

    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(ProtoConsumerSchemaRegistry.class);

    private static String bootstrapServers="localhost:9092";

    /**
     *
     * The entry point of application.
     *
     * @param args the kafkaBootstrapString -- optional defaults to localhost:9092
     */

    public static void main(String args[]){
        if(args.length == 1) {
            bootstrapServers=args[0];
        }
        ProtoConsumerSchemaRegistry consumer = new ProtoConsumerSchemaRegistry();
        consumer.startConsumer();
    }

    private Properties getConsumerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "protobuf");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class.getName());
        props.put(AWSSchemaRegistryConstants.AWS_REGION,"us-east-1");
        props.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, ProtobufMessageType.POJO.getName());
        return props;
    }

  /** Start consumer. */
  public void startConsumer() {
        log.info("starting consumer...");
        String topic = "protobuf-demo-topic";
            try (KafkaConsumer<String, EmployeeOuterClass.Employee> consumer  = new KafkaConsumer<>(getConsumerConfig())){
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            final ConsumerRecords<String, EmployeeOuterClass.Employee> records = consumer.poll(Duration.ofMillis(1000));
            for (final ConsumerRecord<String, EmployeeOuterClass.Employee> record : records) {
                final EmployeeOuterClass.Employee employee = record.value();
                log.warn("Employee Id: " + employee.getId() + " | Name: " + employee.getName() + " | Address: " + employee.getAddress() +
                        " | Age: " + employee.getEmployeeAge().getValue() + " | Startdate: " + employee.getStartDate().getSeconds());
            }
        }
    }catch (Exception e) {
            e.printStackTrace();
        }
    }


}

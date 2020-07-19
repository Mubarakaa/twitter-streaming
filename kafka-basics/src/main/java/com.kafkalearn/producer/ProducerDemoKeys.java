package com.kafkalearn.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "Hello world" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            // key goes to the same partition; unless partitions are increased
            logger.info("Key: {}", key);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes everytime a record is sent sucessfully/exception
                    if (e == null) {
                        logger.info("Received new metadata:  \n"
                                        + "Topic: {} \n"
                                        + "Partition: {} \n"
                                        + "Offset: {} \n"
                                        + "Timestamp {}",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        logger.error("Error", e);
                    }
                }
            }).get();  // get - to make it synchronous - bad practise

        }


        producer.flush();
        producer.close();
    }
}

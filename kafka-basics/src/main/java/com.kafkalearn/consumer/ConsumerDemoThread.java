package com.kafkalearn.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        new ConsumerDemoThread().run();

    }

    public void run() {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "first_group";
        String topic = "first_topic";

        logger.info("creating the consumer thread");
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerThread myConsumerThread = new ConsumerThread(latch, bootstrapServers, groupId, topic);

        Thread thread = new Thread(myConsumerThread);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            myConsumerThread.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted");
        } finally {
            logger.info("Application is closing");
        }
    }

    public static class ConsumerThread implements Runnable {

        private final CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private String topic;

        ConsumerThread(CountDownLatch latch, String bootstrapServers, String groupId, String topic) {
            this.latch = latch;
            this.topic = topic;

            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest, latest, none
            consumer = new KafkaConsumer<>(properties);
        }

        @Override
        public void run() {
            consumer.subscribe(Collections.singletonList(topic));
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: {}, Value: {}", record.key(), record.value());
                        logger.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            // wakeup() method is a special method to interrupt consumer.poll
            // it will throw WakeUpException
            consumer.wakeup();
        }
    }
}

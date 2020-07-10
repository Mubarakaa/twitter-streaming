package com.mub.kafkalearn.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private String consumerKey = "consumerKey";
    private String consumerSecret = "consumerSecret";
    private String token = "token";
    private String secret = "secret";

    private List<String> terms = Lists.newArrayList("bitcoin", "kafka");


    public TwitterProducer() {

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        logger.info("Setup");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // producer
        KafkaProducer<String, String> producer = createKakfaProducer();

        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Adding a shutdown hook");
            client.stop();
            producer.close();
            logger.info("Shutdown completed");
        }));


        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, e) -> {
                    if(e != null) {
                        logger.error("Something bad happened!", e);
                    }
                });
            }
        }
        logger.info("End of application");
    }

    private KafkaProducer<String, String> createKakfaProducer() {
        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }


    public Client createTwitterClient(BlockingQueue<String> messageQueue) {


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(messageQueue));                    // optional: use this if you want to process client events

        // Attempts to establish a connection.
        return builder.build();
    }
}
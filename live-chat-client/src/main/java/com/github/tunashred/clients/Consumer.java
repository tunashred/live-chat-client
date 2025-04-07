package com.github.tunashred.clients;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.Channel;
import com.github.tunashred.dtos.UserMessage;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public class Consumer {
    private static final Logger logger = LogManager.getLogger(Consumer.class);

    KafkaConsumer<String, String> consumer;

    public Consumer(String channelName, String username, Properties properties) throws IOException {
        logger.info("Initializing consumer");
        Properties consumerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/consumer.properties")) {
            consumerProps.load(propsFile);
            consumerProps.putAll(properties);
            consumerProps.put(GROUP_ID_CONFIG, "consumer-" + username); // maybe concatenate channel name too?

            this.consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(channelName));
            logger.info("Consumer initialized and subscribed to group topic '" + channelName + "'");

            // warmup poll
            consumer.poll(Duration.ofMillis(0));
        }
    }

    public Consumer(String channelName, String username) throws IOException {
        this(channelName, username, new Properties());
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Consumer myConsumer = new Consumer("baia-mare", "gulie");
        List<UserMessage> userMessageList = myConsumer.consume();

        for (UserMessage userMessage : userMessageList) {
            System.out.println(userMessage.getUsername() + ": " + userMessage.getMessage());
        }
        myConsumer.consumer.close();
    }

    public List<UserMessage> consume() throws RuntimeException {
        List<UserMessage> userMessageList = new ArrayList<>();

        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(4000));
        for (var record : consumerRecords) {
            try {
                UserMessage userMessage = UserMessage.deserialize(record.value());
                // had a log trace removed here because it was printing extra dto fields
                // do we need another log message here then?

                userMessageList.add(userMessage);
            } catch (JsonProcessingException e) {
                logger.warn("Encountered exception while trying to deserialize record: ", e);
            }
        }
        consumer.commitSync();
        return userMessageList;
    }
}

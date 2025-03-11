package com.github.tunashred.clients;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.MessageInfo;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public class ConsumerRunnable implements Runnable {
    private static final Logger logger = LogManager.getLogger(ConsumerRunnable.class);
    private final String groupTopic;
    private AtomicBoolean keepRunnning = new AtomicBoolean(true);

    public ConsumerRunnable(String groupTopic) {
        this.groupTopic = groupTopic;
    }

    public void stopRunning() {
        keepRunnning.set(false);
    }

    @Override
    public void run() {
        Properties consumerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/consumer.properties")) {
            consumerProps.load(propsFile);
            consumerProps.put(GROUP_ID_CONFIG, "consumer-" + UUID.randomUUID().toString());
        } catch (IOException e) {
            logger.error("Failed to load kafka streams properties file: ", e);
            // TODO: should I keep this?
            // maybe add instead some default properties? but then what is the purpose of using an externalized config
            // if not for the fewer lines of code in this file?
            throw new RuntimeException();
        }

        logger.info("Initializing consumer");
        while (keepRunnning.get()) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList(groupTopic));
                logger.info("Consumer initialized and subscribed to group topic '" + groupTopic + "'");

                while (keepRunnning.get()) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (var record : consumerRecords) {
                        try {
                            MessageInfo messageInfo = MessageInfo.deserialize(record.value());
                            logger.info("\nGroup chat: " + messageInfo.getGroupChat().getChatName() + "/" + messageInfo.getGroupChat().getChatID() +
                                    "\nUser: " + messageInfo.getUser().getName() + "/" + messageInfo.getUser().getUserID() +
                                    "\nMessage: " + messageInfo.getMessage());

                            System.out.println(messageInfo.getUser().getName() + ": " + messageInfo.getMessage());
                        } catch (JsonProcessingException e) {
                            logger.warn("Encountered exception while trying to deserialize record: ", e);
                        }
                    }
                    consumer.commitSync();
                }
            }
        }
    }
}

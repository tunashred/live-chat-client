package com.github.tunashred.clients;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.MessageInfo;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.MapMessage;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public class ConsumerRunnable implements Runnable {
    private static final Logger logger = LogManager.getLogger(ConsumerRunnable.class);
    private final String user;
    private final String groupTopic;
    private AtomicBoolean keepRunnning = new AtomicBoolean(true);

    public ConsumerRunnable(String user, String groupTopic) {
        this.user = user;
        this.groupTopic = groupTopic;
    }

    public void stopRunning() {
        keepRunnning.set(false);
    }

    @Override
    public void run() throws RuntimeException {
        Properties consumerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/consumer.properties")) {
            consumerProps.load(propsFile);
            consumerProps.put(GROUP_ID_CONFIG, "consumer-" + user);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
                            logger.trace(() -> new MapMessage<>(Map.of(
                                    "Group", messageInfo.getGroupChat().getChatName() + "/" + messageInfo.getGroupChat().getChatID(),
                                    "User", messageInfo.getUser().getName() + "/" + messageInfo.getUser().getUserID(),
                                    "Message", messageInfo.getMessage()
                            )));


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

package com.github.tunashred.clients;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.UserMessage;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

@Log4j2
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class Consumer {
    KafkaConsumer<String, String> consumer;

    public Consumer(String channelName, String username) throws IOException {
        this(channelName, username, new Properties());
    }

    public Consumer(String channelName, String username, Properties properties) throws IOException {
        log.info("Initializing consumer");
        Properties consumerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/consumer.properties")) {
            consumerProps.load(propsFile);
            consumerProps.putAll(properties);
            consumerProps.put(GROUP_ID_CONFIG, username + channelName);

            this.consumer = new KafkaConsumer<>(consumerProps);
            this.consumer.subscribe(Collections.singletonList(channelName));
            Set<TopicPartition> assignments = this.consumer.assignment();
            this.consumer.seekToEnd(assignments);
            log.info("Consumer" + "'" + username + "'" + "initialized and subscribed to group topic '" + channelName + "'");
        }
    }

    public List<UserMessage> consume() throws RuntimeException {
        List<UserMessage> userMessageList = new ArrayList<>();

        log.trace("Polling");
        ConsumerRecords<String, String> consumerRecords = this.consumer.poll(Duration.ofMillis(300));
        for (var record : consumerRecords) {
            log.trace("Record to be processed: " + record);
            try {
                UserMessage userMessage = UserMessage.deserialize(record.value());

                userMessageList.add(userMessage);
            } catch (JsonProcessingException e) {
                log.warn("Encountered exception while trying to deserialize record: ", e);
            }
        }
        this.consumer.commitSync();
        log.info("Done consuming");
        return userMessageList;
    }
}

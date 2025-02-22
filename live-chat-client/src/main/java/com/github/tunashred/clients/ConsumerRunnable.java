package com.github.tunashred.clients;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.MessageInfo;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class ConsumerRunnable implements Runnable {
    private AtomicBoolean keepRunnning = new AtomicBoolean(true);

    public void stopRunning() {
        keepRunnning.set(false);
    }

    @Override
    public void run() {
        Properties consumerProps = new Properties();
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        consumerProps.put(GROUP_ID_CONFIG, "consumer-" + UUID.randomUUID().toString());
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put("acks", "all");

        while (keepRunnning.get()) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList("safe_chat"));

                while (keepRunnning.get()) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (var record : consumerRecords) {
                        try {
                            MessageInfo messageInfo = MessageInfo.deserialize(record.value());
                            System.out.println("\nGroup chat: " + messageInfo.getGroupChat().getChatName() + "/" + messageInfo.getGroupChat().getChatID() +
                                    "\nUser: " + messageInfo.getUser().getName() + "/" + messageInfo.getUser().getUserID() +
                                    "\nOriginal message: " + messageInfo.getMessage());
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }
                    consumer.commitSync();
                }
            }
        }
    }
}

package com.github.tunashred.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tunashred.dtos.MessageInfo;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class MessageProducer {
    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        consumerProps.put(GROUP_ID_CONFIG, "moderator-consumer-group");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("acks", "all");

        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("safe_chat"));

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        ObjectMapper objectMapper = new ObjectMapper();
        List<MessageInfo> messages = null;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                messages = objectMapper.readValue(new File("input_messages/mixed_messages.json"), objectMapper.getTypeFactory().constructCollectionType(List.class, MessageInfo.class));
                for (MessageInfo message : messages) {
                    String serialized = objectMapper.writeValueAsString(message);
                    ProducerRecord<String, String> record = new ProducerRecord<>("unsafe_chat", message.getGroupChat().getChatID(), serialized);
                    System.out.println("\nGroup chat: " + message.getGroupChat().getChatName() + "/" + message.getGroupChat().getChatID() +
                            "\nmessage.User: " + message.getUser().getName() + "/" + message.getUser().getUserID() +
                            "\nMessage: " + message.getMessage());
                    producer.send(record);
                }

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            producer.flush();
            producer.close();
            consumer.close();
        }
    }
}


package com.github.tunashred.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tunashred.messageformats.MessageInfo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class MessageProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        File messages_file = new File("input_messages/mixed_messages.json");
        ObjectMapper objectMapper = new ObjectMapper();
        List<MessageInfo> messages = null;
        try {
            messages = objectMapper.readValue(messages_file, objectMapper.getTypeFactory().constructCollectionType(List.class, MessageInfo.class));
            for (MessageInfo message : messages) {
                String serialized = objectMapper.writeValueAsString(message);
                ProducerRecord<String, String> record = new ProducerRecord<>("unsafe_chat", message.getGroupChat().getChatID(), serialized);
                System.out.println("\nGroup chat: " + message.getGroupChat().getChatName() + "/" + message.getGroupChat().getChatID() +
                        "\nmessage.User: " + message.getUser().getName() + "/" + message.getUser().getUserID() +
                        "\nMessage: " + message.getMessage());
                producer.send(record);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        producer.flush();
        producer.close();
    }
}


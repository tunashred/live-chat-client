package com.github.tunashred.clients;

import com.github.tunashred.dtos.GroupChat;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.dtos.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerRunnable implements Runnable {
    private AtomicBoolean keepRunnning = new AtomicBoolean(true);

    public void stopRunning() {
        keepRunnning.set(false);
    }

    @Override
    public void run() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (keepRunnning.get()) {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                while (keepRunnning.get()) {
                    try {
                        MessageInfo messageInfo = new MessageInfo(new GroupChat("some group", "6969"), new User("some user", "13018"), reader.readLine());
                        String serialized = MessageInfo.serialize(messageInfo);

                        ProducerRecord<String, String> record = new ProducerRecord<>("unsafe_chat", messageInfo.getGroupChat().getChatID(), serialized);
                        producer.send(record);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}

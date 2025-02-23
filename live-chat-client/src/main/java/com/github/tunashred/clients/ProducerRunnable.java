package com.github.tunashred.clients;

import com.github.tunashred.dtos.GroupChat;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.dtos.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerRunnable implements Runnable {
    private final User user;
    private final GroupChat groupChat;
    private AtomicBoolean keepRunnning = new AtomicBoolean(true);

    public ProducerRunnable(User user, GroupChat groupChat) {
        this.groupChat = groupChat;
        this.user = user;
    }

    public void stopRunning() {
        keepRunnning.set(false);
    }

    @Override
    public void run() {
        Properties producerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/producer.properties")) {
            producerProps.load(propsFile);
        } catch (IOException e) {
            e.printStackTrace();
            // maybe add instead some default properties? but then what is the purpose of using an externalized config
            // if not for the fewer lines of code in this file?
            throw new RuntimeException(e.getMessage());
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (keepRunnning.get()) {
            // try initializing producer and choosing a group and name for the user
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                while (keepRunnning.get()) {
                    try {
                        MessageInfo messageInfo = new MessageInfo(groupChat, user, reader.readLine());
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

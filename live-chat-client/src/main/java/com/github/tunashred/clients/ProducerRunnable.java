package com.github.tunashred.clients;

import com.github.tunashred.dtos.GroupChat;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.dtos.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerRunnable implements Runnable {
    private static final Logger logger = LogManager.getLogger(ProducerRunnable.class);
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
            logger.error("Failed to load kafka streams properties file: ", e);
            // TODO: should I keep this?
            // maybe add instead some default properties? but then what is the purpose of using an externalized config
            // if not for the fewer lines of code in this file?
            throw new RuntimeException(e.getMessage());
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        logger.info("Initializing producer");
        while (keepRunnning.get()) {
            // try initializing producer and choosing a group and name for the user
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                logger.info("Producer ready");
                while (keepRunnning.get()) {
                    try {
                        MessageInfo messageInfo = new MessageInfo(groupChat, user, reader.readLine());
                        String serialized = MessageInfo.serialize(messageInfo);

                        ProducerRecord<String, String> record = new ProducerRecord<>("unsafe_chat", messageInfo.getGroupChat().getChatID(), serialized);

                        // TODO: rethink logging records here... maybe add a toString inside the dto?
                        logger.info("New record: Group chat: " + messageInfo.getGroupChat().getChatName() + "/" + messageInfo.getGroupChat().getChatID() +
                                "User: " + messageInfo.getUser().getName() + "/" + messageInfo.getUser().getUserID() +
                                "Message: " + messageInfo.getMessage());

                        producer.send(record);
                        logger.info("Record sent");
                    } catch (IOException e) {
                        logger.warn("Encountered exception while trying to serialize record: ", e);
                    }
                }
            }
        }
    }
}

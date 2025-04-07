package com.github.tunashred.clients;

import com.github.tunashred.dtos.UserMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class Producer {
    private static final Logger logger = LogManager.getLogger(Producer.class);

    private KafkaProducer<String, String> kafkaProducer;

    public Producer() throws IOException {
        logger.info("Initializing producer");
        Properties producerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/producer.properties")) {
            producerProps.load(propsFile);
            this.kafkaProducer = new KafkaProducer<>(producerProps);
            logger.info("Producer ready");
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Producer my_producer = new Producer();
        String channel = "baia-mare";
        String username = "gulie";
        String message = "asta e mesaj cica";

        for (int i = 0; i < 2; i++) {
            my_producer.sendMessage(channel, username, message + i + i + i);
        }
    }

    // maybe return a confirmation that the message was really sent successfully?
    public void sendMessage(String channel, String username, String message) {
        try {
            UserMessage userMessage = new UserMessage(username, message);
            String serialized = UserMessage.serialize(userMessage);

            ProducerRecord<String, String> record = new ProducerRecord<>(channel, channel, serialized);

            kafkaProducer.send(record);
            kafkaProducer.flush();
        } catch (IOException e) {
            logger.warn("Encountered exception while trying to serialize record: ", e);
        }
    }
}

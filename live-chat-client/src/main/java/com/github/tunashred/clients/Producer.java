package com.github.tunashred.clients;

import com.github.tunashred.dtos.UserMessage;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Log4j2
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class Producer {
    KafkaProducer<String, String> producer;

    public Producer() throws IOException {
        log.info("Initializing producer");
        Properties producerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/producer.properties")) {
            producerProps.load(propsFile);
            this.producer = new KafkaProducer<>(producerProps);
            log.info("Producer ready");
        }
    }

    // maybe return a confirmation that the message was really sent successfully?
    public void sendMessage(String channel, String username, String message) {
        try {
            UserMessage userMessage = new UserMessage(username, message);
            String serialized = UserMessage.serialize(userMessage);
            log.trace("Serialized user message");

            ProducerRecord<String, String> record = new ProducerRecord<>("unsafe_chat", channel, serialized);

            this.producer.send(record);
            this.producer.flush();
            log.trace("Record flushed");
        } catch (IOException e) {
            log.warn("Encountered exception while trying to serialize record: ", e);
        }
    }
}

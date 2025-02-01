import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ModeratorProducerConsumer {
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
        consumer.subscribe(Collections.singletonList("unsafe_chat"));

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        Moderator moderator = new Moderator("packs/banned.txt");

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    ProcessedMessage processedMessage = moderator.censor(record.value());
                    System.out.printf("\nUser ID: %s\nOriginal message: %s\nProcessed message: %s\n", record.key(), record.value(), processedMessage.getProcessedMessage());

                    if (processedMessage.isCensored()) {
                        producer.send(new ProducerRecord<>("flagged_messages", record.key(), record.value()));
                    }
                    producer.send(new ProducerRecord<>("safe_chat", record.key(), processedMessage.getProcessedMessage()));
                }
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            consumer.close();
            producer.close();
        }
    }
}

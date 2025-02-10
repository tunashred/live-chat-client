package com.github.tunashred.clients;

public class KafkaClient {
    public static void main(String[] args) {
        Thread consumer = new Thread(new ConsumerRunnable());
        consumer.start();

        Thread producer = new Thread(new ProducerRunnable());
        producer.start();
    }
}

package com.github.tunashred.clients.demo;

import com.github.tunashred.clients.Consumer;
import com.github.tunashred.clients.Producer;
import com.github.tunashred.dtos.UserMessage;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.Random;

//@Deprecated
public class DemoAgent {
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: java -jar <this_class>.jar <channel_name> <username> <message_file_path>");
            System.exit(1);
        }

        String channelName = args[0];
        String username = args[1];
        String filePath = args[2];

        List<String> messages = Files.readAllLines(Paths.get(filePath));
        Random random = new Random();
        int maxDelay = random.nextInt(4000);

        Properties securityProps = new Properties();
        try (InputStream inputStream = new FileInputStream("src/main/resources/security.properties")) {
            securityProps.load(inputStream);
        } catch (IOException e) {
            throw new IOException(e);
        }

        Properties consumerProps = new Properties();
        try (InputStream inputStream = new FileInputStream("src/main/resources/consumer.properties")) {
            consumerProps.load(inputStream);
            consumerProps.putAll(securityProps);
        } catch (IOException e) {
            throw new IOException(e);
        }

        Properties producerProps = new Properties();
        try (InputStream inputStream = new FileInputStream("src/main/resources/producer.properties")) {
            producerProps.load(inputStream);
            producerProps.putAll(securityProps);
        } catch (IOException e) {
            throw new IOException(e);
        }

        Producer producer = new Producer(producerProps);
        Consumer consumer = new Consumer(channelName, username, consumerProps);
        consumer.consume();

        while (true) {
            for (String message : messages) {
                simulateTypingAndErase(message, 30, 80);
                producer.sendMessage(channelName, username, message);

                int delay = 500 + random.nextInt(maxDelay);
                Thread.sleep(delay);
                printMessages(consumer.consume());
            }
        }
    }

    private static void simulateTypingAndErase(String message, int minDelayMs, int maxDelayMs) throws InterruptedException {
        Random rand = new Random();

        StringBuilder buffer = new StringBuilder();

        System.out.print(": ");
        for (char c : message.toCharArray()) {
            buffer.append(c);
            System.out.print(c);
            System.out.flush();
            Thread.sleep(minDelayMs + rand.nextInt(maxDelayMs - minDelayMs + 1));
        }

        Thread.sleep(500);

        System.out.print("\r" + " ".repeat(100) + "\r");
        System.out.flush();
    }

    private static void printMessages(List<UserMessage> messages) {
        for (UserMessage message : messages) {
            System.out.println(message.getUsername() + ": " + message.getMessage());
        }
    }
}

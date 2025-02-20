package com.github.tunashred.clients;

import com.github.tunashred.dtos.GroupChat;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.dtos.User;
import com.github.tunashred.utils.GroupchatCreatorReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
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
            // try initializing producer and choosing a group and name for the user
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                List<GroupChat> groups = GroupchatCreatorReader.getGroups("input/groups.txt");
                GroupchatCreatorReader.printGroups(groups);
                System.out.println("Choose a group to join: ");

                String input = reader.readLine();
                int selection = Integer.parseInt(input);
                while (selection < 0 || selection > groups.size() - 1) {
                    System.out.println("Invalid selection. Please enter a number between " + 0 + " and " + (groups.size() - 1));
                    input = reader.readLine();
                    selection = Integer.parseInt(input);
                }
                // TODO: ctor with object arg
                GroupChat chosenGroup = new GroupChat(groups.get(selection).getChatName(), groups.get(selection).getChatID());

                System.out.println("Please choose your username: ");
                input = reader.readLine();
                // TODO: maybe move hashing part to a separate function
                User user = new User(input, String.valueOf(input.length()));
                while (keepRunnning.get()) {
                    try {
                        MessageInfo messageInfo = new MessageInfo(chosenGroup, user, reader.readLine());
                        String serialized = MessageInfo.serialize(messageInfo);

                        ProducerRecord<String, String> record = new ProducerRecord<>("unsafe_chat", messageInfo.getGroupChat().getChatID(), serialized);
                        producer.send(record);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

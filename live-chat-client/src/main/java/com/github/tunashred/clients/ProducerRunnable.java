package com.github.tunashred.clients;

import com.github.tunashred.dtos.GroupChat;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.dtos.User;
import com.github.tunashred.utils.GroupchatCreatorReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
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
                int chosenGroupIndex = selection;

                System.out.println("Please choose your username: ");
                input = reader.readLine();
                User user = new User(input);

                // start sending messages
                while (keepRunnning.get()) {
                    try {
                        MessageInfo messageInfo = new MessageInfo(groups.get(chosenGroupIndex), user, reader.readLine());
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

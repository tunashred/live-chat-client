import java.io.*;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

import org.apache.kafka.common.serialization.StringSerializer;

public class MessageProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        File good_messages_file = new File("input_messages/good_messages.txt");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(good_messages_file));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>("unsafe_chat", "69", line);

                final String line_2 = line;
                producer.send(record, ((recordMetadata, e) -> {
                    if (e == null) {
                        System.out.println("Send message: " + line_2);
                    } else {
                        e.printStackTrace();
                    }
                }));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        producer.flush();
        producer.close();
    }
}

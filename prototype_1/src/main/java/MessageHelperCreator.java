import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.Objects;

public class MessageHelperCreator {
    public static void main(String[] args) throws IOException {
        File file = new File("input_messages/mixed_messages.json");
        ObjectMapper objectMapper = new ObjectMapper();

        GroupChat groupChat = new GroupChat("chat_test", "01236969");
        User user = new User("Ionel", "189432");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));

        String message;
        while (true) {
            message = reader.readLine();
            if (Objects.equals(message, "0")) {
                break;
            }
            MessageInfo messageInfo = new MessageInfo(groupChat, user, message);
            String serialized = objectMapper.writeValueAsString(messageInfo);

            writer.write(serialized);
            writer.newLine();
            System.out.println("Group chat: " + groupChat.getChatName() + "/" + groupChat.getChatID() +
                    "\nUser: " + user.getName() + "/" + user.getUserID() +
                    "\nMessage: " + message);

        }
        writer.flush();
        writer.close();
    }
}

package com.github.tunashred.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tunashred.dtos.GroupChat;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.dtos.User;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class MessageHelperCreator {
    public static void main(String[] args) throws IOException {
        File file = new File("input_messages/mixed_messages.json");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        ObjectMapper objectMapper = new ObjectMapper();

        GroupChat groupChat = new GroupChat("chat_test");
        User user = new User("Ionel");

        List<MessageInfo> messages = new ArrayList<>();
        while (true) {
            String message = reader.readLine();
            if ("0".equals(message)) {
                break;
            }
            MessageInfo messageInfo = new MessageInfo(groupChat, user, message);
            messages.add(messageInfo);

            System.out.println("Group chat: " + groupChat.getChatName() + "/" + groupChat.getChatID() +
                    "\nmessage.User: " + user.getName() + "/" + user.getUserID() +
                    "\nMessage: " + message);

        }
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(file, messages);
    }
}

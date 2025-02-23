package com.github.tunashred.clients;

import com.github.tunashred.dtos.GroupChat;
import com.github.tunashred.dtos.User;
import com.github.tunashred.utils.GroupchatCreatorReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class KafkaClient {
    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

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

        ChatSession session = new ChatSession(user, groups.get(chosenGroupIndex));
    }
}

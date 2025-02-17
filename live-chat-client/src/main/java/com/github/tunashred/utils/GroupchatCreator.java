package com.github.tunashred.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tunashred.dtos.GroupChat;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class GroupchatCreator {
    public static void main(String[] args) throws IOException {
        File file = new File("input/groups.txt");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        ObjectMapper mapper = new ObjectMapper();

        List<GroupChat> groups = new ArrayList<>();
        if (file.exists() && file.length() > 0) {
            try {
                groups = mapper.readValue(file, new TypeReference<List<GroupChat>>() {
                });
            } catch (Exception e) {
                System.out.println("Error reading existing groups file, starting with an empty list.");
                groups = new ArrayList<>();
            }
        }

        System.out.print("Enter a group name: ");
        String groupName = reader.readLine();
        GroupChat group = new GroupChat(groupName, DigestUtils.sha256Hex(groupName));
        groups.add(group);
        System.out.println("Group name: " + groupName + "\nGroup ID: " + group.getChatID());

        mapper.writerWithDefaultPrettyPrinter().writeValue(file, groups);
    }
}

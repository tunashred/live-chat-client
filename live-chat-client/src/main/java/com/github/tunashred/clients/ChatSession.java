package com.github.tunashred.clients;

import com.github.tunashred.dtos.GroupChat;
import com.github.tunashred.dtos.User;

public class ChatSession {
    User user;
    GroupChat groupChat;

    Thread consumerThread;
    Thread producerThread;

    public ChatSession(User user, GroupChat groupChat) {
        this.user = user;
        this.groupChat = groupChat;

        // having user id allows multiple users having same name in a common group chat
        this.consumerThread = new Thread(new ConsumerRunnable(user.getUserID(), groupChat.getChatName()));
        consumerThread.start();

        this.producerThread = new Thread(new ProducerRunnable(user, groupChat));
        producerThread.start();
    }
}

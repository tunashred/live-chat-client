package com.github.tunashred.clients;

import com.github.tunashred.dtos.GroupChat;
import com.github.tunashred.dtos.User;

public class ChatSession {
    User user;
    GroupChat groupChat;

    ConsumerRunnable consumer;
    ProducerRunnable producer;

    Thread consumerThread;
    Thread producerThread;

    public ChatSession(User user, GroupChat groupChat) {
        this.user = user;
        this.groupChat = groupChat;

        this.consumer = new ConsumerRunnable(groupChat.getChatName());
        this.consumerThread = new Thread(this.consumer);
        consumerThread.start();

        this.producer = new ProducerRunnable(user, groupChat);
        this.producerThread = new Thread(this.producer);
        producerThread.start();
    }
}

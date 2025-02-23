package com.github.tunashred.clients;

import com.github.tunashred.dtos.GroupChat;
import com.github.tunashred.dtos.User;

public class ChatSession {
    User user;
    GroupChat groupChat;
    Thread consumer;
    Thread producer;

    public ChatSession(User user, GroupChat groupChat) {
        this.user = user;
        this.groupChat = groupChat;

        this.consumer = new Thread(new ConsumerRunnable(groupChat.getChatName()));
        consumer.start();

        this.producer = new Thread(new ProducerRunnable(user, groupChat));
        producer.start();
    }
}

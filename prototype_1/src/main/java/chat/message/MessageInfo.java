package chat.message;

public class MessageInfo {
    private GroupChat groupChat;
    private User user;
    private String message;

    public MessageInfo(GroupChat groupChat, User user, String message) {
        this.groupChat = groupChat;
        this.user = user;
        this.message = message;
    }

    public MessageInfo() {
    }

    public GroupChat getGroupChat() {
        return groupChat;
    }

    public User getUser() {
        return user;
    }

    public String getMessage() {
        return message;
    }
}

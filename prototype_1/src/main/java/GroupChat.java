public class GroupChat {
    private String chatName;
    private String chatID;

    public GroupChat(String chatName, String chatID) {
        this.chatName = chatName;
        this.chatID = chatID;
    }

    public GroupChat() {
    }

    public String getChatName() {
        return chatName;
    }

    public String getChatID() {
        return chatID;
    }
}

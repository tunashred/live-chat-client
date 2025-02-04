public class ProcessedMessage extends MessageInfo {
    private final String processedMessage;
    private final boolean isCensored;

    public ProcessedMessage(GroupChat groupChat, User user, String message, String processedMessage, boolean isCensored) {
        super(groupChat, user, message);
        this.processedMessage = processedMessage;
        this.isCensored = isCensored;
    }

    public ProcessedMessage(String processedMessage, boolean isCensored) {
        this.processedMessage = processedMessage;
        this.isCensored = isCensored;
    }

    public String getProcessedMessage() {
        return processedMessage;
    }

    public boolean isCensored() {
        return isCensored;
    }
}

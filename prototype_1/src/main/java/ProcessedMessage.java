public class ProcessedMessage {
    private final String processedMessage;
    private final boolean isCensored;

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

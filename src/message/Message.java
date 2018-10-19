package message;

/**
 *  A Message class models every message pass across the clients and replicas with a string literal for simplicity
 */
public class Message {
    /**
     *  Examples:
     *  HeartBeat Message: "HEART_BEAT:0:1539876988101", note that "0" is the view number and "1539876988101" is the timestamp.
     */
    private final String messageLiteral;
    public enum MESSAGE_TYPE {
        HEART_BEAT,
    }

    public Message(String messageLiteral) {
        this.messageLiteral = messageLiteral;
    }

    public static Message parseFromString(final String messageLiteral) {
        return null;
    }


}

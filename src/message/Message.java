package message;

/**
 * A Message class models every message pass across the clients and replicas with a string literal for simplicity
 */
public class Message {
    /**
     * Examples:
     * HeartBeat Message: "HEART_BEAT:0:1539876988101", note that "0" is the view number and "1539876988101" is the timestamp.
     */

    public enum MESSAGE_TYPE {
        CLIENT_TO_SERVER,
        SERVER_TO_CLIENT,
        HEART_BEAT,
        PREPARE,
        PREPARE_RESPONSE,
        ACCEPT,
        ACCEPT_RESPONSE,
        SUCCESS,
    }

    private final String messageLiteral;
    private final MESSAGE_TYPE messageType;

    public Message(String messageLiteral) {
        this.messageLiteral = messageLiteral;
        this.messageType = getMessageType(messageLiteral);
    }

    public static MESSAGE_TYPE getMessageType(final String messageLiteral) {
        final String type = messageLiteral.split(":")[0];
        switch (type) {
            case "CLIENT_TO_SERVER":
                return MESSAGE_TYPE.CLIENT_TO_SERVER;
            case "SERVER_TO_CLIENT":
                return MESSAGE_TYPE.SERVER_TO_CLIENT;
            case "HEART_BEAT":
                return MESSAGE_TYPE.HEART_BEAT;
            case "PREPARE":
                return MESSAGE_TYPE.PREPARE;
            case "PREPARE_RESPONSE":
                return MESSAGE_TYPE.PREPARE_RESPONSE;
            case "ACCEPT":
                return MESSAGE_TYPE.ACCEPT;
            case "ACCEPT_RESPONSE":
                return MESSAGE_TYPE.ACCEPT_RESPONSE;
            case "SUCCESS":
                return MESSAGE_TYPE.SUCCESS;
            default:
                throw new IllegalArgumentException("Can not detect message type!");
        }
    }

    public static Message parseFromString(final String messageLiteral) {
        return null;
    }


}

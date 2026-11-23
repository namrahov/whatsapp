package az.whatsapp.gateway;

import az.whatsapp.dto.event.MessageCreatedEvent;
import az.whatsapp.entity.MessageEntity;

import java.time.Instant;
import java.util.UUID;

/**
 * The JSON frames of the WebSocket protocol.
 * <pre>
 * client → server  {"type":"SEND","conversationId":1,"clientMessageId":"uuid","payload":"base64"}
 *                  {"type":"ACK","messageId":10}     DELIVERED: stored on the device
 *                  {"type":"READ","messageId":10}    READ: opened by the user
 * server → client  {"type":"SENT",...}               durable acceptance of a SEND
 *                  {"type":"MESSAGE",...}            a new message in one of the user's conversations
 *                  {"type":"ERROR",...}              a frame was rejected; retryable=true means resend as is
 * </pre>
 */
public final class Frames {

    private Frames() {
    }

    public record ClientFrame(String type, Long conversationId, UUID clientMessageId, String payload, Long messageId) {
    }

    public record Sent(String type, long messageId, long conversationId, UUID clientMessageId, long sequenceNumber,
                       Instant createdAt, boolean duplicate) {

        public static Sent of(MessageEntity message, boolean duplicate) {
            return new Sent("SENT", message.getId(), message.getConversationId(), message.getClientMessageId(),
                    message.getSequenceNumber(), message.getCreatedAt(), duplicate);
        }
    }

    /** Clients display by sequenceNumber, not arrival order, and drop a messageId they already have. */
    public record Message(String type, long messageId, long conversationId, long senderId, long sequenceNumber,
                          String payload, Instant createdAt) {

        public static Message of(MessageCreatedEvent event) {
            return new Message("MESSAGE", event.messageId(), event.conversationId(), event.senderId(),
                    event.sequenceNumber(), event.payload(), event.createdAt());
        }
    }

    public record Error(String type, UUID clientMessageId, Long messageId, int status, String reason,
                        boolean retryable) {

        public static Error of(ClientFrame frame, int status, String reason, boolean retryable) {
            return new Error("ERROR", frame == null ? null : frame.clientMessageId(),
                    frame == null ? null : frame.messageId(), status, reason, retryable);
        }
    }
}

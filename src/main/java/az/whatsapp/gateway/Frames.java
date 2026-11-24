package az.whatsapp.gateway;

import az.whatsapp.dto.event.MessageChangedEvent;
import az.whatsapp.dto.event.MessageCreatedEvent;
import az.whatsapp.dto.event.ReceiptAdvancedEvent;
import az.whatsapp.entity.MessageEntity;
import az.whatsapp.entity.ReceiptKind;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * The JSON frames of the WebSocket protocol.
 * <pre>
 * client → server  {"type":"SEND","conversationId":1,"clientMessageId":"uuid","payload":"base64"}
 *                  {"type":"ACK","conversationId":1,"upToSequence":5}    this device stored everything up to 5
 *                  {"type":"READ","conversationId":1,"upToSequence":5}   the user has seen everything up to 5
 *                  {"type":"HEARTBEAT"}                                  every ~15 s; silent sockets are closed
 * server → client  {"type":"PONG"}          answer to HEARTBEAT
 *                  {"type":"SENT",...}      durable acceptance of a SEND
 *                  {"type":"MESSAGE",...}   a new message in one of the user's conversations
 *                  {"type":"MESSAGE_CHANGED",...} an edit or delete-for-everyone (apply if version is newer)
 *                  {"type":"RECEIPT",...}   a delivery/read cursor moved (userId null = all members of a group)
 *                  {"type":"ERROR",...}     a frame was rejected; retryable=true means resend it as is
 * </pre>
 * Client rules: show messages by sequenceNumber, not arrival order; drop a messageId already stored. If a
 * sequence is missing (5 then 7), wait briefly, then fetch {@code GET /messages?afterSequence=5}. ACK only what
 * is stored locally, cumulatively. A READ receipt up to N also means DELIVERED up to N.
 */
public final class Frames {

    /** Answer to {"type":"HEARTBEAT"}, which clients send every ~15 s. */
    public static final Map<String, String> PONG = Map.of("type", "PONG");

    private Frames() {
    }

    public record ClientFrame(String type, Long conversationId, UUID clientMessageId, String payload,
                              Long upToSequence) {
    }

    public record Sent(String type, long messageId, long conversationId, UUID clientMessageId, long sequenceNumber,
                       Instant createdAt, boolean duplicate) {

        public static Sent of(MessageEntity message, boolean duplicate) {
            return new Sent("SENT", message.getId(), message.getConversationId(), message.getClientMessageId(),
                    message.getSequenceNumber(), message.getCreatedAt(), duplicate);
        }
    }

    public record Message(String type, long messageId, long conversationId, long senderId, long sequenceNumber,
                          String payload, Instant createdAt) {

        public static Message of(MessageCreatedEvent event) {
            return new Message("MESSAGE", event.messageId(), event.conversationId(), event.senderId(),
                    event.sequenceNumber(), event.payload(), event.createdAt());
        }
    }

    /** An edit or delete-for-everyone. Apply only if {@code version} is higher than the one shown. */
    public record MessageChanged(String type, long messageId, long conversationId, long sequenceNumber, int version,
                                 long changeNumber, boolean deleted, String payload, Instant changedAt) {

        public static MessageChanged of(MessageChangedEvent event) {
            return new MessageChanged("MESSAGE_CHANGED", event.messageId(), event.conversationId(),
                    event.sequenceNumber(), event.version(), event.changeNumber(), event.deleted(), event.payload(),
                    event.changedAt());
        }
    }

    /** @param userId whose cursor moved; {@code null} = every member of the group has reached upToSequence */
    public record Receipt(String type, long conversationId, Long userId, ReceiptKind kind, long upToSequence) {

        public static Receipt of(ReceiptAdvancedEvent event) {
            return new Receipt("RECEIPT", event.conversationId(), event.userId(), event.kind(), event.upToSequence());
        }
    }

    public record Error(String type, UUID clientMessageId, Long conversationId, int status, String reason,
                        boolean retryable) {

        public static Error of(ClientFrame frame, int status, String reason, boolean retryable) {
            return new Error("ERROR", frame == null ? null : frame.clientMessageId(),
                    frame == null ? null : frame.conversationId(), status, reason, retryable);
        }
    }
}

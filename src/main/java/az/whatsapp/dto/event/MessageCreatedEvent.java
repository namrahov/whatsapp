package az.whatsapp.dto.event;

import az.whatsapp.entity.MessageEntity;

import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

/**
 * Published (via the outbox) after a message is durably stored. Carries the ciphertext too, so the delivery
 * worker can push the message without reading it back from PostgreSQL.
 */
public record MessageCreatedEvent(
        long messageId,
        long conversationId,
        long senderId,
        UUID clientMessageId,
        long sequenceNumber,
        String payload,
        Instant createdAt) {

    public static final String TYPE = "MessageCreated";

    public static MessageCreatedEvent from(MessageEntity message) {
        return new MessageCreatedEvent(message.getId(), message.getConversationId(), message.getSenderId(),
                message.getClientMessageId(), message.getSequenceNumber(),
                Base64.getEncoder().encodeToString(message.getEncryptedPayload()), message.getCreatedAt());
    }
}

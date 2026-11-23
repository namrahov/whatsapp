package az.whatsapp.dto.response;

import az.whatsapp.entity.MessageEntity;

import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

public record MessageResponse(long messageId, long conversationId, long senderId, UUID clientMessageId,
                              long sequenceNumber, String payload, Instant createdAt) {

    public static MessageResponse from(MessageEntity message) {
        return new MessageResponse(message.getId(), message.getConversationId(), message.getSenderId(),
                message.getClientMessageId(), message.getSequenceNumber(),
                Base64.getEncoder().encodeToString(message.getEncryptedPayload()), message.getCreatedAt());
    }
}

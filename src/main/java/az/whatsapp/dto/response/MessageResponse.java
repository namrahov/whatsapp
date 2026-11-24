package az.whatsapp.dto.response;

import az.whatsapp.entity.MessageEntity;

import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

/**
 * A message as it is now. {@code payload} is null when deleted for everyone (a tombstone that keeps its sequence).
 * {@code version} grows with every edit/delete; {@code changeNumber} is its place in the conversation's change
 * stream (null if never changed).
 */
public record MessageResponse(long messageId, long conversationId, long senderId, UUID clientMessageId,
                              long sequenceNumber, String payload, Instant createdAt, int version,
                              Long changeNumber, Instant editedAt, boolean deleted) {

    public static MessageResponse from(MessageEntity message) {
        return new MessageResponse(message.getId(), message.getConversationId(), message.getSenderId(),
                message.getClientMessageId(), message.getSequenceNumber(),
                message.isDeleted() ? null : Base64.getEncoder().encodeToString(message.getEncryptedPayload()),
                message.getCreatedAt(), message.getVersion(), message.getChangeNumber(), message.getEditedAt(),
                message.isDeleted());
    }
}

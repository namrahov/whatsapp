package az.whatsapp.dto.response;

import az.whatsapp.entity.MessageEntity;

import java.time.Instant;
import java.util.UUID;

/**
 * The SENT acknowledgement: the message is durably stored. {@code duplicate} is true when this was a retry of
 * a message the server had already accepted; the ids and sequence are those of the original.
 */
public record SendMessageResponse(long messageId, long conversationId, UUID clientMessageId, long sequenceNumber,
                                  Instant createdAt, boolean duplicate) {

    public static SendMessageResponse from(MessageEntity message, boolean duplicate) {
        return new SendMessageResponse(message.getId(), message.getConversationId(), message.getClientMessageId(),
                message.getSequenceNumber(), message.getCreatedAt(), duplicate);
    }
}

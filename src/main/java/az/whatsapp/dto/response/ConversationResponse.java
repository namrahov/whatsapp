package az.whatsapp.dto.response;

import az.whatsapp.entity.ConversationEntity;
import az.whatsapp.entity.ConversationType;

import java.time.Instant;

/**
 * What a client needs after (re)connecting:
 * <ul>
 *   <li>{@code lastSequence - lastReadSequence} = unread count;</li>
 *   <li>{@code lastReceivedSequence} = where this device's sync starts
 *       ({@code GET /messages?afterSequence=lastReceivedSequence}); null without X-Device-Id.</li>
 * </ul>
 */
public record ConversationResponse(long id, ConversationType type, String title, long lastSequence,
                                   long lastReadSequence, Long lastReceivedSequence, Instant createdAt) {

    public static ConversationResponse from(ConversationEntity conversation) {
        return from(conversation, 0, null);
    }

    public static ConversationResponse from(ConversationEntity conversation, long lastReadSequence,
                                            Long lastReceivedSequence) {
        return new ConversationResponse(conversation.getId(), conversation.getType(), conversation.getTitle(),
                conversation.getLastSequence(), lastReadSequence, lastReceivedSequence, conversation.getCreatedAt());
    }
}

package az.whatsapp.dto.response;

import az.whatsapp.entity.ConversationEntity;
import az.whatsapp.entity.ConversationType;

import java.time.Instant;

/**
 * What a client needs after (re)connecting:
 * <ul>
 *   <li>{@code lastSequence - lastReadSequence} = unread count;</li>
 *   <li>{@code lastReceivedSequence} = where this device's sync starts
 *       ({@code GET /messages?afterSequence=lastReceivedSequence}); null without X-Device-Id;</li>
 *   <li>{@code firstAvailableSequence}: older messages were deleted by retention, do not wait for them;</li>
 *   <li>{@code lastChange}: if higher than the client's change cursor, fetch {@code GET /changes?afterChange=}.</li>
 * </ul>
 */
public record ConversationResponse(long id, ConversationType type, String title, long lastSequence,
                                   long lastReadSequence, Long lastReceivedSequence, long firstAvailableSequence,
                                   long lastChange, Instant createdAt) {

    public static ConversationResponse from(ConversationEntity conversation) {
        return from(conversation, 0, null);
    }

    public static ConversationResponse from(ConversationEntity conversation, long lastReadSequence,
                                            Long lastReceivedSequence) {
        return new ConversationResponse(conversation.getId(), conversation.getType(), conversation.getTitle(),
                conversation.getLastSequence(), lastReadSequence, lastReceivedSequence,
                conversation.getFirstAvailableSequence(), conversation.getLastChange(), conversation.getCreatedAt());
    }
}

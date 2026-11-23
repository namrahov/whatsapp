package az.whatsapp.dto.response;

import az.whatsapp.entity.ConversationEntity;
import az.whatsapp.entity.ConversationType;

import java.time.Instant;

/** lastSequence lets a reconnecting client see at once which conversations have messages it has not synced. */
public record ConversationResponse(long id, ConversationType type, String title, long lastSequence, Instant createdAt) {

    public static ConversationResponse from(ConversationEntity conversation) {
        return new ConversationResponse(conversation.getId(), conversation.getType(), conversation.getTitle(),
                conversation.getLastSequence(), conversation.getCreatedAt());
    }
}

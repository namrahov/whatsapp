package az.whatsapp.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;

@Embeddable
public record ConversationMemberId(
        @Column(name = "conversation_id") long conversationId,
        @Column(name = "user_id") long userId) implements Serializable {
}

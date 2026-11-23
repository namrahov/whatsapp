package az.whatsapp.entity;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "conversation_members")
public class ConversationMemberEntity {

    @EmbeddedId
    private ConversationMemberId id;

    @Column(name = "joined_at", nullable = false, insertable = false, updatable = false)
    private Instant joinedAt;

    protected ConversationMemberEntity() {
    }

    public ConversationMemberEntity(long conversationId, long userId) {
        this.id = new ConversationMemberId(conversationId, userId);
    }

    public ConversationMemberId getId() {
        return id;
    }

    public Instant getJoinedAt() {
        return joinedAt;
    }
}

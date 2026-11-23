package az.whatsapp.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Entity
@Table(name = "conversations")
public class ConversationEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private ConversationType type;

    @Column(length = 100)
    private String title;

    /** "smallerUserId:largerUserId" for DIRECT conversations, null for groups. */
    @Column(name = "direct_key", length = 41)
    private String directKey;

    /**
     * Read-only for JPA: only {@code ConversationSequenceRepository} changes it, with an atomic UPDATE ... RETURNING.
     * A JPA write of a stale value would hand out a sequence number twice.
     */
    @Column(name = "last_sequence", nullable = false, insertable = false, updatable = false)
    private long lastSequence;

    @Column(name = "created_by", nullable = false, updatable = false)
    private long createdBy;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected ConversationEntity() {
    }

    public static ConversationEntity direct(long createdBy, String directKey) {
        var conversation = new ConversationEntity();
        conversation.type = ConversationType.DIRECT;
        conversation.directKey = directKey;
        conversation.createdBy = createdBy;
        conversation.createdAt = Instant.now().truncatedTo(ChronoUnit.MICROS);
        return conversation;
    }

    public static ConversationEntity group(long createdBy, String title) {
        var conversation = new ConversationEntity();
        conversation.type = ConversationType.GROUP;
        conversation.title = title;
        conversation.createdBy = createdBy;
        conversation.createdAt = Instant.now().truncatedTo(ChronoUnit.MICROS);
        return conversation;
    }

    public Long getId() {
        return id;
    }

    public ConversationType getType() {
        return type;
    }

    public String getTitle() {
        return title;
    }

    public long getLastSequence() {
        return lastSequence;
    }

    public long getCreatedBy() {
        return createdBy;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}

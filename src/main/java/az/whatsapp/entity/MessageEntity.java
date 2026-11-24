package az.whatsapp.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/**
 * One row per logical message, holding ciphertext only. JPA only inserts it: edits and deletes are atomic SQL
 * updates (MessageChangeRepository), so every JPA column is read-only after insert.
 */
@Entity
@Table(name = "messages")
public class MessageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "conversation_id", nullable = false, updatable = false)
    private long conversationId;

    @Column(name = "sender_id", nullable = false, updatable = false)
    private long senderId;

    @Column(name = "client_message_id", nullable = false, updatable = false)
    private UUID clientMessageId;

    @Column(name = "sequence_number", nullable = false, updatable = false)
    private long sequenceNumber;

    /** Null once deleted for everyone. */
    @Column(name = "encrypted_payload", updatable = false)
    private byte[] encryptedPayload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /** 1 when created (the DB default), +1 per edit or delete. */
    @Column(name = "version", nullable = false, insertable = false, updatable = false)
    private int version = 1;

    /** Position in the conversation's change stream; null if never edited or deleted. */
    @Column(name = "change_number", insertable = false, updatable = false)
    private Long changeNumber;

    @Column(name = "edited_at", insertable = false, updatable = false)
    private Instant editedAt;

    @Column(name = "deleted_at", insertable = false, updatable = false)
    private Instant deletedAt;

    protected MessageEntity() {
    }

    public MessageEntity(long conversationId, long senderId, UUID clientMessageId, long sequenceNumber,
                         byte[] encryptedPayload) {
        this.conversationId = conversationId;
        this.senderId = senderId;
        this.clientMessageId = clientMessageId;
        this.sequenceNumber = sequenceNumber;
        this.encryptedPayload = encryptedPayload;
        // Set here, not by the DB default, so the value is known right after insert (for the ACK and the event).
        this.createdAt = Instant.now().truncatedTo(ChronoUnit.MICROS);
    }

    public Long getId() {
        return id;
    }

    public long getConversationId() {
        return conversationId;
    }

    public long getSenderId() {
        return senderId;
    }

    public UUID getClientMessageId() {
        return clientMessageId;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public byte[] getEncryptedPayload() {
        return encryptedPayload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public int getVersion() {
        return version;
    }

    public Long getChangeNumber() {
        return changeNumber;
    }

    public Instant getEditedAt() {
        return editedAt;
    }

    public Instant getDeletedAt() {
        return deletedAt;
    }

    public boolean isDeleted() {
        return deletedAt != null;
    }
}

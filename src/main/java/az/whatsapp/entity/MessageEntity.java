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

/** Immutable once written: one row per logical message, holding ciphertext only. */
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

    @Column(name = "encrypted_payload", nullable = false, updatable = false)
    private byte[] encryptedPayload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

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
}

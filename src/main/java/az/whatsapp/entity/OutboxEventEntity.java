package az.whatsapp.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** Insert-only from JPA; the publisher reads and updates rows with plain SQL (see OutboxQueueRepository). */
@Entity
@Table(name = "outbox")
public class OutboxEventEntity {

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "aggregate_id", nullable = false)
    private long aggregateId;

    @Column(name = "kafka_key", nullable = false, length = 100)
    private String kafkaKey;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload;

    protected OutboxEventEntity() {
    }

    public OutboxEventEntity(UUID eventId, long aggregateId, String kafkaKey, String eventType, String payload) {
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.kafkaKey = kafkaKey;
        this.eventType = eventType;
        this.payload = payload;
    }

    public UUID getEventId() {
        return eventId;
    }
}

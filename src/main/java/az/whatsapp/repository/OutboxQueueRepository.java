package az.whatsapp.repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * The outbox seen as a work queue. Every method is a single auto-committed statement: no lock or connection
 * is held while the publisher talks to Kafka.
 */
@Repository
public class OutboxQueueRepository {

    public record OutboxMessage(UUID eventId, String kafkaKey, String eventType, String payload, Instant createdAt) {
    }

    private final JdbcClient jdbc;

    public OutboxQueueRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Takes up to {@code limit} of the oldest pending, unclaimed events for {@code claimSeconds}.
     * SKIP LOCKED: concurrent publishers (other instances) take different rows instead of waiting.
     * A claim that is never completed (publisher crashed) expires and the rows are taken again.
     *
     * @return claimed events, oldest first
     */
    public List<OutboxMessage> claim(int limit, int claimSeconds) {
        return jdbc.sql("""
                        UPDATE outbox o
                        SET locked_until = now() + make_interval(secs => :claimSeconds)
                        WHERE o.event_id IN (
                            SELECT event_id FROM outbox
                            WHERE status = 'PENDING' AND (locked_until IS NULL OR locked_until < now())
                            ORDER BY created_at, event_id
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED)
                        RETURNING o.event_id, o.kafka_key, o.event_type, o.payload::text AS payload, o.created_at
                        """)
                .param("claimSeconds", claimSeconds)
                .param("limit", limit)
                .query((rs, i) -> new OutboxMessage(
                        rs.getObject("event_id", UUID.class),
                        rs.getString("kafka_key"),
                        rs.getString("event_type"),
                        rs.getString("payload"),
                        rs.getObject("created_at", OffsetDateTime.class).toInstant()))
                .list()
                .stream()
                // RETURNING has no guaranteed order; send in the order the events were written.
                .sorted(Comparator.comparing(OutboxMessage::createdAt).thenComparing(OutboxMessage::eventId))
                .toList();
    }

    public void markPublished(Collection<UUID> eventIds) {
        if (eventIds.isEmpty()) {
            return;
        }
        jdbc.sql("""
                        UPDATE outbox SET status = 'PUBLISHED', published_at = now(), locked_until = NULL
                        WHERE event_id IN (:ids)
                        """)
                .param("ids", eventIds)
                .update();
    }

    /** Gives failed events back to the queue right away (instead of waiting for the claim to expire). */
    public void release(Collection<UUID> eventIds) {
        if (eventIds.isEmpty()) {
            return;
        }
        jdbc.sql("UPDATE outbox SET locked_until = NULL, attempts = attempts + 1 WHERE event_id IN (:ids)")
                .param("ids", eventIds)
                .update();
    }

    /**
     * Deletes at most {@code limit} published events older than {@code retentionDays}. Small batches keep each
     * transaction short; SKIP LOCKED lets two instances run the cleanup at the same time without blocking.
     *
     * @return rows deleted
     */
    public int deletePublishedOlderThan(int retentionDays, int limit) {
        return jdbc.sql("""
                        DELETE FROM outbox
                        WHERE event_id IN (
                            SELECT event_id FROM outbox
                            WHERE status = 'PUBLISHED' AND published_at < now() - make_interval(days => :days)
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED)
                        """)
                .param("days", retentionDays)
                .param("limit", limit)
                .update();
    }
}

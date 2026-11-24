package az.whatsapp.repository;

import java.time.Duration;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class RetentionRepository {

    private final JdbcClient jdbc;

    public RetentionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Deletes at most {@code limit} of the oldest messages older than {@code ttl}, and in the same statement raises
     * each affected conversation's first_available_sequence past the highest deleted sequence. SKIP LOCKED: two
     * instances running the job take different rows instead of waiting for each other.
     * <p>
     * Within a conversation, created_at grows with the sequence (both are assigned under the conversation row
     * lock), so deleting by age removes a conversation's messages from the oldest sequence up.
     * <p>
     * Expiry is not delivery: delivery and read cursors are not touched.
     *
     * @return messages deleted
     */
    public int deleteExpiredMessages(Duration ttl, int limit) {
        return jdbc.sql("""
                        WITH doomed AS (
                            SELECT id FROM messages
                            WHERE created_at < now() - make_interval(secs => :ttlSeconds)
                            ORDER BY created_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED),
                        deleted AS (
                            DELETE FROM messages m USING doomed d WHERE m.id = d.id
                            RETURNING m.conversation_id, m.sequence_number),
                        moved AS (
                            UPDATE conversations c
                            SET first_available_sequence = GREATEST(c.first_available_sequence, x.max_sequence + 1)
                            FROM (SELECT conversation_id, max(sequence_number) AS max_sequence
                                  FROM deleted GROUP BY conversation_id) x
                            WHERE c.id = x.conversation_id
                            RETURNING c.id)
                        SELECT count(*) FROM deleted
                        """)
                .param("ttlSeconds", ttl.toSeconds())
                .param("limit", limit)
                .query(Integer.class)
                .single();
    }
}

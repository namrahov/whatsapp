package az.whatsapp.repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Edits and deletes as atomic SQL updates. Both run in a transaction that first took the conversation's next
 * change number, which locks the conversation row: changes to one conversation are serialised, and their change
 * numbers come out in the order they commit (like message sequences).
 */
@Repository
public class MessageChangeRepository {

    private final JdbcClient jdbc;

    public MessageChangeRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Next position in the conversation's change stream. Locks the conversation row until commit. */
    public long nextChange(long conversationId) {
        return jdbc.sql("UPDATE conversations SET last_change = last_change + 1 WHERE id = :id RETURNING last_change")
                .param("id", conversationId)
                .query(Long.class)
                .single();
    }

    /**
     * Optimistic: applies only if the message is still at {@code expectedVersion} and not deleted.
     *
     * @return false if someone changed it in between
     */
    public boolean edit(long messageId, int expectedVersion, byte[] payload, long changeNumber, Instant at) {
        return jdbc.sql("""
                        UPDATE messages
                        SET encrypted_payload = :payload, version = version + 1, change_number = :change, edited_at = :at
                        WHERE id = :id AND version = :version AND deleted_at IS NULL
                        """)
                .param("payload", payload)
                .param("change", changeNumber)
                .param("at", OffsetDateTime.ofInstant(at, ZoneOffset.UTC))
                .param("id", messageId)
                .param("version", expectedVersion)
                .update() == 1;
    }

    /** Leaves a tombstone: the row (and its sequence) stays, the ciphertext goes. */
    public boolean delete(long messageId, int expectedVersion, long changeNumber, Instant at) {
        return jdbc.sql("""
                        UPDATE messages
                        SET encrypted_payload = NULL, version = version + 1, change_number = :change, deleted_at = :at
                        WHERE id = :id AND version = :version AND deleted_at IS NULL
                        """)
                .param("change", changeNumber)
                .param("at", OffsetDateTime.ofInstant(at, ZoneOffset.UTC))
                .param("id", messageId)
                .param("version", expectedVersion)
                .update() == 1;
    }
}

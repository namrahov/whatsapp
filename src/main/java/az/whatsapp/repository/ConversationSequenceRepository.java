package az.whatsapp.repository;

import java.util.OptionalLong;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Hands out the next position in a conversation. The UPDATE locks the conversation row until the caller's
 * transaction ends, so concurrent senders to the same conversation are serialised and each gets a distinct,
 * gap-free number; if the transaction rolls back, so does the increment.
 */
@Repository
public class ConversationSequenceRepository {

    private final JdbcClient jdbc;

    public ConversationSequenceRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Must run inside the transaction that inserts the message. Empty if the conversation does not exist. */
    public OptionalLong next(long conversationId) {
        return jdbc.sql("""
                        UPDATE conversations SET last_sequence = last_sequence + 1
                        WHERE id = :id
                        RETURNING last_sequence
                        """)
                .param("id", conversationId)
                .query(Long.class)
                .optional()
                .map(OptionalLong::of)
                .orElse(OptionalLong.empty());
    }
}

package az.whatsapp.repository;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * DELIVERED / READ receipts. Each statement checks in SQL that the user is a member of the message's
 * conversation and not its sender, so a client cannot write receipts for messages it never received.
 * Both are idempotent: a repeated ACK changes nothing.
 */
@Repository
public class ReceiptRepository {

    private final JdbcClient jdbc;

    public ReceiptRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** @return false if the message does not exist or the user may not acknowledge it */
    public boolean markDelivered(long messageId, long userId) {
        int rows = jdbc.sql("""
                        INSERT INTO message_receipts (message_id, user_id, delivered_at)
                        SELECT m.id, :userId, now()
                        FROM messages m
                        JOIN conversation_members cm ON cm.conversation_id = m.conversation_id AND cm.user_id = :userId
                        WHERE m.id = :messageId AND m.sender_id <> :userId
                        ON CONFLICT (message_id, user_id) DO NOTHING
                        """)
                .param("messageId", messageId)
                .param("userId", userId)
                .update();
        return rows > 0 || exists(messageId, userId);
    }

    /** Read implies delivered: a READ that arrives without an earlier ACK fills both. */
    public boolean markRead(long messageId, long userId) {
        int rows = jdbc.sql("""
                        INSERT INTO message_receipts (message_id, user_id, delivered_at, read_at)
                        SELECT m.id, :userId, now(), now()
                        FROM messages m
                        JOIN conversation_members cm ON cm.conversation_id = m.conversation_id AND cm.user_id = :userId
                        WHERE m.id = :messageId AND m.sender_id <> :userId
                        ON CONFLICT (message_id, user_id)
                            DO UPDATE SET read_at = COALESCE(message_receipts.read_at, EXCLUDED.read_at)
                        """)
                .param("messageId", messageId)
                .param("userId", userId)
                .update();
        return rows > 0;
    }

    private boolean exists(long messageId, long userId) {
        return jdbc.sql("SELECT EXISTS (SELECT 1 FROM message_receipts WHERE message_id = :m AND user_id = :u)")
                .param("m", messageId)
                .param("u", userId)
                .query(Boolean.class)
                .single();
    }
}

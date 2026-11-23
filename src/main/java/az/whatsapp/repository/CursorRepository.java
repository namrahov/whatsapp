package az.whatsapp.repository;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Delivery and read cursors. Both only move forward (GREATEST semantics): an old or repeated ACK is a no-op.
 * A cursor never goes past the conversation's last_sequence, so a client cannot acknowledge messages that do
 * not exist yet. Callers check membership first; the foreign keys to conversation_members back that up.
 * <p>
 * "Delivered to a user" = the furthest of the user's devices, or the read cursor if that is further
 * (read implies delivered; a user's own messages count as read by them).
 */
@Repository
public class CursorRepository {

    public record MemberCursors(long userId, long deliveredUpTo, long readUpTo) {
    }

    /** Per member: furthest device cursor, raised to the read cursor. */
    private static final String MEMBER_CURSORS = """
            SELECT cm.user_id,
                   GREATEST(COALESCE(d.max_received, 0), COALESCE(r.last_read_sequence, 0)) AS delivered_up_to,
                   COALESCE(r.last_read_sequence, 0) AS read_up_to
            FROM conversation_members cm
            LEFT JOIN (SELECT user_id, max(last_received_sequence) AS max_received
                       FROM device_sync WHERE conversation_id = :c GROUP BY user_id) d ON d.user_id = cm.user_id
            LEFT JOIN conversation_read_state r ON r.conversation_id = cm.conversation_id AND r.user_id = cm.user_id
            WHERE cm.conversation_id = :c
            """;

    private final JdbcClient jdbc;

    public CursorRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Moves this device's cursor forward to {@code upToSequence} (capped at the last sequence). */
    public void advanceDevice(long userId, String deviceId, long conversationId, long upToSequence) {
        jdbc.sql("""
                        INSERT INTO device_sync (user_id, device_id, conversation_id, last_received_sequence)
                        SELECT :u, :d, c.id, LEAST(:seq, c.last_sequence) FROM conversations c WHERE c.id = :c
                        ON CONFLICT (user_id, device_id, conversation_id) DO UPDATE
                            SET last_received_sequence = EXCLUDED.last_received_sequence, updated_at = now()
                            WHERE EXCLUDED.last_received_sequence > device_sync.last_received_sequence
                        """)
                .param("u", userId)
                .param("d", deviceId)
                .param("c", conversationId)
                .param("seq", upToSequence)
                .update();
    }

    /**
     * Moves the user's read cursor forward.
     *
     * @return the new cursor if it moved, empty if it was already there (or further)
     */
    public OptionalLong advanceRead(long userId, long conversationId, long upToSequence) {
        return jdbc.sql("""
                        INSERT INTO conversation_read_state (conversation_id, user_id, last_read_sequence)
                        SELECT c.id, :u, LEAST(:seq, c.last_sequence) FROM conversations c WHERE c.id = :c
                        ON CONFLICT (conversation_id, user_id) DO UPDATE
                            SET last_read_sequence = EXCLUDED.last_read_sequence, updated_at = now()
                            WHERE EXCLUDED.last_read_sequence > conversation_read_state.last_read_sequence
                        RETURNING last_read_sequence
                        """)
                .param("u", userId)
                .param("c", conversationId)
                .param("seq", upToSequence)
                .query(Long.class)
                .optional()
                .filter(sequence -> sequence > 0)
                .map(OptionalLong::of)
                .orElse(OptionalLong.empty());
    }

    public long deliveredTo(long userId, long conversationId) {
        return jdbc.sql("""
                        SELECT GREATEST(
                            (SELECT COALESCE(max(last_received_sequence), 0) FROM device_sync
                             WHERE user_id = :u AND conversation_id = :c),
                            (SELECT COALESCE(max(last_read_sequence), 0) FROM conversation_read_state
                             WHERE user_id = :u AND conversation_id = :c))
                        """)
                .param("u", userId)
                .param("c", conversationId)
                .query(Long.class)
                .single();
    }

    /** Every member's cursors. A group's "delivered/read by all" is the minimum over these. */
    public List<MemberCursors> memberCursors(long conversationId) {
        return jdbc.sql(MEMBER_CURSORS)
                .param("c", conversationId)
                .query((rs, i) -> new MemberCursors(rs.getLong("user_id"), rs.getLong("delivered_up_to"),
                        rs.getLong("read_up_to")))
                .list();
    }

    public long deliveredToAll(long conversationId) {
        return jdbc.sql("SELECT COALESCE(min(delivered_up_to), 0) FROM (" + MEMBER_CURSORS + ") m")
                .param("c", conversationId)
                .query(Long.class)
                .single();
    }

    public long readByAll(long conversationId) {
        return jdbc.sql("SELECT COALESCE(min(read_up_to), 0) FROM (" + MEMBER_CURSORS + ") m")
                .param("c", conversationId)
                .query(Long.class)
                .single();
    }

    /** conversationId → the user's read cursor, for unread counts. Conversations never read are absent. */
    public Map<Long, Long> readCursorsOf(long userId) {
        return jdbc.sql("SELECT conversation_id, last_read_sequence FROM conversation_read_state WHERE user_id = :u")
                .param("u", userId)
                .query((rs, i) -> Map.entry(rs.getLong(1), rs.getLong(2)))
                .list()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** conversationId → this device's delivery cursor: where its reconnect sync starts. */
    public Map<Long, Long> deviceCursorsOf(long userId, String deviceId) {
        return jdbc.sql("""
                        SELECT conversation_id, last_received_sequence FROM device_sync
                        WHERE user_id = :u AND device_id = :d
                        """)
                .param("u", userId)
                .param("d", deviceId)
                .query((rs, i) -> Map.entry(rs.getLong(1), rs.getLong(2)))
                .list()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}

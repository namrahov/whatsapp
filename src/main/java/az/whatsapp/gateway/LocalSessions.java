package az.whatsapp.gateway;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

/**
 * The WebSocket connections open on this instance, by connection id, with the time each was last heard from.
 * That "last seen" table lives only in this gateway's memory: a heartbeat costs no Redis write (PDF section 09).
 */
@Component
public class LocalSessions {

    private static final Logger log = LoggerFactory.getLogger(LocalSessions.class);

    public static final class LocalConnection {

        private final long userId;
        private final WebSocketSession session;
        private volatile long lastSeenNanos = System.nanoTime();

        LocalConnection(long userId, WebSocketSession session) {
            this.userId = userId;
            this.session = session;
        }

        public long userId() {
            return userId;
        }

        public WebSocketSession session() {
            return session;
        }
    }

    private final Map<String, LocalConnection> connections = new ConcurrentHashMap<>();

    /** @param session must be safe for concurrent sends (wrapped in ConcurrentWebSocketSessionDecorator) */
    public void add(String connectionId, long userId, WebSocketSession session) {
        connections.put(connectionId, new LocalConnection(userId, session));
    }

    public void remove(String connectionId) {
        connections.remove(connectionId);
    }

    /** Any frame from the client (a heartbeat or real traffic) proves the connection is alive. */
    public void touch(String connectionId) {
        LocalConnection connection = connections.get(connectionId);
        if (connection != null) {
            connection.lastSeenNanos = System.nanoTime();
        }
    }

    /**
     * Writing to the socket only hands the frame to the network: it is not "delivered". Delivery is confirmed
     * by the client's ACK after it has stored the message.
     *
     * @return false if the connection is not (or no longer) open on this instance
     */
    public boolean send(String connectionId, String frame) {
        LocalConnection connection = connections.get(connectionId);
        if (connection == null || !connection.session().isOpen()) {
            return false;
        }
        try {
            connection.session().sendMessage(new TextMessage(frame));
            return true;
        } catch (Exception e) {
            // Slow client over its buffer limit, or a broken socket. Close it: the client reconnects and syncs
            // from its last sequence, instead of this instance buffering for it without bound.
            log.debug("Closing connection {} after failed send: {}", connectionId, e.toString());
            close(connection.session(), CloseStatus.SESSION_NOT_RELIABLE);
            return false;
        }
    }

    /** Closes connections not heard from for {@code idleTimeout}; their close handler unregisters them. */
    public int closeIdle(Duration idleTimeout) {
        long now = System.nanoTime();
        List<LocalConnection> idle = connections.values().stream()
                .filter(c -> now - c.lastSeenNanos > idleTimeout.toNanos())
                .toList();
        idle.forEach(c -> close(c.session(), CloseStatus.SESSION_NOT_RELIABLE.withReason("heartbeat timeout")));
        return idle.size();
    }

    /** @return connectionId → userId of the connections open right now */
    public Map<String, Long> snapshot() {
        return connections.entrySet().stream()
                .filter(entry -> entry.getValue().session().isOpen())
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().userId()));
    }

    public int size() {
        return connections.size();
    }

    public boolean hasUser(long userId) {
        return connections.values().stream().anyMatch(c -> c.userId() == userId);
    }

    private static void close(WebSocketSession session, CloseStatus status) {
        try {
            session.close(status);
        } catch (Exception ignored) {
            // already closed
        }
    }
}

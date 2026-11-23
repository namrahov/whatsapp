package az.whatsapp.gateway;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

/** The WebSocket connections open on this instance, by connection id. */
@Component
public class LocalSessions {

    private static final Logger log = LoggerFactory.getLogger(LocalSessions.class);

    public record LocalConnection(long userId, WebSocketSession session) {
    }

    private final Map<String, LocalConnection> connections = new ConcurrentHashMap<>();

    /** @param session must be safe for concurrent sends (wrapped in ConcurrentWebSocketSessionDecorator) */
    public void add(String connectionId, long userId, WebSocketSession session) {
        connections.put(connectionId, new LocalConnection(userId, session));
    }

    public void remove(String connectionId) {
        connections.remove(connectionId);
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
            close(connection.session());
            return false;
        }
    }

    /** @return connectionId → userId of the connections open right now */
    public Map<String, Long> snapshot() {
        return connections.entrySet().stream()
                .filter(entry -> entry.getValue().session().isOpen())
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().userId()));
    }

    private static void close(WebSocketSession session) {
        try {
            session.close(CloseStatus.SESSION_NOT_RELIABLE);
        } catch (Exception ignored) {
            // already closed
        }
    }
}

package az.whatsapp.gateway;

import az.whatsapp.gateway.Frames.ClientFrame;
import az.whatsapp.service.MessageService;
import az.whatsapp.service.ReceiptService;
import az.whatsapp.service.RetryableErrors;

import java.util.Base64;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * One WebSocket per client connection. On connect the connection is put in the local table and in the Redis
 * registry, so any instance can route to it. Incoming SEND frames go through the same {@link MessageService}
 * as the REST API: the SENT reply comes only after the message is durably stored.
 */
@Component
public class ChatWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(ChatWebSocketHandler.class);

    /** A client that cannot take frames for this long, or falls this far behind, is disconnected (bounded queue). */
    private static final int SEND_TIME_LIMIT_MS = 5_000;
    private static final int SEND_BUFFER_LIMIT_BYTES = 512 * 1024;

    private final LocalSessions localSessions;
    private final ConnectionRegistry registry;
    private final GatewayInstance gateway;
    private final MessageService messages;
    private final ReceiptService receipts;
    private final JsonMapper json;

    public ChatWebSocketHandler(LocalSessions localSessions, ConnectionRegistry registry, GatewayInstance gateway,
                                MessageService messages, ReceiptService receipts, JsonMapper json) {
        this.localSessions = localSessions;
        this.registry = registry;
        this.gateway = gateway;
        this.messages = messages;
        this.receipts = receipts;
        this.json = json;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        long userId = userId(session);
        // Several threads write to one socket (replies, routed messages): the decorator serialises the sends
        // and bounds what may queue up for a slow client.
        var safeSession = new ConcurrentWebSocketSessionDecorator(session, SEND_TIME_LIMIT_MS, SEND_BUFFER_LIMIT_BYTES,
                ConcurrentWebSocketSessionDecorator.OverflowStrategy.TERMINATE);
        localSessions.add(session.getId(), userId, safeSession);
        try {
            registry.register(userId, session.getId(), gateway.id());
        } catch (RuntimeException e) {
            // Unreachable for routing = not really online. Let the client reconnect (with backoff) instead.
            log.warn("Registry unavailable, closing connection of user {}: {}", userId, e.toString());
            localSessions.remove(session.getId());
            session.close(CloseStatus.SERVICE_RESTARTED);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        localSessions.remove(session.getId());
        try {
            registry.unregister(userId(session), List.of(session.getId()));
        } catch (RuntimeException e) {
            // The entry is dropped later: on a failed local delivery or when its TTL runs out.
            log.debug("Could not unregister connection {}: {}", session.getId(), e.toString());
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        ClientFrame frame;
        try {
            frame = json.readValue(message.getPayload(), ClientFrame.class);
        } catch (JacksonException e) {
            reply(session, Frames.Error.of(null, 400, "Malformed frame", false));
            return;
        }
        if (frame.type() == null) {
            reply(session, Frames.Error.of(frame, 400, "type is required", false));
            return;
        }
        long userId = userId(session);
        try {
            switch (frame.type()) {
                case "SEND" -> handleSend(session, userId, frame);
                case "ACK" -> receipts.markDelivered(userId, requireMessageId(frame));
                case "READ" -> receipts.markRead(userId, requireMessageId(frame));
                default -> reply(session, Frames.Error.of(frame, 400, "Unknown type " + frame.type(), false));
            }
        } catch (ResponseStatusException e) {
            int status = e.getStatusCode().value();
            reply(session, Frames.Error.of(frame, status, e.getReason(), status >= 500));
        } catch (RuntimeException e) {
            if (RetryableErrors.isRetryable(e)) {
                reply(session, Frames.Error.of(frame, 503, "Storage temporarily unavailable, not accepted", true));
            } else {
                log.error("Failed to handle {} frame of user {}", frame.type(), userId, e);
                reply(session, Frames.Error.of(frame, 500, "Internal error", false));
            }
        }
    }

    private void handleSend(WebSocketSession session, long userId, ClientFrame frame) {
        if (frame.conversationId() == null || frame.clientMessageId() == null || frame.payload() == null) {
            reply(session, Frames.Error.of(frame, 400, "conversationId, clientMessageId and payload are required",
                    false));
            return;
        }
        byte[] payload;
        try {
            payload = Base64.getDecoder().decode(frame.payload());
        } catch (IllegalArgumentException e) {
            reply(session, Frames.Error.of(frame, 400, "payload must be Base64", false));
            return;
        }
        var result = messages.send(userId, frame.conversationId(), frame.clientMessageId(), payload);
        reply(session, Frames.Sent.of(result.message(), result.duplicate()));
    }

    private static long requireMessageId(ClientFrame frame) {
        if (frame.messageId() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "messageId is required");
        }
        return frame.messageId();
    }

    private void reply(WebSocketSession session, Object frame) {
        localSessions.send(session.getId(), json.writeValueAsString(frame));
    }

    private static long userId(WebSocketSession session) {
        return (Long) session.getAttributes().get(UserIdHandshakeInterceptor.USER_ID_ATTRIBUTE);
    }
}

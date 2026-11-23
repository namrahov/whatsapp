package az.whatsapp.gateway;

import az.whatsapp.security.CurrentUserProvider;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

/**
 * Authenticates the WebSocket upgrade request with the same {@link CurrentUserProvider} as the REST API
 * (the X-User-Id header). A request that fails gets 401 and no socket is opened.
 */
@Component
public class UserIdHandshakeInterceptor implements HandshakeInterceptor {

    public static final String USER_ID_ATTRIBUTE = "userId";

    private final CurrentUserProvider currentUserProvider;

    public UserIdHandshakeInterceptor(CurrentUserProvider currentUserProvider) {
        this.currentUserProvider = currentUserProvider;
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                   WebSocketHandler wsHandler, Map<String, Object> attributes) {
        if (!(request instanceof ServletServerHttpRequest servletRequest)) {
            response.setStatusCode(HttpStatus.BAD_REQUEST);
            return false;
        }
        try {
            attributes.put(USER_ID_ATTRIBUTE, currentUserProvider.requireUserId(servletRequest.getServletRequest()));
            return true;
        } catch (ResponseStatusException e) {
            response.setStatusCode(e.getStatusCode());
            return false;
        }
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler,
                               Exception exception) {
    }
}

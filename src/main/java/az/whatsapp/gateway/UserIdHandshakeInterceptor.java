package az.whatsapp.gateway;

import az.whatsapp.security.CurrentDeviceProvider;
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
 * Authenticates the WebSocket upgrade request with the same providers as the REST API: X-User-Id (401 if
 * missing or invalid) and X-Device-Id (400). A request that fails gets no socket.
 */
@Component
public class UserIdHandshakeInterceptor implements HandshakeInterceptor {

    public static final String USER_ID_ATTRIBUTE = "userId";
    public static final String DEVICE_ID_ATTRIBUTE = "deviceId";

    private final CurrentUserProvider currentUserProvider;
    private final CurrentDeviceProvider currentDeviceProvider;

    public UserIdHandshakeInterceptor(CurrentUserProvider currentUserProvider,
                                      CurrentDeviceProvider currentDeviceProvider) {
        this.currentUserProvider = currentUserProvider;
        this.currentDeviceProvider = currentDeviceProvider;
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                   WebSocketHandler wsHandler, Map<String, Object> attributes) {
        if (!(request instanceof ServletServerHttpRequest servletRequest)) {
            response.setStatusCode(HttpStatus.BAD_REQUEST);
            return false;
        }
        try {
            var servlet = servletRequest.getServletRequest();
            attributes.put(USER_ID_ATTRIBUTE, currentUserProvider.requireUserId(servlet));
            // A socket is one device: its ACKs move that device's delivery cursor.
            attributes.put(DEVICE_ID_ATTRIBUTE, currentDeviceProvider.requireDeviceId(servlet));
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

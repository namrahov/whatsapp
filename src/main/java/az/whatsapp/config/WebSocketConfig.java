package az.whatsapp.config;

import az.whatsapp.gateway.ChatWebSocketHandler;
import az.whatsapp.gateway.UserIdHandshakeInterceptor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

/**
 * Plain WebSocket at {@code /ws}, not STOMP: STOMP's simple broker lives in one instance's memory and would hide
 * exactly the cross-instance routing this project is about.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    /** 64 KB ciphertext as Base64 plus the JSON around it. */
    private static final int MAX_TEXT_MESSAGE_BYTES = 128 * 1024;

    private final ChatWebSocketHandler handler;
    private final UserIdHandshakeInterceptor handshakeInterceptor;

    public WebSocketConfig(ChatWebSocketHandler handler, UserIdHandshakeInterceptor handshakeInterceptor) {
        this.handler = handler;
        this.handshakeInterceptor = handshakeInterceptor;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, "/ws").addInterceptors(handshakeInterceptor);
    }

    @Bean
    public ServletServerContainerFactoryBean webSocketContainer() {
        var container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(MAX_TEXT_MESSAGE_BYTES);
        return container;
    }
}

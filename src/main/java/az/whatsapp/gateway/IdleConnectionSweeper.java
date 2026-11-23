package az.whatsapp.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Closes half-open connections: a phone that lost network does not close its socket, the server just stops
 * hearing from it. Without heartbeats it would stay "online" and in the registry until TCP gives up (minutes).
 */
@Component
public class IdleConnectionSweeper {

    private static final Logger log = LoggerFactory.getLogger(IdleConnectionSweeper.class);

    private final LocalSessions sessions;
    private final GatewayProperties properties;

    public IdleConnectionSweeper(LocalSessions sessions, GatewayProperties properties) {
        this.sessions = sessions;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${whatsapp.gateway.idle-sweep-interval-ms:10000}")
    public void sweep() {
        int closed = sessions.closeIdle(properties.idleTimeout());
        if (closed > 0) {
            log.info("Closed {} connection(s) silent for more than {}", closed, properties.idleTimeout());
        }
    }
}

package az.whatsapp.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Keeps this instance's users in the registry while their connections are open. One pipelined batch per
 * interval, not one Redis write per client heartbeat.
 */
@Component
public class RegistryRefresher {

    private static final Logger log = LoggerFactory.getLogger(RegistryRefresher.class);

    private final LocalSessions sessions;
    private final ConnectionRegistry registry;
    private final GatewayInstance gateway;

    public RegistryRefresher(LocalSessions sessions, ConnectionRegistry registry, GatewayInstance gateway) {
        this.sessions = sessions;
        this.registry = registry;
        this.gateway = gateway;
    }

    @Scheduled(fixedDelayString = "${whatsapp.registry.refresh-interval-ms:30000}")
    public void refresh() {
        try {
            registry.refresh(sessions.snapshot(), gateway.id());
        } catch (RuntimeException e) {
            log.warn("Registry refresh failed, will retry: {}", e.toString());
        }
    }
}

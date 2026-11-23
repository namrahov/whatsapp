package az.whatsapp.gateway;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Identity of this running WebSocket gateway. New on every start: entries a crashed process left in the
 * connection registry point to an id nobody listens on any more, which is how they are recognised as stale.
 */
@Component
public class GatewayInstance {

    private static final Logger log = LoggerFactory.getLogger(GatewayInstance.class);
    private static final String CHANNEL_PREFIX = "ws:instance:";

    private final String id = UUID.randomUUID().toString();

    public GatewayInstance() {
        log.info("Gateway instance id {}", id);
    }

    public String id() {
        return id;
    }

    /** Redis Pub/Sub channel this instance listens on. */
    public String channel() {
        return channelOf(id);
    }

    public static String channelOf(String instanceId) {
        return CHANNEL_PREFIX + instanceId;
    }
}

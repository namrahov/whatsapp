package az.whatsapp.gateway;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Pushes a frame to every open connection of a user, wherever it is: directly if the connection is on this
 * instance, otherwise through the owning instance's Redis Pub/Sub channel.
 * <p>
 * This is a best-effort notification path, not storage. Pub/Sub keeps nothing: if the target instance is down
 * the frame is lost, which is fine because the message is already in PostgreSQL and the client fetches it on
 * reconnect ({@code GET /messages?afterSequence=}).
 */
@Component
public class DeliveryRouter {

    private static final Logger log = LoggerFactory.getLogger(DeliveryRouter.class);

    private final ConnectionRegistry registry;
    private final LocalSessions localSessions;
    private final GatewayInstance gateway;
    private final StringRedisTemplate redis;
    private final JsonMapper json;

    public DeliveryRouter(ConnectionRegistry registry, LocalSessions localSessions, GatewayInstance gateway,
                          StringRedisTemplate redis, JsonMapper json) {
        this.registry = registry;
        this.localSessions = localSessions;
        this.gateway = gateway;
        this.redis = redis;
        this.json = json;
    }

    /** @return number of connections the frame was handed to (0 = user offline, stays in DB) */
    public int deliver(long userId, String frame) {
        Map<String, String> connections = registry.connectionsOf(userId);
        if (connections.isEmpty()) {
            return 0;
        }
        Map<String, List<String>> byInstance = connections.entrySet().stream()
                .collect(Collectors.groupingBy(Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

        int handedOver = 0;
        for (var target : byInstance.entrySet()) {
            String instanceId = target.getKey();
            List<String> connectionIds = target.getValue();
            if (instanceId.equals(gateway.id())) {
                handedOver += deliverLocally(userId, connectionIds, frame);
            } else {
                handedOver += publish(userId, instanceId, connectionIds, frame);
            }
        }
        return handedOver;
    }

    /** Called for frames that arrive on this instance's channel, and for local targets. */
    int deliverLocally(long userId, List<String> connectionIds, String frame) {
        List<String> gone = connectionIds.stream().filter(id -> !localSessions.send(id, frame)).toList();
        if (!gone.isEmpty()) {
            // Registered here, but no longer open here: a disconnect whose unregister did not reach Redis.
            registry.unregister(userId, gone);
        }
        return connectionIds.size() - gone.size();
    }

    private int publish(long userId, String instanceId, List<String> connectionIds, String frame) {
        String envelope = json.writeValueAsString(new ChannelEnvelope(userId, connectionIds, frame));
        Long receivers = redis.convertAndSend(GatewayInstance.channelOf(instanceId), envelope);
        if (receivers == null || receivers == 0) {
            // Nobody listens on that channel: the instance is dead (or restarted with a new id).
            // Its registry entries are stale; drop them so the next lookup does not try again.
            log.debug("Instance {} is gone, dropping {} stale connection(s) of user {}",
                    instanceId, connectionIds.size(), userId);
            registry.unregister(userId, connectionIds);
            return 0;
        }
        return connectionIds.size();
    }
}

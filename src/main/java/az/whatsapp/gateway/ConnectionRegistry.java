package az.whatsapp.gateway;

import java.util.Collection;
import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

/**
 * Where is a user connected? One Redis hash per user: {@code ws:user:<userId>} with
 * field = connectionId, value = gateway instance id. A user may have several connections (tabs, devices).
 * <p>
 * The connection id is the field, so removing one connection can never remove a newer one: after a reconnect
 * to another instance the user has a new connection id, and the old instance's late "disconnected" deletes only
 * its own field. The key has a TTL that the owning instances keep refreshing; if an instance crashes, its
 * entries are dropped when the router finds nobody listening on its channel, or at the latest when the TTL runs out.
 */
@Repository
public class ConnectionRegistry {

    private static final String KEY_PREFIX = "ws:user:";

    private final StringRedisTemplate redis;
    private final RegistryProperties properties;

    public ConnectionRegistry(StringRedisTemplate redis, RegistryProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    public void register(long userId, String connectionId, String instanceId) {
        String key = key(userId);
        redis.opsForHash().put(key, connectionId, instanceId);
        redis.expire(key, properties.ttl());
    }

    public void unregister(long userId, Collection<String> connectionIds) {
        if (!connectionIds.isEmpty()) {
            redis.opsForHash().delete(key(userId), connectionIds.toArray());
        }
    }

    /** @return connectionId → gateway instance id; empty when the user is offline */
    public Map<String, String> connectionsOf(long userId) {
        return redis.<String, String>opsForHash().entries(key(userId));
    }

    /**
     * Re-writes this instance's connections and extends their TTL, in one round trip (pipelined). Writing the
     * fields again, not only EXPIRE, puts users back after Redis lost the keys (restart, eviction).
     *
     * @param connections connectionId → userId of the connections open on {@code instanceId}
     */
    public void refresh(Map<String, Long> connections, String instanceId) {
        if (connections.isEmpty()) {
            return;
        }
        long ttlSeconds = properties.ttl().toSeconds();
        redis.executePipelined((RedisCallback<Object>) connection -> {
            var strings = (StringRedisConnection) connection;
            connections.forEach((connectionId, userId) -> {
                strings.hSet(key(userId), connectionId, instanceId);
                strings.expire(key(userId), ttlSeconds);
            });
            return null;
        });
    }

    private static String key(long userId) {
        return KEY_PREFIX + userId;
    }
}

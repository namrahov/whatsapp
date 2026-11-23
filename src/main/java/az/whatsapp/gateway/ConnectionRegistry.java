package az.whatsapp.gateway;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

/**
 * Where is a user connected? One Redis hash per user: {@code ws:user:<userId>} with
 * field = connectionId, value = gateway instance id. A user may have several connections (tabs, devices).
 * <p>
 * The connection id is the field, so removing one connection can never remove a newer one: after a reconnect
 * to another instance the user has a new connection id, and the old instance's late "disconnected" deletes only
 * its own field.
 * <p>
 * Whether an entry is live is decided by its gateway's lease ({@link InstanceLease}), not by a per-connection TTL:
 * one lease write per gateway replaces one write per connection (PDF section 09). Entries of a dead gateway are
 * removed lazily, when a lookup or the router meets them; the key TTL only cleans up keys nobody looks at.
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

    /** @return connectionId → gateway instance id, as registered (may include entries of a dead gateway) */
    public Map<String, String> connectionsOf(long userId) {
        return redis.<String, String>opsForHash().entries(key(userId));
    }

    /**
     * The user's connections on gateways whose lease is alive. Entries of dead gateways are removed on the way
     * (reconciliation of stale registry entries).
     */
    public Map<String, String> liveConnectionsOf(long userId) {
        Map<String, String> all = connectionsOf(userId);
        if (all.isEmpty()) {
            return all;
        }
        Set<String> alive = aliveInstances(new HashSet<>(all.values()));
        Map<String, String> live = new HashMap<>();
        List<String> stale = new ArrayList<>();
        all.forEach((connectionId, instanceId) -> {
            if (alive.contains(instanceId)) {
                live.put(connectionId, instanceId);
            } else {
                stale.add(connectionId);
            }
        });
        unregister(userId, stale);
        return live;
    }

    /** Which of the users have at least one connection on a live gateway. Two round trips for any number. */
    public Set<Long> onlineAmong(Collection<Long> userIds) {
        if (userIds.isEmpty()) {
            return Set.of();
        }
        List<Long> ids = List.copyOf(userIds);
        List<Object> hashes = RedisPipeline.run(redis, ops -> ids.forEach(id -> ops.opsForHash().entries(key(id))));
        Set<String> instances = new HashSet<>();
        hashes.forEach(hash -> instances.addAll(asStringMap(hash).values()));
        Set<String> alive = aliveInstances(instances);
        Set<Long> online = new HashSet<>();
        for (int i = 0; i < ids.size(); i++) {
            if (asStringMap(hashes.get(i)).values().stream().anyMatch(alive::contains)) {
                online.add(ids.get(i));
            }
        }
        return online;
    }

    /** One MGET over the gateways' lease keys. */
    public Set<String> aliveInstances(Set<String> instanceIds) {
        if (instanceIds.isEmpty()) {
            return Set.of();
        }
        List<String> ids = List.copyOf(instanceIds);
        List<String> leases = redis.opsForValue().multiGet(ids.stream().map(GatewayInstance::leaseKeyOf).toList());
        Set<String> alive = new HashSet<>();
        for (int i = 0; i < ids.size(); i++) {
            if (leases != null && leases.get(i) != null) {
                alive.add(ids.get(i));
            }
        }
        return alive;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> asStringMap(Object hash) {
        return hash instanceof Map<?, ?> map ? (Map<String, String>) map : Map.of();
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
        RedisPipeline.run(redis, ops -> connections.forEach((connectionId, userId) -> {
            ops.opsForHash().put(key(userId), connectionId, instanceId);
            ops.expire(key(userId), properties.ttl());
        }));
    }

    private static String key(long userId) {
        return KEY_PREFIX + userId;
    }
}

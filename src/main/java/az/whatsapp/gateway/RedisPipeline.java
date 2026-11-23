package az.whatsapp.gateway;

import java.util.List;
import java.util.function.Consumer;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Runs several Redis commands in one round trip. Uses a SessionCallback over the template's own (string)
 * operations: the low-level connection handed to a RedisCallback is a proxy and cannot be cast to
 * StringRedisConnection.
 */
final class RedisPipeline {

    private RedisPipeline() {
    }

    /** @return one result per command, in order (deserialised: Boolean, Map, String, ...) */
    static List<Object> run(StringRedisTemplate redis, Consumer<RedisOperations<String, String>> commands) {
        return redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> operations) throws DataAccessException {
                commands.accept((RedisOperations<String, String>) operations);
                return null;
            }
        });
    }
}

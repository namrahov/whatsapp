package az.whatsapp.service;

import az.whatsapp.dto.response.PresenceResponse;
import az.whatsapp.gateway.ConnectionRegistry;
import az.whatsapp.repository.ConversationMemberRepository;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Online / last seen (PDF sections 08–09). Ephemeral state, so Redis, never PostgreSQL.
 * <ul>
 *   <li>Online = at least one connection on a gateway whose lease is alive (multi-device: any device).</li>
 *   <li>Offline detection is not instant: a clean disconnect is seen at once, a crashed gateway after its
 *       lease expires, a half-open connection after the heartbeat timeout. That lag is accepted on purpose.</li>
 *   <li>Last seen is written when a connection closes. After a gateway crash it is not written, so it can be
 *       older than the real last activity.</li>
 * </ul>
 * Only people who share a conversation with the caller are visible.
 */
@Service
public class PresenceService {

    public static final int MAX_USERS_PER_QUERY = 200;
    private static final String LAST_SEEN_PREFIX = "presence:last-seen:";
    private static final Duration LAST_SEEN_TTL = Duration.ofDays(30);

    private final ConnectionRegistry registry;
    private final ConversationMemberRepository members;
    private final StringRedisTemplate redis;

    public PresenceService(ConnectionRegistry registry, ConversationMemberRepository members, StringRedisTemplate redis) {
        this.registry = registry;
        this.members = members;
        this.redis = redis;
    }

    public List<PresenceResponse> presenceOf(long callerId, Set<Long> userIds) {
        if (userIds.isEmpty() || userIds.size() > MAX_USERS_PER_QUERY) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Ask for 1.." + MAX_USERS_PER_QUERY + " users at a time");
        }
        Set<Long> visible = new HashSet<>(members.findPeersAmong(callerId, userIds));
        if (userIds.contains(callerId)) {
            visible.add(callerId);
        }
        List<Long> ids = new ArrayList<>(new LinkedHashSet<>(userIds));
        ids.retainAll(visible);
        if (ids.isEmpty()) {
            return List.of();
        }
        Set<Long> online = registry.onlineAmong(ids);
        List<String> lastSeen = redis.opsForValue().multiGet(ids.stream().map(id -> LAST_SEEN_PREFIX + id).toList());
        List<PresenceResponse> result = new ArrayList<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            String millis = lastSeen == null ? null : lastSeen.get(i);
            result.add(new PresenceResponse(ids.get(i), online.contains(ids.get(i)),
                    millis == null ? null : Instant.ofEpochMilli(Long.parseLong(millis))));
        }
        return result;
    }

    /** Called when one of the user's connections closes. If another device is still on, "online" wins anyway. */
    public void recordLastSeen(long userId) {
        redis.opsForValue().set(LAST_SEEN_PREFIX + userId, String.valueOf(System.currentTimeMillis()), LAST_SEEN_TTL);
    }
}

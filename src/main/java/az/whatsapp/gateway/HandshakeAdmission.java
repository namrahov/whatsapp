package az.whatsapp.gateway;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;

import java.time.Duration;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Admission control for new WebSocket connections (PDF section 10). When a gateway dies, its users all reconnect
 * at once; without a limit they would knock over the next gateway, and then the next.
 * <ol>
 *   <li>Instance full (max connections) → 503: go to another instance.</li>
 *   <li>Handshake rate over the limit (token bucket) → 429: come back later.</li>
 *   <li>The user already has too many live connections (a client stuck in a reconnect loop) → 429.</li>
 * </ol>
 * Every rejection carries a randomised Retry-After. The client adds its own backoff with jitter on top.
 */
@Component
public class HandshakeAdmission {

    private static final Logger log = LoggerFactory.getLogger(HandshakeAdmission.class);

    public record Rejection(HttpStatus status, long retryAfterSeconds, String reason) {
    }

    private final AdmissionProperties properties;
    private final LocalSessions localSessions;
    private final ConnectionRegistry registry;
    private final RateLimiter handshakeRate;

    public HandshakeAdmission(AdmissionProperties properties, LocalSessions localSessions, ConnectionRegistry registry) {
        this.properties = properties;
        this.localSessions = localSessions;
        this.registry = registry;
        this.handshakeRate = RateLimiter.of("ws-handshake", RateLimiterConfig.custom()
                .limitForPeriod(properties.handshakesPerSecond())
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(Duration.ZERO) // never queue a handshake: answer at once
                .build());
    }

    public Optional<Rejection> check(long userId) {
        if (localSessions.size() >= properties.maxConnectionsPerInstance()) {
            return reject(HttpStatus.SERVICE_UNAVAILABLE, "Gateway is full");
        }
        if (!handshakeRate.acquirePermission()) {
            return reject(HttpStatus.TOO_MANY_REQUESTS, "Too many connection attempts, retry later");
        }
        try {
            if (registry.liveConnectionsOf(userId).size() >= properties.maxConnectionsPerUser()) {
                return reject(HttpStatus.TOO_MANY_REQUESTS, "Too many open connections for this user");
            }
        } catch (RuntimeException e) {
            // Redis unreachable: skip this check; registering the connection will fail and close it anyway.
            log.debug("Per-user connection check skipped: {}", e.toString());
        }
        return Optional.empty();
    }

    private Optional<Rejection> reject(HttpStatus status, String reason) {
        return Optional.of(new Rejection(status, properties.randomRetryAfterSeconds(), reason));
    }
}

package az.whatsapp.gateway;

import jakarta.annotation.PreDestroy;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Server-level lease (PDF section 09). Instead of every connection renewing its own presence key every ~10 s
 * (100M connections → 10M Redis writes/s), each gateway renews one key: {@code ws:instance:<id>:alive}.
 * A connection is live while its gateway's lease is; when a gateway dies, all its connections become offline
 * together once the lease expires. The price is that lag (up to lease-ttl) and lazy cleanup of its entries.
 * <p>
 * Each renewal also checks a marker key. If Redis restarted and lost its data, the marker is gone, and this
 * gateway re-registers all its open connections at once instead of waiting for the next registry refresh.
 */
@Component
public class InstanceLease {

    private static final Logger log = LoggerFactory.getLogger(InstanceLease.class);
    private static final Duration MARKER_TTL = Duration.ofDays(1);

    private final StringRedisTemplate redis;
    private final GatewayInstance gateway;
    private final GatewayProperties properties;
    private final RegistryRefresher registryRefresher;

    public InstanceLease(StringRedisTemplate redis, GatewayInstance gateway, GatewayProperties properties,
                         RegistryRefresher registryRefresher) {
        this.redis = redis;
        this.gateway = gateway;
        this.properties = properties;
        this.registryRefresher = registryRefresher;
    }

    @Scheduled(fixedDelayString = "${whatsapp.gateway.lease-renew-interval-ms:10000}")
    public void renew() {
        String marker = GatewayInstance.markerKeyOf(gateway.id());
        String lease = GatewayInstance.leaseKeyOf(gateway.id());
        try {
            // One round trip: EXISTS answers for the state before this batch set the marker again.
            List<Object> results = RedisPipeline.run(redis, ops -> {
                ops.hasKey(marker);
                ops.opsForValue().set(marker, "1", MARKER_TTL);
                ops.opsForValue().set(lease, "1", properties.leaseTtl());
            });
            if (Boolean.FALSE.equals(results.getFirst())) {
                log.info("Redis has no registry marker for this gateway (first start or Redis lost its data); "
                        + "re-registering open connections");
                registryRefresher.refresh();
            }
        } catch (RuntimeException e) {
            // If this keeps failing the lease expires and other instances treat our users as offline,
            // which is the honest answer while we cannot reach Redis.
            log.warn("Lease renewal failed, will retry: {}", e.toString());
        }
    }

    /** Graceful shutdown: others see our connections as offline at once, not after lease-ttl. */
    @PreDestroy
    public void release() {
        try {
            redis.delete(List.of(GatewayInstance.leaseKeyOf(gateway.id()), GatewayInstance.markerKeyOf(gateway.id())));
        } catch (RuntimeException e) {
            log.debug("Could not release lease: {}", e.toString());
        }
    }
}

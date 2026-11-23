package az.whatsapp.gateway;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param leaseTtl    a gateway that stops renewing its lease for this long is considered dead
 * @param idleTimeout a connection that sends nothing (not even a heartbeat) for this long is closed
 */
@ConfigurationProperties("whatsapp.gateway")
public record GatewayProperties(Duration leaseTtl, Duration idleTimeout) {

    public GatewayProperties {
        if (leaseTtl == null || leaseTtl.toSeconds() < 3) {
            throw new IllegalArgumentException("whatsapp.gateway.lease-ttl must be at least 3s");
        }
        if (idleTimeout == null || idleTimeout.toSeconds() < 1) {
            throw new IllegalArgumentException("whatsapp.gateway.idle-timeout must be at least 1s");
        }
    }
}

package az.whatsapp.gateway;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** @param ttl how long a user's registry entry outlives the last refresh by its gateway instance */
@ConfigurationProperties("whatsapp.registry")
public record RegistryProperties(Duration ttl) {

    public RegistryProperties {
        if (ttl == null || ttl.toSeconds() < 3) {
            throw new IllegalArgumentException("whatsapp.registry.ttl must be at least 3s");
        }
    }
}

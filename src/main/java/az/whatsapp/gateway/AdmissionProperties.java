package az.whatsapp.gateway;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("whatsapp.admission")
public record AdmissionProperties(int handshakesPerSecond, int maxConnectionsPerInstance, int maxConnectionsPerUser,
                                  int syncMaxConcurrent, Duration retryAfterMin, Duration retryAfterMax) {

    public AdmissionProperties {
        if (handshakesPerSecond < 1 || maxConnectionsPerInstance < 1 || maxConnectionsPerUser < 1
                || syncMaxConcurrent < 1) {
            throw new IllegalArgumentException("whatsapp.admission limits must be at least 1");
        }
        if (retryAfterMin == null || retryAfterMax == null || retryAfterMin.toSeconds() < 1
                || retryAfterMax.compareTo(retryAfterMin) < 0) {
            throw new IllegalArgumentException("whatsapp.admission retry-after needs 1s <= min <= max");
        }
    }

    /**
     * A random Retry-After in [min, max]: if every rejected client were told the same number of seconds, they
     * would all come back in the same second and be rejected again (a retry storm the server itself caused).
     */
    public long randomRetryAfterSeconds() {
        return ThreadLocalRandom.current().nextLong(retryAfterMin.toSeconds(), retryAfterMax.toSeconds() + 1);
    }
}

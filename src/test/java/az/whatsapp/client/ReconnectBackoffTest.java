package az.whatsapp.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

class ReconnectBackoffTest {

    private final ReconnectBackoff backoff = new ReconnectBackoff(new Random(42));

    @Test
    void delayStaysWithinTheExponentialCap() {
        for (int i = 0; i < 1000; i++) {
            assertThat(backoff.delay(0, null)).isBetween(Duration.ZERO, Duration.ofSeconds(1));
            assertThat(backoff.delay(3, null)).isBetween(Duration.ZERO, Duration.ofSeconds(8));
            assertThat(backoff.delay(50, null)).isBetween(Duration.ZERO, Duration.ofSeconds(30));
        }
    }

    @Test
    void honoursRetryAfterAsAMinimum() {
        for (int i = 0; i < 1000; i++) {
            assertThat(backoff.delay(2, Duration.ofSeconds(5))).isBetween(Duration.ofSeconds(5), Duration.ofSeconds(9));
        }
    }

    @Test
    void spreadsClientsThatFailedTogether() {
        Set<Long> seconds = new HashSet<>();
        for (int client = 0; client < 1000; client++) {
            seconds.add(backoff.delay(5, null).toSeconds());
        }
        // 1000 clients on attempt 5 land across the whole 0..32 s window, not on one instant.
        assertThat(seconds).hasSizeGreaterThan(25);
    }
}

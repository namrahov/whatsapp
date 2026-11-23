package az.whatsapp.client;

import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * Reference client logic for PDF section 10 (lives in test sources: the server does not use it).
 * <p>
 * Exponential backoff with full jitter: {@code cap = min(30 s, 1 s * 2^attempt)}, {@code delay = random(0, cap)}.
 * Without the random part, a million clients dropped by the same gateway crash would all retry at 1 s, 2 s, 4 s
 * ... together and knock the next gateway over. If the server sent {@code Retry-After}, the client waits at least
 * that long, plus the jitter, so clients told the same Retry-After still spread out.
 */
public final class ReconnectBackoff {

    private static final Duration BASE = Duration.ofSeconds(1);
    private static final Duration CAP = Duration.ofSeconds(30);

    private final RandomGenerator random;

    public ReconnectBackoff(RandomGenerator random) {
        this.random = random;
    }

    /**
     * @param attempt    0 for the first retry
     * @param retryAfter the server's Retry-After, or null
     */
    public Duration delay(int attempt, Duration retryAfter) {
        long capMillis = Math.min(CAP.toMillis(), BASE.toMillis() << Math.min(Math.max(attempt, 0), 20));
        long jitter = random.nextLong(capMillis + 1);
        long floor = retryAfter == null ? 0 : retryAfter.toMillis();
        return Duration.ofMillis(floor + jitter);
    }
}

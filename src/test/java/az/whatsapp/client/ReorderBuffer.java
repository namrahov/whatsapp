package az.whatsapp.client;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Reference client logic for PDF section 07 (lives in test sources: the server does not use it).
 * <p>
 * Shows one conversation's messages in sequence order whatever order they arrive in. Arrival 103, 101, 102 with
 * 100 already shown: 103 waits in the buffer, 101 is shown, 102 is shown and releases 103. If a gap lasts longer
 * than {@code gapTimeout}, {@link #overdueGap()} names the range to fetch with
 * {@code GET /messages?afterSequence=&untilSequence=}. Duplicates (at-least-once delivery) are dropped.
 */
public final class ReorderBuffer<T> {

    /** Missing messages: sequences in (afterSequence, untilSequence]. */
    public record Gap(long afterSequence, long untilSequence) {
    }

    private final TreeMap<Long, T> pending = new TreeMap<>();
    private final Duration gapTimeout;
    private final InstantSource clock;
    private long lastShown;
    private Instant gapSince;

    /** @param lastShown highest sequence already stored and shown (0 for a new conversation) */
    public ReorderBuffer(long lastShown, Duration gapTimeout, InstantSource clock) {
        this.lastShown = lastShown;
        this.gapTimeout = gapTimeout;
        this.clock = clock;
    }

    /** @return the messages that can be shown now, in order; empty if this one has to wait or is a duplicate */
    public List<T> offer(long sequence, T message) {
        if (sequence <= lastShown || pending.containsKey(sequence)) {
            return List.of();
        }
        pending.put(sequence, message);
        List<T> ready = new ArrayList<>();
        while (!pending.isEmpty() && pending.firstKey() == lastShown + 1) {
            ready.add(pending.pollFirstEntry().getValue());
            lastShown++;
        }
        if (pending.isEmpty()) {
            gapSince = null;
        } else if (!ready.isEmpty() || gapSince == null) {
            gapSince = clock.instant(); // a (new) gap starts now
        }
        return ready;
    }

    /** The gap to repair from the server, once it has lasted longer than the timeout. */
    public Optional<Gap> overdueGap() {
        if (gapSince == null || Duration.between(gapSince, clock.instant()).compareTo(gapTimeout) < 0) {
            return Optional.empty();
        }
        return Optional.of(new Gap(lastShown, pending.firstKey() - 1));
    }

    public long lastShown() {
        return lastShown;
    }
}

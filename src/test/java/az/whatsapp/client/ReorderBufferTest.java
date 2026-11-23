package az.whatsapp.client;

import static org.assertj.core.api.Assertions.assertThat;

import az.whatsapp.client.ReorderBuffer.Gap;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class ReorderBufferTest {

    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-11-23T10:00:00Z"));
    private final ReorderBuffer<String> buffer = new ReorderBuffer<>(100, Duration.ofSeconds(2), now::get);

    @Test
    void showsMessagesInSequenceOrderWhateverTheArrivalOrder() {
        assertThat(buffer.offer(103, "m103")).isEmpty();
        assertThat(buffer.offer(101, "m101")).containsExactly("m101");
        assertThat(buffer.offer(102, "m102")).containsExactly("m102", "m103");
        assertThat(buffer.lastShown()).isEqualTo(103);
    }

    @Test
    void dropsDuplicates() {
        buffer.offer(101, "m101");
        assertThat(buffer.offer(101, "again")).isEmpty();
        buffer.offer(103, "m103");
        assertThat(buffer.offer(103, "again")).isEmpty();
        assertThat(buffer.offer(90, "old")).isEmpty();
    }

    @Test
    void reportsAGapOnlyAfterTheTimeout() {
        buffer.offer(104, "m104");
        assertThat(buffer.overdueGap()).isEmpty();

        now.set(now.get().plusSeconds(3));
        assertThat(buffer.overdueGap()).contains(new Gap(100, 103));
    }

    @Test
    void progressRestartsTheGapTimer() {
        buffer.offer(103, "m103");
        now.set(now.get().plusSeconds(1));
        buffer.offer(101, "m101"); // 101 shown, 102 still missing: a new gap starts now
        now.set(now.get().plusSeconds(1));
        assertThat(buffer.overdueGap()).isEmpty();

        now.set(now.get().plusSeconds(2));
        assertThat(buffer.overdueGap()).contains(new Gap(101, 102));
    }

    @Test
    void noGapOnceFilled() {
        buffer.offer(102, "m102");
        buffer.offer(101, "m101");
        now.set(now.get().plusSeconds(10));
        assertThat(buffer.overdueGap()).isEmpty();
    }
}

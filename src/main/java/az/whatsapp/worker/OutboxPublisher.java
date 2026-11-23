package az.whatsapp.worker;

import az.whatsapp.config.KafkaConfig;
import az.whatsapp.repository.OutboxQueueRepository;
import az.whatsapp.repository.OutboxQueueRepository.OutboxMessage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Moves committed outbox rows to Kafka in three short steps, so no DB lock or connection is held while
 * waiting for Kafka:
 * <ol>
 *   <li>claim a batch (one UPDATE, committed at once),</li>
 *   <li>send the whole batch asynchronously, then wait for all acks (no transaction open),</li>
 *   <li>mark the acked ones PUBLISHED and give the failed ones back (one UPDATE each).</li>
 * </ol>
 * Delivery is at-least-once: a crash between 2 and 3 leaves the claim to expire and the batch is sent again.
 * The delivery worker tolerates duplicates: clients drop a messageId they already have.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int BATCH_SIZE = 100;
    /** Must comfortably exceed the time to send a batch, or another instance could claim it again meanwhile. */
    private static final int CLAIM_SECONDS = 30;
    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final OutboxQueueRepository outbox;
    private final KafkaTemplate<String, String> kafka;

    public OutboxPublisher(OutboxQueueRepository outbox, KafkaTemplate<String, String> kafka) {
        this.outbox = outbox;
        this.kafka = kafka;
    }

    @Scheduled(fixedDelay = 500)
    public void publishPending() {
        List<OutboxMessage> batch = outbox.claim(BATCH_SIZE, CLAIM_SECONDS);
        if (batch.isEmpty()) {
            return;
        }

        // Send everything first, then wait: one network wait per batch instead of one per event.
        // The idempotent producer keeps per-partition (= per-key) order of these sends.
        List<CompletableFuture<SendResult<String, String>>> acks = new ArrayList<>(batch.size());
        for (OutboxMessage message : batch) {
            CompletableFuture<SendResult<String, String>> ack = send(message);
            acks.add(ack);
            if (ack.isCompletedExceptionally()) {
                // send() blocked for max.block.ms and failed: the broker is unreachable. Don't wait that long
                // for every remaining event (the claim would run out); fail the rest of the batch now.
                while (acks.size() < batch.size()) {
                    acks.add(CompletableFuture.failedFuture(new IllegalStateException("Kafka unavailable")));
                }
                break;
            }
        }

        List<UUID> published = new ArrayList<>();
        List<UUID> failed = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SEND_TIMEOUT_SECONDS);
        for (int i = 0; i < batch.size(); i++) {
            UUID eventId = batch.get(i).eventId();
            try {
                long left = Math.max(0, deadline - System.nanoTime());
                acks.get(i).get(left, TimeUnit.NANOSECONDS);
                published.add(eventId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failed.add(eventId);
            } catch (Exception e) {
                failed.add(eventId);
            }
        }

        outbox.markPublished(published);
        outbox.release(failed);
        if (!failed.isEmpty()) {
            // A failed event may now go out after a later event of the same conversation. Clients order by
            // sequence number, not arrival order, and fill gaps from the DB, so that is safe.
            log.warn("{} of {} outbox events not published, will retry", failed.size(), batch.size());
        }
    }

    private CompletableFuture<SendResult<String, String>> send(OutboxMessage message) {
        var record = new ProducerRecord<>(KafkaConfig.topicFor(message.eventType()),
                message.kafkaKey(), message.payload());
        record.headers().add(KafkaConfig.EVENT_TYPE_HEADER, message.eventType().getBytes(StandardCharsets.UTF_8));
        try {
            return kafka.send(record);
        } catch (RuntimeException e) {
            // send() itself throws when the broker is unreachable for max.block.ms
            return CompletableFuture.failedFuture(e);
        }
    }
}

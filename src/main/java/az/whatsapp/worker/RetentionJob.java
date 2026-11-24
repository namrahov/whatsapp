package az.whatsapp.worker;

import az.whatsapp.repository.OutboxQueueRepository;
import az.whatsapp.repository.RetentionRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Background cleanup (PDF section 15), in small batches so no statement holds locks for long:
 * <ul>
 *   <li>messages older than whatsapp.retention.message-ttl (example policy: 30 days, delivered or not; an expired
 *       message is not "delivered", a device that was offline that long simply never gets it);</li>
 *   <li>outbox events published more than whatsapp.retention.outbox-ttl ago (they were only kept for debugging).</li>
 * </ul>
 */
@Component
public class RetentionJob {

    private static final Logger log = LoggerFactory.getLogger(RetentionJob.class);

    private final RetentionRepository retention;
    private final OutboxQueueRepository outbox;
    private final RetentionProperties properties;

    public RetentionJob(RetentionRepository retention, OutboxQueueRepository outbox, RetentionProperties properties) {
        this.retention = retention;
        this.outbox = outbox;
        this.properties = properties;
    }

    @Scheduled(initialDelayString = "${whatsapp.retention.initial-delay-ms:30000}",
            fixedDelayString = "${whatsapp.retention.interval-ms:600000}")
    public void run() {
        try {
            deleteExpiredMessages();
            deletePublishedOutboxEvents();
        } catch (RuntimeException e) {
            log.warn("Retention run failed, will retry next time: {}", e.toString());
        }
    }

    public int deleteExpiredMessages() {
        int total = 0;
        for (int i = 0; i < properties.maxBatchesPerRun(); i++) {
            int deleted = retention.deleteExpiredMessages(properties.messageTtl(), properties.batchSize());
            total += deleted;
            if (deleted < properties.batchSize()) {
                break;
            }
        }
        if (total > 0) {
            log.info("Deleted {} messages older than {}", total, properties.messageTtl());
        }
        return total;
    }

    public int deletePublishedOutboxEvents() {
        int days = (int) Math.max(1, properties.outboxTtl().toDays());
        int total = 0;
        for (int i = 0; i < properties.maxBatchesPerRun(); i++) {
            int deleted = outbox.deletePublishedOlderThan(days, properties.batchSize());
            total += deleted;
            if (deleted < properties.batchSize()) {
                break;
            }
        }
        if (total > 0) {
            log.info("Deleted {} published outbox events older than {} days", total, days);
        }
        return total;
    }
}

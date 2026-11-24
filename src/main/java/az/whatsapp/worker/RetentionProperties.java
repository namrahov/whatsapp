package az.whatsapp.worker;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param messageTtl       messages older than this are deleted, delivered or not (a product decision)
 * @param outboxTtl        published outbox events are kept this long for debugging / replay
 * @param batchSize        rows per DELETE: short transactions, no long locks
 * @param maxBatchesPerRun caps one run; what is left goes on the next run
 */
@ConfigurationProperties("whatsapp.retention")
public record RetentionProperties(Duration messageTtl, Duration outboxTtl, int batchSize, int maxBatchesPerRun) {

    public RetentionProperties {
        if (messageTtl == null || outboxTtl == null || messageTtl.toMinutes() < 1 || outboxTtl.toMinutes() < 1) {
            throw new IllegalArgumentException("whatsapp.retention TTLs must be at least 1 minute");
        }
        if (batchSize < 1 || maxBatchesPerRun < 1) {
            throw new IllegalArgumentException("whatsapp.retention batch settings must be at least 1");
        }
    }
}

package az.whatsapp.service;

import az.whatsapp.gateway.AdmissionProperties;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;

import java.time.Duration;
import java.util.function.Supplier;

import org.springframework.stereotype.Component;

/**
 * Caps concurrent sync queries ({@code GET /messages?afterSequence=}). After a reconnect storm every client syncs
 * at once; unbounded, those queries would use up the connection pool and slow down sending for everyone.
 * Beyond the cap the request fails at once with 503 + Retry-After instead of queueing.
 */
@Component
public class SyncBulkhead {

    private final Bulkhead bulkhead;
    private final AdmissionProperties properties;

    public SyncBulkhead(AdmissionProperties properties) {
        this.properties = properties;
        this.bulkhead = Bulkhead.of("sync", BulkheadConfig.custom()
                .maxConcurrentCalls(properties.syncMaxConcurrent())
                .maxWaitDuration(Duration.ZERO)
                .build());
    }

    public <T> T run(Supplier<T> query) {
        if (!bulkhead.tryAcquirePermission()) {
            throw new OverloadedException("Too many syncs in progress, retry later",
                    properties.randomRetryAfterSeconds());
        }
        try {
            return query.get();
        } finally {
            bulkhead.onComplete();
        }
    }
}

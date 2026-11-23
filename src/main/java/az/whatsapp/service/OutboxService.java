package az.whatsapp.service;

import az.whatsapp.entity.OutboxEventEntity;
import az.whatsapp.repository.OutboxRepository;

import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
public class OutboxService {

    private final OutboxRepository outbox;
    private final JsonMapper json;

    public OutboxService(OutboxRepository outbox, JsonMapper json) {
        this.outbox = outbox;
        this.json = json;
    }

    /**
     * Stores the event in the caller's transaction, so it commits or rolls back together with the business change.
     * MANDATORY: calling this outside a transaction is a bug (the event could be saved without the change).
     *
     * @param kafkaKey becomes the Kafka key: events with the same key keep their order
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void append(UUID eventId, String eventType, long aggregateId, String kafkaKey, Object event) {
        outbox.save(new OutboxEventEntity(eventId, aggregateId, kafkaKey, eventType, json.writeValueAsString(event)));
    }
}

package az.whatsapp.config;

import az.whatsapp.dto.event.MessageCreatedEvent;
import az.whatsapp.dto.event.ReceiptAdvancedEvent;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

@Configuration
public class KafkaConfig {

    /**
     * Message events, keyed by conversation id: one conversation always maps to one partition, so its events are
     * consumed in order. (Keying by user id would split A→B and B→A of the same chat across partitions.)
     */
    public static final String MESSAGE_EVENTS_TOPIC = "message-events";

    /** Kafka header carrying the outbox event type, so one listener can tell event kinds apart. */
    public static final String EVENT_TYPE_HEADER = "event-type";

    public static final String DLT_SUFFIX = ".DLT";

    /** Which topic an outbox row of a given event type goes to. */
    public static String topicFor(String eventType) {
        return switch (eventType) {
            // Same topic and key: a receipt is consumed after the message it acknowledges.
            case MessageCreatedEvent.TYPE, ReceiptAdvancedEvent.TYPE -> MESSAGE_EVENTS_TOPIC;
            default -> throw new IllegalArgumentException("No topic for event type " + eventType);
        };
    }

    public static String dltOf(String topic) {
        return topic + DLT_SUFFIX;
    }

    /** Created on startup by KafkaAdmin if missing. 3 partitions = up to 3 delivery consumers in parallel. */
    @Bean
    public NewTopic messageEventsTopic() {
        return TopicBuilder.name(MESSAGE_EVENTS_TOPIC).partitions(3).replicas(1).build();
    }

    /** Same partition count as the source topic: a dead letter keeps its original partition (and key order). */
    @Bean
    public NewTopic messageEventsDltTopic() {
        return TopicBuilder.name(dltOf(MESSAGE_EVENTS_TOPIC)).partitions(3).replicas(1).build();
    }

    /**
     * Retry a failed record for ~10 s (a short Redis/DB blip), then park it in {@code <topic>.DLT} instead of
     * dropping it. A message that is parked was still stored: the recipient gets it on the next reconnect sync.
     * Unparseable payloads go to the DLT at once; retrying cannot fix them.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaOperations<String, String> kafka) {
        var recoverer = new DeadLetterPublishingRecoverer(kafka, (record, exception) -> new TopicPartition(
                record.topic().endsWith(DLT_SUFFIX) ? record.topic() : dltOf(record.topic()),
                record.partition()));
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000, 9));
        handler.addNotRetryableExceptions(JacksonException.class);
        return handler;
    }
}

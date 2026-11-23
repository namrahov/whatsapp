package az.whatsapp.dto.event;

import az.whatsapp.entity.ReceiptKind;

/**
 * A delivery or read cursor moved forward. Goes through the outbox on the same topic and key (conversation id)
 * as the messages. Arrival order relative to messages is not guaranteed (see KafkaConfig#topicFor); receivers
 * keep the maximum, so duplicates and early arrivals are harmless.
 *
 * @param userId       whose cursor moved; {@code null} = the "all members" aggregate of a group
 * @param toAllMembers push to every member (1-to-1 chats, group aggregates) or only to {@code userId}'s own
 *                     connections (a group member's personal read state, to sync their other devices)
 */
public record ReceiptAdvancedEvent(long conversationId, Long userId, ReceiptKind kind, long upToSequence,
                                   boolean toAllMembers) {

    public static final String TYPE = "ReceiptAdvanced";
}

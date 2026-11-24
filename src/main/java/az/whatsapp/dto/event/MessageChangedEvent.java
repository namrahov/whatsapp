package az.whatsapp.dto.event;

import java.time.Instant;

/**
 * A message was edited or deleted for everyone. Goes through the outbox on the conversation's key like every
 * other event of the conversation.
 * <p>
 * Clients apply it only if {@code version} is higher than the version they have, so a late, older event never
 * overwrites a newer one, and applying it twice changes nothing. {@code changeNumber} is the client's cursor into
 * the conversation's change stream ({@code GET /changes?afterChange=}).
 *
 * @param payload new ciphertext (Base64); null when deleted
 */
public record MessageChangedEvent(long messageId, long conversationId, long sequenceNumber, int version,
                                  long changeNumber, boolean deleted, String payload, Instant changedAt) {

    public static final String TYPE = "MessageChanged";
}

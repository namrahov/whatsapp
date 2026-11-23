package az.whatsapp.service;

import az.whatsapp.dto.event.MessageCreatedEvent;
import az.whatsapp.entity.MessageEntity;
import az.whatsapp.repository.ConversationSequenceRepository;
import az.whatsapp.repository.MessageRepository;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Limit;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

/**
 * Accepting a message is one short transaction: take the next conversation sequence, insert the message, insert
 * the outbox event. Only after it commits does the sender get SENT; delivery happens afterwards, from Kafka.
 * If PostgreSQL is down nothing is acknowledged, and the client retries with the same clientMessageId.
 */
@Service
public class MessageService {

    public static final int MAX_PAYLOAD_BYTES = 64 * 1024;
    public static final int MAX_SYNC_LIMIT = 100;

    public record SendResult(MessageEntity message, boolean duplicate) {
    }

    private final MessageRepository messages;
    private final ConversationSequenceRepository sequences;
    private final ConversationService conversations;
    private final OutboxService outbox;
    private final TransactionTemplate tx;

    public MessageService(MessageRepository messages, ConversationSequenceRepository sequences,
                          ConversationService conversations, OutboxService outbox, TransactionTemplate tx) {
        this.messages = messages;
        this.sequences = sequences;
        this.conversations = conversations;
        this.outbox = outbox;
        this.tx = tx;
    }

    public SendResult send(long senderId, long conversationId, UUID clientMessageId, byte[] payload) {
        if (payload.length == 0 || payload.length > MAX_PAYLOAD_BYTES) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Payload must be 1.." + MAX_PAYLOAD_BYTES + " bytes");
        }
        // Fast path for retries: the message was stored, but the client never got our ACK.
        var existing = messages.findBySenderIdAndClientMessageId(senderId, clientMessageId);
        if (existing.isPresent()) {
            return asDuplicate(existing.get(), conversationId, payload);
        }
        try {
            return new SendResult(tx.execute(status -> insert(senderId, conversationId, clientMessageId, payload)),
                    false);
        } catch (DataIntegrityViolationException e) {
            // Two attempts with the same clientMessageId ran concurrently and the other one committed first.
            // Our transaction rolled back, its sequence increment included, so no gap was left behind.
            return messages.findBySenderIdAndClientMessageId(senderId, clientMessageId)
                    .map(original -> asDuplicate(original, conversationId, payload))
                    .orElseThrow(() -> e);
        }
    }

    /** Messages after {@code afterSequence}, oldest first: used on reconnect and to repair gaps. */
    public List<MessageEntity> after(long userId, long conversationId, long afterSequence, int limit) {
        conversations.requireMember(conversationId, userId);
        int pageSize = Math.clamp(limit, 1, MAX_SYNC_LIMIT);
        return messages.findByConversationIdAndSequenceNumberGreaterThanOrderBySequenceNumberAsc(
                conversationId, Math.max(0, afterSequence), Limit.of(pageSize));
    }

    private MessageEntity insert(long senderId, long conversationId, UUID clientMessageId, byte[] payload) {
        conversations.requireMember(conversationId, senderId);
        long sequence = sequences.next(conversationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Conversation not found"));
        MessageEntity saved = messages.saveAndFlush(
                new MessageEntity(conversationId, senderId, clientMessageId, sequence, payload));
        // Kafka key = conversation id: every event of one conversation goes to the same partition, in order.
        outbox.append(UUID.randomUUID(), MessageCreatedEvent.TYPE, saved.getId(), String.valueOf(conversationId),
                MessageCreatedEvent.from(saved));
        return saved;
    }

    /**
     * A retry must be the same message. Reusing a clientMessageId for other content or another conversation is
     * a client bug; silently returning the old message would hide it.
     */
    private static SendResult asDuplicate(MessageEntity original, long conversationId, byte[] payload) {
        if (original.getConversationId() != conversationId || !Arrays.equals(original.getEncryptedPayload(), payload)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "clientMessageId was already used for a different message");
        }
        return new SendResult(original, true);
    }
}

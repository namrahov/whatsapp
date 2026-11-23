package az.whatsapp.service;

import az.whatsapp.dto.event.MessageCreatedEvent;
import az.whatsapp.entity.ConversationEntity;
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
    private final ReceiptService receipts;
    private final TransactionTemplate tx;

    public MessageService(MessageRepository messages, ConversationSequenceRepository sequences,
                          ConversationService conversations, OutboxService outbox, ReceiptService receipts,
                          TransactionTemplate tx) {
        this.messages = messages;
        this.sequences = sequences;
        this.conversations = conversations;
        this.outbox = outbox;
        this.receipts = receipts;
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

    /**
     * Messages after {@code afterSequence} (up to {@code untilSequence} if given), oldest first: the reconnect sync
     * and the gap repair of a client that saw 5 and then 7.
     * <p>
     * Sequences have no permanent holes: a sequence is taken under the conversation row lock and released only by
     * commit (the message is there) or rollback (the number is given back). And the lock makes commits happen in
     * sequence order, so once 7 is visible, 6 is too. A gap a client sees is therefore always a lost or late push,
     * never a missing message, and one repair request fills it.
     */
    public List<MessageEntity> after(long userId, long conversationId, long afterSequence, Long untilSequence,
                                     int limit) {
        conversations.requireMember(conversationId, userId);
        int pageSize = Math.clamp(limit, 1, MAX_SYNC_LIMIT);
        long after = Math.max(0, afterSequence);
        if (untilSequence == null) {
            return messages.findByConversationIdAndSequenceNumberGreaterThanOrderBySequenceNumberAsc(
                    conversationId, after, Limit.of(pageSize));
        }
        if (untilSequence <= after) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "untilSequence must be greater than afterSequence");
        }
        return messages.findByConversationIdAndSequenceNumberGreaterThanAndSequenceNumberLessThanEqualOrderBySequenceNumberAsc(
                conversationId, after, untilSequence, Limit.of(pageSize));
    }

    private MessageEntity insert(long senderId, long conversationId, UUID clientMessageId, byte[] payload) {
        ConversationEntity conversation = conversations.requireMembership(conversationId, senderId);
        long sequence = sequences.next(conversationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Conversation not found"));
        MessageEntity saved = messages.saveAndFlush(
                new MessageEntity(conversationId, senderId, clientMessageId, sequence, payload));
        // Kafka key = conversation id: every event of one conversation goes to the same partition, in order.
        outbox.append(UUID.randomUUID(), MessageCreatedEvent.TYPE, saved.getId(), String.valueOf(conversationId),
                MessageCreatedEvent.from(saved));
        receipts.onOwnMessage(conversation, senderId, sequence);
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

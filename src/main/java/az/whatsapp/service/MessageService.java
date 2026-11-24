package az.whatsapp.service;

import az.whatsapp.dto.event.MessageChangedEvent;
import az.whatsapp.dto.event.MessageCreatedEvent;
import az.whatsapp.entity.ConversationEntity;
import az.whatsapp.entity.MessageEntity;
import az.whatsapp.repository.ConversationSequenceRepository;
import az.whatsapp.repository.MessageChangeRepository;
import az.whatsapp.repository.MessageRepository;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
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
    private final MessageChangeRepository changes;
    private final MessageProperties properties;
    private final TransactionTemplate tx;

    public MessageService(MessageRepository messages, ConversationSequenceRepository sequences,
                          ConversationService conversations, OutboxService outbox, ReceiptService receipts,
                          MessageChangeRepository changes, MessageProperties properties, TransactionTemplate tx) {
        this.messages = messages;
        this.sequences = sequences;
        this.conversations = conversations;
        this.outbox = outbox;
        this.receipts = receipts;
        this.changes = changes;
        this.properties = properties;
        this.tx = tx;
    }

    public SendResult send(long senderId, long conversationId, UUID clientMessageId, byte[] payload) {
        requireValidPayload(payload);
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
    public SyncPage after(long userId, long conversationId, long afterSequence, Long untilSequence, int limit) {
        ConversationEntity conversation = conversations.requireMembership(conversationId, userId);
        int pageSize = Math.clamp(limit, 1, MAX_SYNC_LIMIT);
        long after = Math.max(0, afterSequence);
        List<MessageEntity> page;
        if (untilSequence == null) {
            page = messages.findByConversationIdAndSequenceNumberGreaterThanOrderBySequenceNumberAsc(
                    conversationId, after, Limit.of(pageSize));
        } else if (untilSequence <= after) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "untilSequence must be greater than afterSequence");
        } else {
            page = messages.findByConversationIdAndSequenceNumberGreaterThanAndSequenceNumberLessThanEqualOrderBySequenceNumberAsc(
                    conversationId, after, untilSequence, Limit.of(pageSize));
        }
        return new SyncPage(page, conversation.getFirstAvailableSequence());
    }

    /**
     * A page of a sync. Sequences below {@code firstAvailableSequence} were deleted by retention: a client whose
     * cursor is behind it must skip ahead instead of waiting for them.
     */
    public record SyncPage(List<MessageEntity> messages, long firstAvailableSequence) {
    }

    /** Edits and deletes after the client's change cursor: how a device catches up on changes it missed. */
    public List<MessageEntity> changes(long userId, long conversationId, long afterChange, int limit) {
        conversations.requireMember(conversationId, userId);
        return messages.findByConversationIdAndChangeNumberGreaterThanOrderByChangeNumberAsc(
                conversationId, Math.max(0, afterChange), Limit.of(Math.clamp(limit, 1, MAX_SYNC_LIMIT)));
    }

    /** Edit by the sender, within the edit window. Sending the same content again changes nothing. */
    public MessageChangedEvent edit(long userId, long conversationId, long messageId, byte[] payload) {
        requireValidPayload(payload);
        return tx.execute(status -> {
            MessageEntity message = requireOwnMessage(userId, conversationId, messageId, true);
            if (Arrays.equals(message.getEncryptedPayload(), payload)) {
                return currentState(message);
            }
            long change = changes.nextChange(conversationId);
            Instant at = Instant.now().truncatedTo(ChronoUnit.MICROS);
            if (!changes.edit(messageId, message.getVersion(), payload, change, at)) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "The message was changed meanwhile, reload it");
            }
            return publish(new MessageChangedEvent(messageId, conversationId, message.getSequenceNumber(),
                    message.getVersion() + 1, change, false, Base64.getEncoder().encodeToString(payload), at));
        });
    }

    /** Delete for everyone by the sender, within the delete window. Deleting twice changes nothing. */
    public MessageChangedEvent delete(long userId, long conversationId, long messageId) {
        return tx.execute(status -> {
            MessageEntity message = requireOwnMessage(userId, conversationId, messageId, false);
            if (message.isDeleted()) {
                return currentState(message);
            }
            long change = changes.nextChange(conversationId);
            Instant at = Instant.now().truncatedTo(ChronoUnit.MICROS);
            if (!changes.delete(messageId, message.getVersion(), change, at)) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "The message was changed meanwhile, reload it");
            }
            return publish(new MessageChangedEvent(messageId, conversationId, message.getSequenceNumber(),
                    message.getVersion() + 1, change, true, null, at));
        });
    }

    private MessageEntity requireOwnMessage(long userId, long conversationId, long messageId, boolean forEdit) {
        conversations.requireMember(conversationId, userId);
        MessageEntity message = messages.findById(messageId)
                .filter(m -> m.getConversationId() == conversationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Message not found"));
        if (message.getSenderId() != userId) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Only the sender can change a message");
        }
        if (forEdit && message.isDeleted()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "The message was deleted");
        }
        Duration window = forEdit ? properties.editWindow() : properties.deleteWindow();
        if (message.getCreatedAt().plus(window).isBefore(Instant.now())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Too late: the message is older than " + window);
        }
        return message;
    }

    private MessageChangedEvent publish(MessageChangedEvent event) {
        outbox.append(UUID.randomUUID(), MessageChangedEvent.TYPE, event.messageId(),
                String.valueOf(event.conversationId()), event);
        return event;
    }

    private static MessageChangedEvent currentState(MessageEntity message) {
        return new MessageChangedEvent(message.getId(), message.getConversationId(), message.getSequenceNumber(),
                message.getVersion(), message.getChangeNumber() == null ? 0 : message.getChangeNumber(),
                message.isDeleted(),
                message.isDeleted() ? null : Base64.getEncoder().encodeToString(message.getEncryptedPayload()),
                message.isDeleted() ? message.getDeletedAt()
                        : message.getEditedAt() != null ? message.getEditedAt() : message.getCreatedAt());
    }

    private static void requireValidPayload(byte[] payload) {
        if (payload.length == 0 || payload.length > MAX_PAYLOAD_BYTES) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Payload must be 1.." + MAX_PAYLOAD_BYTES + " bytes");
        }
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
        // Once edited or deleted the original content is gone, so a late retry can only be checked by conversation.
        boolean contentChanged = original.getVersion() > 1;
        if (original.getConversationId() != conversationId
                || !contentChanged && !Arrays.equals(original.getEncryptedPayload(), payload)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "clientMessageId was already used for a different message");
        }
        return new SendResult(original, true);
    }
}

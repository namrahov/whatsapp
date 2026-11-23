package az.whatsapp.service;

import az.whatsapp.dto.event.ReceiptAdvancedEvent;
import az.whatsapp.dto.response.ReceiptsResponse;
import az.whatsapp.entity.ConversationEntity;
import az.whatsapp.entity.ConversationType;
import az.whatsapp.entity.ReceiptKind;
import az.whatsapp.repository.CursorRepository;
import az.whatsapp.repository.CursorRepository.MemberCursors;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

/**
 * Cumulative DELIVERED / READ acknowledgements ("everything up to sequence N"), and the receipt events that let
 * the other side update its ticks. A cursor that moves writes a {@link ReceiptAdvancedEvent} to the outbox in the
 * same transaction; one that does not move (old or repeated ACK) writes nothing.
 * <p>
 * Who is told what:
 * <ul>
 *   <li>1-to-1: every move of the peer's cursor goes to both members (the sender's ticks).</li>
 *   <li>Group: pushing every member's every move to every member would be members² frames. Instead the
 *       "delivered to all" / "read by all" minimum goes to everyone when it moves, and a member's own read
 *       cursor goes only to that member's other devices (unread badge). Per-member detail is fetched on demand
 *       ({@code GET /api/conversations/{id}/receipts}).</li>
 * </ul>
 */
@Service
public class ReceiptService {

    private final CursorRepository cursors;
    private final ConversationService conversations;
    private final OutboxService outbox;
    private final TransactionTemplate tx;

    public ReceiptService(CursorRepository cursors, ConversationService conversations, OutboxService outbox,
                          TransactionTemplate tx) {
        this.cursors = cursors;
        this.conversations = conversations;
        this.outbox = outbox;
        this.tx = tx;
    }

    /** DELIVERED: this device has stored every message of the conversation up to {@code upToSequence}. */
    public void markDelivered(long userId, String deviceId, long conversationId, long upToSequence) {
        requirePositive(upToSequence);
        tx.executeWithoutResult(status -> {
            ConversationEntity conversation = conversations.requireMembership(conversationId, userId);
            advanceDelivered(conversation, userId, deviceId, upToSequence);
        });
    }

    /** READ: the user has seen everything up to {@code upToSequence}; on this device, so it is stored there too. */
    public void markRead(long userId, String deviceId, long conversationId, long upToSequence) {
        requirePositive(upToSequence);
        tx.executeWithoutResult(status -> {
            ConversationEntity conversation = conversations.requireMembership(conversationId, userId);
            cursors.advanceDevice(userId, deviceId, conversationId, upToSequence);
            advanceRead(conversation, userId, upToSequence);
        });
    }

    /** Per-member cursors plus the group-wide minimums: the detail behind the ticks. */
    public ReceiptsResponse receipts(long userId, long conversationId) {
        conversations.requireMember(conversationId, userId);
        List<MemberCursors> members = cursors.memberCursors(conversationId);
        long allDelivered = members.stream().mapToLong(MemberCursors::deliveredUpTo).min().orElse(0);
        long allRead = members.stream().mapToLong(MemberCursors::readUpTo).min().orElse(0);
        return new ReceiptsResponse(conversationId, allDelivered, allRead, members);
    }

    public Map<Long, Long> readCursorsOf(long userId) {
        return cursors.readCursorsOf(userId);
    }

    public Map<Long, Long> deviceCursorsOf(long userId, String deviceId) {
        return cursors.deviceCursorsOf(userId, deviceId);
    }

    /**
     * Sending a message means the sender has seen the conversation up to it (as in WhatsApp, replying marks the
     * chat read). Without this the sender would hold back "read by all" for their own messages.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void onOwnMessage(ConversationEntity conversation, long senderId, long sequence) {
        advanceRead(conversation, senderId, sequence);
    }

    private void advanceDelivered(ConversationEntity conversation, long userId, String deviceId, long upToSequence) {
        long conversationId = conversation.getId();
        boolean group = conversation.getType() == ConversationType.GROUP;
        long before = cursors.deliveredTo(userId, conversationId);
        long allBefore = group ? cursors.deliveredToAll(conversationId) : 0;

        cursors.advanceDevice(userId, deviceId, conversationId, upToSequence);

        long after = cursors.deliveredTo(userId, conversationId);
        if (after <= before) {
            return; // another device of this user had already got further
        }
        if (!group) {
            emit(conversationId, userId, ReceiptKind.DELIVERED, after, true);
            return;
        }
        long allAfter = cursors.deliveredToAll(conversationId);
        if (allAfter > allBefore) {
            emit(conversationId, null, ReceiptKind.DELIVERED, allAfter, true);
        }
    }

    private void advanceRead(ConversationEntity conversation, long userId, long upToSequence) {
        long conversationId = conversation.getId();
        boolean group = conversation.getType() == ConversationType.GROUP;
        long allBefore = group ? cursors.readByAll(conversationId) : 0;

        OptionalLong moved = cursors.advanceRead(userId, conversationId, upToSequence);
        if (moved.isEmpty()) {
            return;
        }
        if (!group) {
            emit(conversationId, userId, ReceiptKind.READ, moved.getAsLong(), true);
            return;
        }
        emit(conversationId, userId, ReceiptKind.READ, moved.getAsLong(), false);
        long allAfter = cursors.readByAll(conversationId);
        if (allAfter > allBefore) {
            emit(conversationId, null, ReceiptKind.READ, allAfter, true);
        }
    }

    private void emit(long conversationId, Long userId, ReceiptKind kind, long upToSequence, boolean toAllMembers) {
        outbox.append(UUID.randomUUID(), ReceiptAdvancedEvent.TYPE, conversationId, String.valueOf(conversationId),
                new ReceiptAdvancedEvent(conversationId, userId, kind, upToSequence, toAllMembers));
    }

    private static void requirePositive(long upToSequence) {
        if (upToSequence < 1) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "upToSequence must be at least 1");
        }
    }
}

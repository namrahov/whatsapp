package az.whatsapp.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import az.whatsapp.IntegrationTest;
import az.whatsapp.dto.event.ReceiptAdvancedEvent;
import az.whatsapp.dto.response.ReceiptsResponse;
import az.whatsapp.entity.ReceiptKind;
import az.whatsapp.repository.CursorRepository.MemberCursors;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class ReceiptServiceIT extends IntegrationTest {

    @Autowired
    private MessageService messages;

    @Autowired
    private ReceiptService receipts;

    @Test
    void deliveryCursorOnlyMovesForwardAndStopsAtTheLastMessage() {
        long alice = newUser();
        long bob = newUser();
        long chat = newDirect(alice, bob);
        send(alice, chat, 3);

        receipts.markDelivered(bob, "phone", chat, 2);
        assertThat(cursorsOf(bob, chat).deliveredUpTo()).isEqualTo(2);
        assertThat(receiptEvents(chat, ReceiptKind.DELIVERED, bob)).isEqualTo(1);

        receipts.markDelivered(bob, "phone", chat, 1); // old ACK arriving late
        receipts.markDelivered(bob, "phone", chat, 2); // repeated ACK
        assertThat(cursorsOf(bob, chat).deliveredUpTo()).isEqualTo(2);
        assertThat(receiptEvents(chat, ReceiptKind.DELIVERED, bob)).isEqualTo(1);

        receipts.markDelivered(bob, "phone", chat, 99); // claims messages that do not exist
        assertThat(cursorsOf(bob, chat).deliveredUpTo()).isEqualTo(3);
    }

    @Test
    void anotherDeviceBehindDoesNotMoveTheUsersCursor() {
        long alice = newUser();
        long bob = newUser();
        long chat = newDirect(alice, bob);
        send(alice, chat, 3);

        receipts.markDelivered(bob, "phone", chat, 3);
        receipts.markDelivered(bob, "laptop", chat, 1);

        assertThat(cursorsOf(bob, chat).deliveredUpTo()).isEqualTo(3);
        assertThat(receiptEvents(chat, ReceiptKind.DELIVERED, bob)).isEqualTo(1);
        assertThat(receipts.deviceCursorsOf(bob, "laptop")).containsEntry(chat, 1L);
    }

    @Test
    void readImpliesDeliveredAndSendingMarksTheChatReadForTheSender() {
        long alice = newUser();
        long bob = newUser();
        long chat = newDirect(alice, bob);
        send(alice, chat, 2);

        receipts.markRead(bob, "phone", chat, 2);

        assertThat(cursorsOf(bob, chat)).isEqualTo(new MemberCursors(bob, 2, 2));
        assertThat(cursorsOf(alice, chat).readUpTo()).isEqualTo(2);
        assertThat(receipts.readCursorsOf(bob)).containsEntry(chat, 2L);
    }

    @Test
    void groupAggregateMovesOnlyWhenEveryMemberHasReachedIt() {
        long alice = newUser();
        long bob = newUser();
        long carol = newUser();
        long group = newGroup(alice, bob, carol);
        send(alice, group, 1);

        receipts.markDelivered(bob, "phone", group, 1);
        assertThat(receipts.receipts(alice, group).deliveredToAllUpTo()).isZero();
        assertThat(receiptEvents(group, ReceiptKind.DELIVERED, null)).isZero();

        receipts.markDelivered(carol, "tablet", group, 1);
        assertThat(receipts.receipts(alice, group).deliveredToAllUpTo()).isEqualTo(1);
        assertThat(receiptEvents(group, ReceiptKind.DELIVERED, null)).isEqualTo(1);

        receipts.markRead(bob, "phone", group, 1);
        receipts.markRead(carol, "tablet", group, 1);
        ReceiptsResponse state = receipts.receipts(alice, group);
        assertThat(state.readByAllUpTo()).isEqualTo(1);
        assertThat(receiptEvents(group, ReceiptKind.READ, null)).isEqualTo(1);
    }

    @Test
    void outsidersCannotAcknowledge() {
        long chat = newDirect(newUser(), newUser());
        long outsider = newUser();

        assertThatThrownBy(() -> receipts.markDelivered(outsider, "phone", chat, 1))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND));
    }

    private void send(long sender, long conversationId, int count) {
        for (int i = 0; i < count; i++) {
            messages.send(sender, conversationId, UUID.randomUUID(), payload("m" + i));
        }
    }

    private MemberCursors cursorsOf(long userId, long conversationId) {
        return receipts.receipts(userId, conversationId).members().stream()
                .filter(m -> m.userId() == userId)
                .findFirst()
                .orElseThrow();
    }

    /** @param userId null = the group-wide aggregate */
    private long receiptEvents(long conversationId, ReceiptKind kind, Long userId) {
        return jdbc.sql("""
                        SELECT count(*) FROM outbox
                        WHERE event_type = :type AND kafka_key = :key AND payload->>'kind' = :kind
                          AND (payload->>'userId') IS NOT DISTINCT FROM :userId
                        """)
                .param("type", ReceiptAdvancedEvent.TYPE)
                .param("key", String.valueOf(conversationId))
                .param("kind", kind.name())
                .param("userId", userId == null ? null : String.valueOf(userId))
                .query(Long.class)
                .single();
    }
}

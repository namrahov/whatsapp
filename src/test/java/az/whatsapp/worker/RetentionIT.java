package az.whatsapp.worker;

import static org.assertj.core.api.Assertions.assertThat;

import az.whatsapp.IntegrationTest;
import az.whatsapp.entity.MessageEntity;
import az.whatsapp.service.MessageService;
import az.whatsapp.service.MessageService.SyncPage;
import az.whatsapp.service.ReceiptService;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class RetentionIT extends IntegrationTest {

    @Autowired
    private RetentionJob retention;

    @Autowired
    private MessageService messages;

    @Autowired
    private ReceiptService receipts;

    @Test
    void deletesExpiredMessagesAndTellsSyncingClientsWhereHistoryStarts() {
        long alice = newUser();
        long bob = newUser();
        long chat = newDirect(alice, bob);
        for (int i = 1; i <= 3; i++) {
            messages.send(alice, chat, UUID.randomUUID(), payload("m" + i));
        }
        receipts.markDelivered(bob, "phone", chat, 3);
        backdate(chat, 2, "31 days");

        retention.deleteExpiredMessages();

        SyncPage page = messages.after(bob, chat, 0, null, 10);
        assertThat(page.messages()).extracting(MessageEntity::getSequenceNumber).containsExactly(3L);
        assertThat(page.firstAvailableSequence()).isEqualTo(3);
        // Expiry is not delivery and does not rewind anything: cursors and the sequence counter stay.
        assertThat(lastSequence(chat)).isEqualTo(3);
        assertThat(receipts.deviceCursorsOf(bob, "phone")).containsEntry(chat, 3L);
    }

    @Test
    void keepsMessagesYoungerThanTheTtl() {
        long alice = newUser();
        long chat = newDirect(alice, newUser());
        messages.send(alice, chat, UUID.randomUUID(), payload("fresh"));
        backdate(chat, 1, "29 days");

        retention.deleteExpiredMessages();

        assertThat(messages.after(alice, chat, 0, null, 10).messages()).hasSize(1);
    }

    @Test
    void deletesOnlyOldPublishedOutboxEvents() {
        long alice = newUser();
        long chat = newDirect(alice, newUser());
        messages.send(alice, chat, UUID.randomUUID(), payload("m"));
        jdbc.sql("""
                        UPDATE outbox SET status = 'PUBLISHED', published_at = now() - interval '8 days'
                        WHERE kafka_key = :key
                        """)
                .param("key", String.valueOf(chat))
                .update();

        retention.deletePublishedOutboxEvents();

        assertThat(jdbc.sql("SELECT count(*) FROM outbox WHERE kafka_key = :key")
                .param("key", String.valueOf(chat)).query(Long.class).single()).isZero();
    }

    /** Makes the first {@code count} messages of the conversation look {@code age} old. */
    private void backdate(long conversationId, int count, String age) {
        jdbc.sql("""
                        UPDATE messages SET created_at = now() - CAST(:age AS interval)
                        WHERE conversation_id = :c AND sequence_number <= :n
                        """)
                .param("age", age)
                .param("c", conversationId)
                .param("n", count)
                .update();
    }
}

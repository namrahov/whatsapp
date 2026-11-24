package az.whatsapp.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import az.whatsapp.IntegrationTest;
import az.whatsapp.dto.event.MessageChangedEvent;
import az.whatsapp.entity.MessageEntity;

import java.util.Base64;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class MessageEditIT extends IntegrationTest {

    @Autowired
    private MessageService messages;

    @Test
    void editRaisesTheVersionAndEntersTheChangeStream() {
        long alice = newUser();
        long bob = newUser();
        long chat = newDirect(alice, bob);
        MessageEntity original = send(alice, chat, "helo");

        MessageChangedEvent edited = messages.edit(alice, chat, original.getId(), payload("hello"));
        assertThat(edited.version()).isEqualTo(2);
        assertThat(edited.changeNumber()).isEqualTo(1);
        assertThat(edited.payload()).isEqualTo(base64("hello"));

        // The same edit again (a retry) changes nothing.
        assertThat(messages.edit(alice, chat, original.getId(), payload("hello")).version()).isEqualTo(2);
        assertThat(outboxEvents(MessageChangedEvent.TYPE, chat)).isEqualTo(1);

        // Bob was offline: he catches up through the change stream, the message kept its sequence.
        List<MessageEntity> changes = messages.changes(bob, chat, 0, 10);
        assertThat(changes).singleElement().satisfies(m -> {
            assertThat(m.getSequenceNumber()).isEqualTo(1);
            assertThat(m.getVersion()).isEqualTo(2);
            assertThat(m.getChangeNumber()).isEqualTo(1);
            assertThat(m.getEncryptedPayload()).isEqualTo(payload("hello"));
        });
        assertThat(messages.changes(bob, chat, 1, 10)).isEmpty();
    }

    @Test
    void deleteLeavesATombstoneInPlace() {
        long alice = newUser();
        long bob = newUser();
        long chat = newDirect(alice, bob);
        MessageEntity message = send(alice, chat, "oops");
        send(alice, chat, "next");

        MessageChangedEvent deleted = messages.delete(alice, chat, message.getId());
        assertThat(deleted.deleted()).isTrue();
        assertThat(deleted.payload()).isNull();
        assertThat(deleted.version()).isEqualTo(2);

        assertThat(messages.delete(alice, chat, message.getId()).version()).isEqualTo(2); // idempotent
        assertThat(outboxEvents(MessageChangedEvent.TYPE, chat)).isEqualTo(1);
        assertStatus(() -> messages.edit(alice, chat, message.getId(), payload("x")), HttpStatus.CONFLICT);

        // Still at sequence 1, so a syncing client sees no gap.
        List<MessageEntity> synced = messages.after(bob, chat, 0, null, 10).messages();
        assertThat(synced).extracting(MessageEntity::getSequenceNumber).containsExactly(1L, 2L);
        assertThat(synced.getFirst().isDeleted()).isTrue();
        assertThat(synced.getFirst().getEncryptedPayload()).isNull();
    }

    @Test
    void onlyTheSenderMayChangeAMessageAndOnlyWithinTheWindow() {
        long alice = newUser();
        long bob = newUser();
        long chat = newDirect(alice, bob);
        MessageEntity message = send(alice, chat, "hi");

        assertStatus(() -> messages.edit(bob, chat, message.getId(), payload("x")), HttpStatus.FORBIDDEN);
        assertStatus(() -> messages.delete(newUser(), chat, message.getId()), HttpStatus.NOT_FOUND);

        jdbc.sql("UPDATE messages SET created_at = now() - interval '20 minutes' WHERE id = :id")
                .param("id", message.getId())
                .update();
        assertStatus(() -> messages.edit(alice, chat, message.getId(), payload("late")), HttpStatus.CONFLICT);
        assertThat(messages.delete(alice, chat, message.getId()).deleted()).isTrue(); // 48 h delete window
    }

    @Test
    void aLateSendRetryOfAnEditedMessageIsStillADuplicate() {
        long alice = newUser();
        long chat = newDirect(alice, newUser());
        UUID clientMessageId = UUID.randomUUID();
        MessageEntity original = messages.send(alice, chat, clientMessageId, payload("v1")).message();
        messages.edit(alice, chat, original.getId(), payload("v2"));

        var retry = messages.send(alice, chat, clientMessageId, payload("v1"));

        assertThat(retry.duplicate()).isTrue();
        assertThat(retry.message().getId()).isEqualTo(original.getId());
    }

    private MessageEntity send(long sender, long conversationId, String text) {
        return messages.send(sender, conversationId, UUID.randomUUID(), payload(text)).message();
    }

    private static String base64(String text) {
        return Base64.getEncoder().encodeToString(payload(text));
    }

    private static void assertStatus(Runnable call, HttpStatus status) {
        assertThatThrownBy(call::run)
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(status));
    }
}

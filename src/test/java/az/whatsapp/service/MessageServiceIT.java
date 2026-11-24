package az.whatsapp.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import az.whatsapp.IntegrationTest;
import az.whatsapp.dto.event.MessageCreatedEvent;
import az.whatsapp.service.MessageService.SendResult;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.LongStream;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class MessageServiceIT extends IntegrationTest {

    @Autowired
    private MessageService messages;

    @Test
    void assignsGapFreeSequencesAndWritesOneOutboxEventPerMessage() {
        long alice = newUser();
        long bob = newUser();
        long chat = newDirect(alice, bob);

        for (int i = 1; i <= 3; i++) {
            SendResult result = messages.send(alice, chat, UUID.randomUUID(), payload("m" + i));
            assertThat(result.message().getSequenceNumber()).isEqualTo(i);
            assertThat(result.duplicate()).isFalse();
        }
        assertThat(lastSequence(chat)).isEqualTo(3);
        assertThat(outboxEvents(MessageCreatedEvent.TYPE, chat)).isEqualTo(3);
    }

    @Test
    void retryReturnsTheOriginalWithoutANewSequenceOrEvent() {
        long alice = newUser();
        long chat = newDirect(alice, newUser());
        UUID clientMessageId = UUID.randomUUID();

        SendResult first = messages.send(alice, chat, clientMessageId, payload("hello"));
        SendResult retry = messages.send(alice, chat, clientMessageId, payload("hello"));

        assertThat(retry.duplicate()).isTrue();
        assertThat(retry.message().getId()).isEqualTo(first.message().getId());
        assertThat(retry.message().getSequenceNumber()).isEqualTo(1);
        assertThat(lastSequence(chat)).isEqualTo(1);
        assertThat(outboxEvents(MessageCreatedEvent.TYPE, chat)).isEqualTo(1);
    }

    @Test
    void reusingAClientMessageIdForOtherContentIsAConflict() {
        long alice = newUser();
        long bob = newUser();
        long chat = newDirect(alice, bob);
        long otherChat = newDirect(alice, newUser());
        UUID clientMessageId = UUID.randomUUID();
        messages.send(alice, chat, clientMessageId, payload("hello"));

        assertStatus(() -> messages.send(alice, chat, clientMessageId, payload("other")), HttpStatus.CONFLICT);
        assertStatus(() -> messages.send(alice, otherChat, clientMessageId, payload("hello")), HttpStatus.CONFLICT);
    }

    @Test
    void outsidersCannotSendOrSync() {
        long chat = newDirect(newUser(), newUser());
        long outsider = newUser();

        assertStatus(() -> messages.send(outsider, chat, UUID.randomUUID(), payload("x")), HttpStatus.NOT_FOUND);
        assertStatus(() -> messages.after(outsider, chat, 0, null, 10), HttpStatus.NOT_FOUND);
        assertThat(lastSequence(chat)).isZero();
    }

    @Test
    void concurrentSendersGetDistinctGapFreeSequences() throws Exception {
        long alice = newUser();
        long bob = newUser();
        long chat = newDirect(alice, bob);
        int senders = 20;

        List<SendResult> results = runConcurrently(senders, i ->
                messages.send(i % 2 == 0 ? alice : bob, chat, UUID.randomUUID(), payload("m" + i)));

        assertThat(results).extracting(r -> r.message().getSequenceNumber())
                .containsExactlyInAnyOrderElementsOf(LongStream.rangeClosed(1, senders).boxed().toList());
        assertThat(lastSequence(chat)).isEqualTo(senders);
    }

    @Test
    void concurrentRetriesOfOneMessageStoreItOnce() throws Exception {
        long alice = newUser();
        long chat = newDirect(alice, newUser());
        UUID clientMessageId = UUID.randomUUID();

        List<SendResult> results = runConcurrently(10, i -> messages.send(alice, chat, clientMessageId, payload("once")));

        assertThat(results).extracting(r -> r.message().getId()).containsOnly(results.getFirst().message().getId());
        assertThat(results).filteredOn(r -> !r.duplicate()).hasSize(1);
        // The losers rolled back, their sequence increments included: no gap.
        assertThat(lastSequence(chat)).isEqualTo(1);
        assertThat(outboxEvents(MessageCreatedEvent.TYPE, chat)).isEqualTo(1);
    }

    private interface Indexed<T> {
        T call(int index) throws Exception;
    }

    /** Starts all tasks at the same moment, to make them race. */
    private static <T> List<T> runConcurrently(int count, Indexed<T> task) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                int index = i;
                Callable<T> call = () -> {
                    start.await();
                    return task.call(index);
                };
                futures.add(executor.submit(call));
            }
            start.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> future : futures) {
                results.add(future.get());
            }
            return results;
        }
    }

    private static void assertStatus(Runnable call, HttpStatus status) {
        assertThatThrownBy(call::run)
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(status));
    }
}

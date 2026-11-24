package az.whatsapp.controller;

import az.whatsapp.dto.event.MessageChangedEvent;
import az.whatsapp.dto.request.EditMessageRequest;
import az.whatsapp.dto.request.SendMessageRequest;
import az.whatsapp.dto.response.MessageResponse;
import az.whatsapp.dto.response.SendMessageResponse;
import az.whatsapp.security.CurrentUser;
import az.whatsapp.service.MessageService;
import az.whatsapp.service.SyncBulkhead;

import jakarta.validation.Valid;

import java.util.Base64;
import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Send (HTTP form of the SEND frame), reconnect sync, gap repair, edit, delete and the change stream.
 * (ACK / READ live in ConversationController, as cursors of a conversation.)
 */
@RestController
public class MessageController {

    public static final String FIRST_AVAILABLE_SEQUENCE_HEADER = "X-First-Available-Sequence";

    private final MessageService messages;
    private final SyncBulkhead syncBulkhead;

    public MessageController(MessageService messages, SyncBulkhead syncBulkhead) {
        this.messages = messages;
        this.syncBulkhead = syncBulkhead;
    }

    /** 201 when stored now, 200 when this was a retry of an already stored message (same ids returned). */
    @PostMapping("/api/conversations/{conversationId}/messages")
    public ResponseEntity<SendMessageResponse> send(@CurrentUser long userId, @PathVariable long conversationId,
                                                    @Valid @RequestBody SendMessageRequest request) {
        var result = messages.send(userId, conversationId, request.clientMessageId(), decode(request.payload()));
        return ResponseEntity.status(result.duplicate() ? HttpStatus.OK : HttpStatus.CREATED)
                .body(SendMessageResponse.from(result.message(), result.duplicate()));
    }

    /**
     * Reconnect sync ({@code afterSequence} only) or gap repair ({@code afterSequence} + {@code untilSequence}):
     * messages in (afterSequence, untilSequence], oldest first. During a reconnect storm these queries are
     * bounded by the sync bulkhead: beyond its limit the answer is 503 + Retry-After instead of a DB pile-up.
     */
    @GetMapping("/api/conversations/{conversationId}/messages")
    public ResponseEntity<List<MessageResponse>> after(@CurrentUser long userId, @PathVariable long conversationId,
                                                       @RequestParam(defaultValue = "0") long afterSequence,
                                                       @RequestParam(required = false) Long untilSequence,
                                                       @RequestParam(defaultValue = "50") int limit) {
        var page = syncBulkhead.run(() -> messages.after(userId, conversationId, afterSequence, untilSequence, limit));
        // Sequences below this were deleted by retention: the client skips them instead of waiting.
        return ResponseEntity.ok()
                .header(FIRST_AVAILABLE_SEQUENCE_HEADER, String.valueOf(page.firstAvailableSequence()))
                .body(page.messages().stream().map(MessageResponse::from).toList());
    }

    /** Edits and deletes after the client's change cursor, in order: catching up on changes to old messages. */
    @GetMapping("/api/conversations/{conversationId}/changes")
    public List<MessageResponse> changes(@CurrentUser long userId, @PathVariable long conversationId,
                                         @RequestParam(defaultValue = "0") long afterChange,
                                         @RequestParam(defaultValue = "50") int limit) {
        return syncBulkhead.run(() -> messages.changes(userId, conversationId, afterChange, limit)).stream()
                .map(MessageResponse::from)
                .toList();
    }

    /** Edit by the sender within the edit window. Members get a MESSAGE_CHANGED frame. */
    @PatchMapping("/api/conversations/{conversationId}/messages/{messageId}")
    public MessageChangedEvent edit(@CurrentUser long userId, @PathVariable long conversationId,
                                    @PathVariable long messageId, @Valid @RequestBody EditMessageRequest request) {
        return messages.edit(userId, conversationId, messageId, decode(request.payload()));
    }

    /** Delete for everyone by the sender within the delete window: leaves a tombstone, keeps the sequence. */
    @DeleteMapping("/api/conversations/{conversationId}/messages/{messageId}")
    public MessageChangedEvent delete(@CurrentUser long userId, @PathVariable long conversationId,
                                      @PathVariable long messageId) {
        return messages.delete(userId, conversationId, messageId);
    }

    static byte[] decode(String base64) {
        try {
            return Base64.getDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "payload must be Base64");
        }
    }
}

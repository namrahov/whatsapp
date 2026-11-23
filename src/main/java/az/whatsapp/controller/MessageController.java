package az.whatsapp.controller;

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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * HTTP equivalent of the WebSocket SEND frame, plus the reconnect sync. (ACK / READ live in
 * ConversationController, as cursors of a conversation.)
 */
@RestController
public class MessageController {

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
    public List<MessageResponse> after(@CurrentUser long userId, @PathVariable long conversationId,
                                       @RequestParam(defaultValue = "0") long afterSequence,
                                       @RequestParam(required = false) Long untilSequence,
                                       @RequestParam(defaultValue = "50") int limit) {
        return syncBulkhead.run(() -> messages.after(userId, conversationId, afterSequence, untilSequence, limit))
                .stream()
                .map(MessageResponse::from)
                .toList();
    }

    static byte[] decode(String base64) {
        try {
            return Base64.getDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "payload must be Base64");
        }
    }
}

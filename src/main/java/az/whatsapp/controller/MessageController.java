package az.whatsapp.controller;

import az.whatsapp.dto.request.SendMessageRequest;
import az.whatsapp.dto.response.MessageResponse;
import az.whatsapp.dto.response.SendMessageResponse;
import az.whatsapp.security.CurrentUser;
import az.whatsapp.service.MessageService;

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

    public MessageController(MessageService messages) {
        this.messages = messages;
    }

    /** 201 when stored now, 200 when this was a retry of an already stored message (same ids returned). */
    @PostMapping("/api/conversations/{conversationId}/messages")
    public ResponseEntity<SendMessageResponse> send(@CurrentUser long userId, @PathVariable long conversationId,
                                                    @Valid @RequestBody SendMessageRequest request) {
        var result = messages.send(userId, conversationId, request.clientMessageId(), decode(request.payload()));
        return ResponseEntity.status(result.duplicate() ? HttpStatus.OK : HttpStatus.CREATED)
                .body(SendMessageResponse.from(result.message(), result.duplicate()));
    }

    /** Reconnect / gap repair: messages with sequence greater than {@code afterSequence}, oldest first. */
    @GetMapping("/api/conversations/{conversationId}/messages")
    public List<MessageResponse> after(@CurrentUser long userId, @PathVariable long conversationId,
                                       @RequestParam(defaultValue = "0") long afterSequence,
                                       @RequestParam(defaultValue = "50") int limit) {
        return messages.after(userId, conversationId, afterSequence, limit).stream()
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

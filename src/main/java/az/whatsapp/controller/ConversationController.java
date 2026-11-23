package az.whatsapp.controller;

import az.whatsapp.dto.request.CreateDirectConversationRequest;
import az.whatsapp.dto.request.CreateGroupRequest;
import az.whatsapp.dto.request.CursorRequest;
import az.whatsapp.dto.response.ConversationResponse;
import az.whatsapp.dto.response.ReceiptsResponse;
import az.whatsapp.security.CurrentDevice;
import az.whatsapp.security.CurrentUser;
import az.whatsapp.service.ConversationService;
import az.whatsapp.service.ReceiptService;

import jakarta.validation.Valid;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/conversations")
public class ConversationController {

    private final ConversationService conversations;
    private final ReceiptService receipts;

    public ConversationController(ConversationService conversations, ReceiptService receipts) {
        this.conversations = conversations;
        this.receipts = receipts;
    }

    /** Idempotent: opening a chat with the same user again returns the existing conversation. */
    @PostMapping("/direct")
    public ConversationResponse direct(@CurrentUser long userId,
                                       @Valid @RequestBody CreateDirectConversationRequest request) {
        return ConversationResponse.from(conversations.getOrCreateDirect(userId, request.userId()));
    }

    @PostMapping("/group")
    @ResponseStatus(HttpStatus.CREATED)
    public ConversationResponse group(@CurrentUser long userId, @Valid @RequestBody CreateGroupRequest request) {
        return ConversationResponse.from(conversations.createGroup(userId, request.title(), request.memberIds()));
    }

    /** With X-Device-Id, each entry also says where this device's sync starts. */
    @GetMapping
    public List<ConversationResponse> mine(@CurrentUser long userId,
                                           @CurrentDevice(required = false) String deviceId) {
        Map<Long, Long> read = receipts.readCursorsOf(userId);
        Map<Long, Long> received = deviceId == null ? Map.of() : receipts.deviceCursorsOf(userId, deviceId);
        return conversations.listForMember(userId).stream()
                .map(c -> ConversationResponse.from(c, read.getOrDefault(c.getId(), 0L),
                        deviceId == null ? null : received.getOrDefault(c.getId(), 0L)))
                .toList();
    }

    @GetMapping("/{conversationId}/receipts")
    public ReceiptsResponse receipts(@CurrentUser long userId, @PathVariable long conversationId) {
        return receipts.receipts(userId, conversationId);
    }

    /** DELIVERED, cumulative: this device has stored everything up to upToSequence. */
    @PostMapping("/{conversationId}/delivered")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delivered(@CurrentUser long userId, @CurrentDevice String deviceId, @PathVariable long conversationId,
                          @Valid @RequestBody CursorRequest request) {
        receipts.markDelivered(userId, deviceId, conversationId, request.upToSequence());
    }

    /** READ, cumulative: the user has seen everything up to upToSequence (on this device). */
    @PostMapping("/{conversationId}/read")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void read(@CurrentUser long userId, @CurrentDevice String deviceId, @PathVariable long conversationId,
                     @Valid @RequestBody CursorRequest request) {
        receipts.markRead(userId, deviceId, conversationId, request.upToSequence());
    }
}

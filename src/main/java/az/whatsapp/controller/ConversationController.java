package az.whatsapp.controller;

import az.whatsapp.dto.request.CreateDirectConversationRequest;
import az.whatsapp.dto.request.CreateGroupRequest;
import az.whatsapp.dto.response.ConversationResponse;
import az.whatsapp.security.CurrentUser;
import az.whatsapp.service.ConversationService;

import jakarta.validation.Valid;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/conversations")
public class ConversationController {

    private final ConversationService conversations;

    public ConversationController(ConversationService conversations) {
        this.conversations = conversations;
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

    @GetMapping
    public List<ConversationResponse> mine(@CurrentUser long userId) {
        return conversations.listForMember(userId).stream().map(ConversationResponse::from).toList();
    }
}

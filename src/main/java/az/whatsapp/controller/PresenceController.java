package az.whatsapp.controller;

import az.whatsapp.dto.response.PresenceResponse;
import az.whatsapp.security.CurrentUser;
import az.whatsapp.service.PresenceService;

import java.util.List;
import java.util.Set;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PresenceController {

    private final PresenceService presence;

    public PresenceController(PresenceService presence) {
        this.presence = presence;
    }

    /**
     * {@code GET /api/presence?userIds=2,3,4} — for the chat list. Users the caller shares no conversation with
     * are left out of the answer.
     */
    @GetMapping("/api/presence")
    public List<PresenceResponse> presence(@CurrentUser long userId, @RequestParam Set<Long> userIds) {
        return presence.presenceOf(userId, userIds);
    }
}

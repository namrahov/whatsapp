package az.whatsapp.dto.response;

import java.time.Instant;

/** @param lastSeenAt when a connection of the user last closed; null if never recorded */
public record PresenceResponse(long userId, boolean online, Instant lastSeenAt) {
}

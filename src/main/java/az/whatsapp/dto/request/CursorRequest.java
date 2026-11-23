package az.whatsapp.dto.request;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/** "Everything in this conversation up to this sequence" — the HTTP form of the ACK / READ frames. */
public record CursorRequest(@NotNull @Positive Long upToSequence) {
}

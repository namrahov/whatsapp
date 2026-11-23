package az.whatsapp.dto.request;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record CreateDirectConversationRequest(@NotNull @Positive Long userId) {
}

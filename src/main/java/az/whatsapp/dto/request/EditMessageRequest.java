package az.whatsapp.dto.request;

import jakarta.validation.constraints.NotBlank;

/** @param payload the new ciphertext, Base64-encoded */
public record EditMessageRequest(@NotBlank String payload) {
}

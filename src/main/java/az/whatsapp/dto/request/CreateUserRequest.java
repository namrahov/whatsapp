package az.whatsapp.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record CreateUserRequest(
        @NotBlank @Pattern(regexp = "^[A-Za-z0-9_]{1,50}$") String username,
        @NotBlank @Size(max = 100) String displayName) {
}

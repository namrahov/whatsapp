package az.whatsapp.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.util.Set;

/** Members besides the creator; the design limit is 1000 members per group, creator included. */
public record CreateGroupRequest(
        @NotBlank @Size(max = 100) String title,
        @NotEmpty @Size(max = 999) Set<@NotNull @Positive Long> memberIds) {
}

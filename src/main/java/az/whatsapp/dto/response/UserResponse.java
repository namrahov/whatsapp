package az.whatsapp.dto.response;

import az.whatsapp.entity.UserEntity;

import java.time.Instant;

public record UserResponse(long id, String username, String displayName, Instant createdAt) {

    public static UserResponse from(UserEntity user) {
        return new UserResponse(user.getId(), user.getUsername(), user.getDisplayName(), user.getCreatedAt());
    }
}

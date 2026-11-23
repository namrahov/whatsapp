package az.whatsapp.security;

import jakarta.servlet.http.HttpServletRequest;

import java.util.Optional;

/**
 * Resolves which of the user's devices (phone, laptop, ...) is making the request. Delivery cursors are kept
 * per device. Like {@link CurrentUserProvider}, business code does not depend on how this is done.
 */
public interface CurrentDeviceProvider {

    /** @throws org.springframework.web.server.ResponseStatusException with 400 if missing or malformed */
    String requireDeviceId(HttpServletRequest request);

    /** @throws org.springframework.web.server.ResponseStatusException with 400 if present but malformed */
    Optional<String> deviceId(HttpServletRequest request);
}

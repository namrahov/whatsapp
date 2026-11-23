package az.whatsapp.security;

import jakarta.servlet.http.HttpServletRequest;

import java.util.Optional;
import java.util.regex.Pattern;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/**
 * Development-only: trusts the {@value #HEADER} header, chosen by the client (e.g. "phone", "laptop").
 * Like X-User-Id, a real deployment would take it from a validated token.
 */
@Component
public class HeaderCurrentDeviceProvider implements CurrentDeviceProvider {

    public static final String HEADER = "X-Device-Id";
    private static final Pattern VALID = Pattern.compile("^[A-Za-z0-9_-]{1,64}$");

    @Override
    public String requireDeviceId(HttpServletRequest request) {
        return deviceId(request)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.BAD_REQUEST, "Missing " + HEADER + " header"));
    }

    @Override
    public Optional<String> deviceId(HttpServletRequest request) {
        String value = request.getHeader(HEADER);
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        String deviceId = value.strip();
        if (!VALID.matcher(deviceId).matches()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Invalid " + HEADER + " header: 1-64 letters, digits, '_' or '-'");
        }
        return Optional.of(deviceId);
    }
}

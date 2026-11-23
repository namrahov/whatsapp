package az.whatsapp.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.server.ResponseStatusException;

class HeaderCurrentDeviceProviderTest {

    private final HeaderCurrentDeviceProvider provider = new HeaderCurrentDeviceProvider();

    @Test
    void returnsDeviceIdFromHeader() {
        assertThat(provider.requireDeviceId(requestWith(" phone-1 "))).isEqualTo("phone-1");
    }

    @Test
    void optionalLookupIsEmptyWithoutHeader() {
        assertThat(provider.deviceId(new MockHttpServletRequest())).isEmpty();
    }

    @Test
    void requiredLookupRejectsMissingHeader() {
        assertBadRequest(() -> provider.requireDeviceId(new MockHttpServletRequest()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"phone 1", "phone/1", "ü", "a234567890123456789012345678901234567890123456789012345678901234x"})
    void rejectsMalformedHeader(String value) {
        assertBadRequest(() -> provider.deviceId(requestWith(value)));
    }

    private static MockHttpServletRequest requestWith(String value) {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(HeaderCurrentDeviceProvider.HEADER, value);
        return request;
    }

    private static void assertBadRequest(Runnable call) {
        assertThatThrownBy(call::run)
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST));
    }
}

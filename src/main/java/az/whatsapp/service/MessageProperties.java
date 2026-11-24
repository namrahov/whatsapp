package az.whatsapp.service;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** How long after sending the sender may edit, or delete for everyone. A product decision, not a technical one. */
@ConfigurationProperties("whatsapp.messages")
public record MessageProperties(Duration editWindow, Duration deleteWindow) {

    public MessageProperties {
        if (editWindow == null || deleteWindow == null || editWindow.isNegative() || deleteWindow.isNegative()) {
            throw new IllegalArgumentException("whatsapp.messages edit-window and delete-window are required");
        }
    }
}

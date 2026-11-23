package az.whatsapp.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** Receives frames other instances routed to connections held by this instance. */
@Component
public class InstanceChannelListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(InstanceChannelListener.class);

    private final DeliveryRouter router;
    private final JsonMapper json;

    public InstanceChannelListener(DeliveryRouter router, JsonMapper json) {
        this.router = router;
        this.json = json;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            ChannelEnvelope envelope = json.readValue(message.getBody(), ChannelEnvelope.class);
            router.deliverLocally(envelope.userId(), envelope.connectionIds(), envelope.frame());
        } catch (RuntimeException e) {
            // Lost frame only: the message is stored and comes back on the client's next sync.
            log.warn("Could not deliver routed frame: {}", e.toString());
        }
    }
}

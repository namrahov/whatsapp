package az.whatsapp.gateway;

import java.util.List;

/** What one gateway sends another over Redis Pub/Sub: a frame for some of the target instance's connections. */
public record ChannelEnvelope(long userId, List<String> connectionIds, String frame) {
}

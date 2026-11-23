package az.whatsapp.worker;

import az.whatsapp.config.KafkaConfig;
import az.whatsapp.dto.event.MessageCreatedEvent;
import az.whatsapp.gateway.DeliveryRouter;
import az.whatsapp.gateway.Frames;
import az.whatsapp.repository.ConversationMemberRepository;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Turns a stored message into real-time pushes: every member except the sender, on every open connection.
 * <p>
 * The offset is committed once this method returns, i.e. when the pushes were handed off; it says nothing
 * about the user having received or read the message (that is the client ACK / READ). Offline members get
 * nothing here: the message waits in PostgreSQL until they reconnect and sync by sequence.
 * At-least-once: after a crash an event is processed again and a client may see a message twice; it drops
 * the duplicate by messageId.
 */
@Component
public class MessageDeliveryWorker {

    private final ConversationMemberRepository members;
    private final DeliveryRouter router;
    private final JsonMapper json;

    public MessageDeliveryWorker(ConversationMemberRepository members, DeliveryRouter router, JsonMapper json) {
        this.members = members;
        this.router = router;
        this.json = json;
    }

    @KafkaListener(topics = KafkaConfig.MESSAGE_EVENTS_TOPIC, groupId = "delivery")
    public void onMessageEvent(String payload, @Header(KafkaConfig.EVENT_TYPE_HEADER) String eventType) {
        if (!MessageCreatedEvent.TYPE.equals(eventType)) {
            return;
        }
        MessageCreatedEvent event = json.readValue(payload, MessageCreatedEvent.class);
        String frame = json.writeValueAsString(Frames.Message.of(event));
        for (long memberId : members.findUserIds(event.conversationId())) {
            if (memberId != event.senderId()) {
                router.deliver(memberId, frame);
            }
        }
    }
}

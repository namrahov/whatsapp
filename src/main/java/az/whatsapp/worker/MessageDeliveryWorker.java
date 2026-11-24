package az.whatsapp.worker;

import az.whatsapp.config.KafkaConfig;
import az.whatsapp.dto.event.MessageChangedEvent;
import az.whatsapp.dto.event.MessageCreatedEvent;
import az.whatsapp.dto.event.ReceiptAdvancedEvent;
import az.whatsapp.gateway.DeliveryRouter;
import az.whatsapp.gateway.Frames;
import az.whatsapp.repository.ConversationMemberRepository;

import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Turns stored events into real-time pushes.
 * <ul>
 *   <li>MessageCreated → MESSAGE frame to every member except the sender, on every open connection.</li>
 *   <li>ReceiptAdvanced → RECEIPT frame to all members or only to the user whose cursor moved
 *       (decided when the event was written, see ReceiptService).</li>
 * </ul>
 * The offset is committed once this method returns, i.e. when the pushes were handed off; it says nothing
 * about the user having received the frame. Offline members get nothing here and sync on reconnect.
 * At-least-once: after a crash an event is processed again. Clients drop a messageId they already have, and
 * receipts are cursors, so applying one twice changes nothing.
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
    public void onEvent(String payload, @Header(KafkaConfig.EVENT_TYPE_HEADER) String eventType) {
        switch (eventType) {
            case MessageCreatedEvent.TYPE -> onMessage(json.readValue(payload, MessageCreatedEvent.class));
            case MessageChangedEvent.TYPE -> onChange(json.readValue(payload, MessageChangedEvent.class));
            case ReceiptAdvancedEvent.TYPE -> onReceipt(json.readValue(payload, ReceiptAdvancedEvent.class));
            default -> {
                // unknown type from a newer version: skip rather than block the partition
            }
        }
    }

    private void onMessage(MessageCreatedEvent event) {
        String frame = json.writeValueAsString(Frames.Message.of(event));
        for (long memberId : members.findUserIds(event.conversationId())) {
            if (memberId != event.senderId()) {
                router.deliver(memberId, frame);
            }
        }
    }

    /** To every member, the sender included: the sender's other devices must show the edit / tombstone too. */
    private void onChange(MessageChangedEvent event) {
        String frame = json.writeValueAsString(Frames.MessageChanged.of(event));
        for (long memberId : members.findUserIds(event.conversationId())) {
            router.deliver(memberId, frame);
        }
    }

    private void onReceipt(ReceiptAdvancedEvent event) {
        String frame = json.writeValueAsString(Frames.Receipt.of(event));
        List<Long> recipients = event.toAllMembers() || event.userId() == null
                ? members.findUserIds(event.conversationId())
                : List.of(event.userId());
        for (long recipient : recipients) {
            router.deliver(recipient, frame);
        }
    }
}

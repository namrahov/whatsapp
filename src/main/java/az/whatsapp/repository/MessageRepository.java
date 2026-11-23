package az.whatsapp.repository;

import az.whatsapp.entity.MessageEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MessageRepository extends JpaRepository<MessageEntity, Long> {

    Optional<MessageEntity> findBySenderIdAndClientMessageId(long senderId, UUID clientMessageId);

    /** Reconnect / gap sync: everything after the client's last durably stored sequence, in order. */
    List<MessageEntity> findByConversationIdAndSequenceNumberGreaterThanOrderBySequenceNumberAsc(
            long conversationId, long afterSequence, Limit limit);

    /** Gap repair: exactly the missing range (afterSequence, untilSequence]. */
    List<MessageEntity> findByConversationIdAndSequenceNumberGreaterThanAndSequenceNumberLessThanEqualOrderBySequenceNumberAsc(
            long conversationId, long afterSequence, long untilSequence, Limit limit);
}

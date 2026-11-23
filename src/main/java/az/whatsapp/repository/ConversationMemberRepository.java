package az.whatsapp.repository;

import az.whatsapp.entity.ConversationMemberEntity;
import az.whatsapp.entity.ConversationMemberId;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface ConversationMemberRepository extends JpaRepository<ConversationMemberEntity, ConversationMemberId> {

    @Query("SELECT m.id.userId FROM ConversationMemberEntity m WHERE m.id.conversationId = :conversationId")
    List<Long> findUserIds(long conversationId);
}

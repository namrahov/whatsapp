package az.whatsapp.repository;

import az.whatsapp.entity.ConversationEntity;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface ConversationRepository extends JpaRepository<ConversationEntity, Long> {

    Optional<ConversationEntity> findByDirectKey(String directKey);

    @Query("""
            SELECT c FROM ConversationEntity c
            WHERE c.id IN (SELECT m.id.conversationId FROM ConversationMemberEntity m WHERE m.id.userId = :userId)
            ORDER BY c.id DESC
            """)
    List<ConversationEntity> findAllForMember(long userId);
}

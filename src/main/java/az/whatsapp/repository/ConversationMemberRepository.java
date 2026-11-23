package az.whatsapp.repository;

import az.whatsapp.entity.ConversationMemberEntity;
import az.whatsapp.entity.ConversationMemberId;

import java.util.Collection;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface ConversationMemberRepository extends JpaRepository<ConversationMemberEntity, ConversationMemberId> {

    @Query("SELECT m.id.userId FROM ConversationMemberEntity m WHERE m.id.conversationId = :conversationId")
    List<Long> findUserIds(long conversationId);

    /** Which of {@code userIds} share at least one conversation with {@code userId}. */
    @Query("""
            SELECT DISTINCT other.id.userId
            FROM ConversationMemberEntity mine, ConversationMemberEntity other
            WHERE mine.id.conversationId = other.id.conversationId
              AND mine.id.userId = :userId AND other.id.userId IN :userIds AND other.id.userId <> :userId
            """)
    List<Long> findPeersAmong(long userId, Collection<Long> userIds);
}

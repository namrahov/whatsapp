package az.whatsapp.service;

import az.whatsapp.entity.ConversationEntity;
import az.whatsapp.entity.ConversationMemberEntity;
import az.whatsapp.entity.ConversationMemberId;
import az.whatsapp.repository.ConversationMemberRepository;
import az.whatsapp.repository.ConversationRepository;
import az.whatsapp.repository.UserRepository;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

@Service
public class ConversationService {

    /** Interview assumption: groups of up to 1000 members, creator included. */
    public static final int MAX_GROUP_MEMBERS = 1000;

    private final ConversationRepository conversations;
    private final ConversationMemberRepository members;
    private final UserRepository users;
    private final TransactionTemplate tx;

    public ConversationService(ConversationRepository conversations, ConversationMemberRepository members,
                               UserRepository users, TransactionTemplate tx) {
        this.conversations = conversations;
        this.members = members;
        this.users = users;
        this.tx = tx;
    }

    /** Returns the existing 1-to-1 conversation between the two users, creating it on first use. */
    public ConversationEntity getOrCreateDirect(long userId, long peerId) {
        if (userId == peerId) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Cannot start a conversation with yourself");
        }
        requireUsersExist(Set.of(userId, peerId));
        String directKey = Math.min(userId, peerId) + ":" + Math.max(userId, peerId);
        return conversations.findByDirectKey(directKey).orElseGet(() -> {
            try {
                return tx.execute(status -> {
                    ConversationEntity created = conversations.saveAndFlush(ConversationEntity.direct(userId, directKey));
                    members.saveAll(List.of(new ConversationMemberEntity(created.getId(), userId),
                            new ConversationMemberEntity(created.getId(), peerId)));
                    return created;
                });
            } catch (DataIntegrityViolationException e) {
                // Both users opened the chat at the same moment and the other request created it first.
                return conversations.findByDirectKey(directKey).orElseThrow(() -> e);
            }
        });
    }

    public ConversationEntity createGroup(long creatorId, String title, Set<Long> memberIds) {
        Set<Long> all = new LinkedHashSet<>(memberIds);
        all.add(creatorId);
        if (all.size() < 2) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "A group needs at least one other member");
        }
        if (all.size() > MAX_GROUP_MEMBERS) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "A group can have at most " + MAX_GROUP_MEMBERS + " members");
        }
        requireUsersExist(all);
        return tx.execute(status -> {
            ConversationEntity created = conversations.saveAndFlush(ConversationEntity.group(creatorId, title.strip()));
            members.saveAll(all.stream().map(id -> new ConversationMemberEntity(created.getId(), id)).toList());
            return created;
        });
    }

    public List<ConversationEntity> listForMember(long userId) {
        return conversations.findAllForMember(userId);
    }

    /** 404 for both "no such conversation" and "not a member", so outsiders cannot probe which ids exist. */
    public void requireMember(long conversationId, long userId) {
        if (!members.existsById(new ConversationMemberId(conversationId, userId))) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Conversation not found");
        }
    }

    private void requireUsersExist(Set<Long> ids) {
        if (users.findAllById(ids).size() != ids.size()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found");
        }
    }
}

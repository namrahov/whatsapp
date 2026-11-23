package az.whatsapp.dto.response;

import az.whatsapp.repository.CursorRepository.MemberCursors;

import java.util.List;

/**
 * A message with sequence S is delivered to everyone when {@code S <= deliveredToAllUpTo} (two grey ticks) and
 * read by everyone when {@code S <= readByAllUpTo} (blue ticks). {@code members} is the per-person detail.
 */
public record ReceiptsResponse(long conversationId, long deliveredToAllUpTo, long readByAllUpTo,
                               List<MemberCursors> members) {
}

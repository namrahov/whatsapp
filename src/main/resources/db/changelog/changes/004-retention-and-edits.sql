--liquibase formatted sql

--changeset whatsapp:011-conversation-retention
-- Messages older than the retention period are deleted. first_available_sequence tells a syncing client that
-- everything before it is gone for good, so it does not wait for (or keep requesting) expired sequences.
ALTER TABLE conversations ADD COLUMN first_available_sequence BIGINT NOT NULL DEFAULT 1;
ALTER TABLE conversations ADD CONSTRAINT chk_conversations_first_available CHECK (first_available_sequence >= 1);
-- The cleanup job takes the oldest messages first.
CREATE INDEX idx_messages_created_at ON messages (created_at);
--rollback DROP INDEX idx_messages_created_at;
--rollback ALTER TABLE conversations DROP COLUMN first_available_sequence;

--changeset whatsapp:012-message-edits
-- Edit / delete-for-everyone. A message keeps its sequence (its place in the chat); what changes is:
--   version        per message, +1 per change: a client never lets an older version overwrite a newer one
--   change_number  per conversation, from conversations.last_change: "edits and deletes after N" is how an
--                  offline device catches up on changes to messages it already has (their sequence did not move)
-- A deleted message stays as a tombstone (no payload) so its sequence is not mistaken for a gap.
ALTER TABLE conversations ADD COLUMN last_change BIGINT NOT NULL DEFAULT 0;
ALTER TABLE messages ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE messages ADD COLUMN change_number BIGINT;
ALTER TABLE messages ADD COLUMN edited_at TIMESTAMPTZ;
ALTER TABLE messages ADD COLUMN deleted_at TIMESTAMPTZ;
ALTER TABLE messages ALTER COLUMN encrypted_payload DROP NOT NULL;
ALTER TABLE messages DROP CONSTRAINT chk_messages_payload_size;
ALTER TABLE messages ADD CONSTRAINT chk_messages_payload CHECK (
    (deleted_at IS NULL AND encrypted_payload IS NOT NULL AND octet_length(encrypted_payload) BETWEEN 1 AND 65536)
    OR (deleted_at IS NOT NULL AND encrypted_payload IS NULL)
);
ALTER TABLE messages ADD CONSTRAINT chk_messages_version CHECK (version >= 1);
CREATE UNIQUE INDEX uq_messages_conversation_change ON messages (conversation_id, change_number)
    WHERE change_number IS NOT NULL;
--rollback DROP INDEX uq_messages_conversation_change;
--rollback ALTER TABLE messages DROP CONSTRAINT chk_messages_version;
--rollback ALTER TABLE messages DROP CONSTRAINT chk_messages_payload;
--rollback ALTER TABLE messages ADD CONSTRAINT chk_messages_payload_size CHECK (octet_length(encrypted_payload) BETWEEN 1 AND 65536);
--rollback ALTER TABLE messages ALTER COLUMN encrypted_payload SET NOT NULL;
--rollback ALTER TABLE messages DROP COLUMN deleted_at;
--rollback ALTER TABLE messages DROP COLUMN edited_at;
--rollback ALTER TABLE messages DROP COLUMN change_number;
--rollback ALTER TABLE messages DROP COLUMN version;
--rollback ALTER TABLE conversations DROP COLUMN last_change;

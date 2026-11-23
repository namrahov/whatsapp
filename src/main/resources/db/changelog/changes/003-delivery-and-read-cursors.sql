--liquibase formatted sql

--changeset whatsapp:008-device-sync
-- DELIVERED as a cumulative cursor, like a TCP ACK: "this device has stored every message up to N".
-- One row per user, device and conversation instead of one row per message and recipient.
-- The foreign key to conversation_members means only members can have a cursor.
CREATE TABLE device_sync (
    user_id BIGINT NOT NULL,
    device_id VARCHAR(64) NOT NULL,
    conversation_id BIGINT NOT NULL,
    last_received_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, device_id, conversation_id),
    CONSTRAINT fk_device_sync_member FOREIGN KEY (conversation_id, user_id)
        REFERENCES conversation_members (conversation_id, user_id),
    CONSTRAINT chk_device_sync_device_id CHECK (device_id ~ '^[A-Za-z0-9_-]{1,64}$'),
    CONSTRAINT chk_device_sync_sequence CHECK (last_received_sequence >= 0)
);
-- "how far has every member got" (group aggregate, receipts endpoint)
CREATE INDEX idx_device_sync_conversation ON device_sync (conversation_id, user_id);
--rollback DROP TABLE device_sync;

--changeset whatsapp:009-conversation-read-state
-- READ is per user, not per device: read on the phone = read on the laptop.
-- Reading up to N implies delivered up to N. A user's own message counts as read by them.
CREATE TABLE conversation_read_state (
    conversation_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    last_read_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (conversation_id, user_id),
    CONSTRAINT fk_conversation_read_state_member FOREIGN KEY (conversation_id, user_id)
        REFERENCES conversation_members (conversation_id, user_id),
    CONSTRAINT chk_conversation_read_state_sequence CHECK (last_read_sequence >= 0)
);
-- "my read state in all my conversations" (unread counts)
CREATE INDEX idx_conversation_read_state_user ON conversation_read_state (user_id, conversation_id);
--rollback DROP TABLE conversation_read_state;

--changeset whatsapp:010-drop-message-receipts
-- Replaced by the two cursors above. Existing per-message receipts are not migrated: they carry no device id.
DROP TABLE message_receipts;
--rollback CREATE TABLE message_receipts (message_id BIGINT NOT NULL REFERENCES messages(id), user_id BIGINT NOT NULL REFERENCES users(id), delivered_at TIMESTAMPTZ NOT NULL, read_at TIMESTAMPTZ, PRIMARY KEY (message_id, user_id), CONSTRAINT chk_message_receipts_order CHECK (read_at IS NULL OR read_at >= delivered_at));

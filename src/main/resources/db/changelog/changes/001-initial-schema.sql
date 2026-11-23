--liquibase formatted sql

--changeset whatsapp:001-users
CREATE TABLE users (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_users_username CHECK (username ~ '^[A-Za-z0-9_]{1,50}$'),
    CONSTRAINT chk_users_display_name CHECK (length(btrim(display_name)) > 0)
);
CREATE UNIQUE INDEX uq_users_username_lower ON users (lower(username));
--rollback DROP TABLE users;

--changeset whatsapp:002-conversations
-- last_sequence is the per-conversation counter that gives every message its authoritative order.
-- direct_key ("smallerUserId:largerUserId") makes a 1-to-1 conversation between two users unique.
CREATE TABLE conversations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type VARCHAR(10) NOT NULL,
    title VARCHAR(100),
    direct_key VARCHAR(41),
    last_sequence BIGINT NOT NULL DEFAULT 0,
    created_by BIGINT NOT NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_conversations_type CHECK (type IN ('DIRECT', 'GROUP')),
    CONSTRAINT chk_conversations_last_sequence CHECK (last_sequence >= 0),
    CONSTRAINT chk_conversations_kind CHECK (
        (type = 'DIRECT' AND direct_key IS NOT NULL AND title IS NULL)
        OR (type = 'GROUP' AND direct_key IS NULL AND title IS NOT NULL AND length(btrim(title)) > 0)
    ),
    CONSTRAINT uq_conversations_direct_key UNIQUE (direct_key)
);
--rollback DROP TABLE conversations;

--changeset whatsapp:003-conversation-members
CREATE TABLE conversation_members (
    conversation_id BIGINT NOT NULL REFERENCES conversations(id),
    user_id BIGINT NOT NULL REFERENCES users(id),
    joined_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (conversation_id, user_id)
);
-- "my conversations" lookup; the primary key already serves "members of a conversation".
CREATE INDEX idx_conversation_members_user ON conversation_members (user_id, conversation_id);
--rollback DROP TABLE conversation_members;

--changeset whatsapp:004-messages
-- One row per logical message. The server stores ciphertext only (E2EE happens on devices).
-- uq_messages_sender_client_id: a retried send with the same clientMessageId is recognised, not stored twice.
-- uq_messages_conversation_sequence: no two messages share a position; also serves "messages after sequence N".
CREATE TABLE messages (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    conversation_id BIGINT NOT NULL REFERENCES conversations(id),
    sender_id BIGINT NOT NULL REFERENCES users(id),
    client_message_id UUID NOT NULL,
    sequence_number BIGINT NOT NULL,
    encrypted_payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_messages_sequence_number CHECK (sequence_number > 0),
    CONSTRAINT chk_messages_payload_size CHECK (octet_length(encrypted_payload) BETWEEN 1 AND 65536),
    CONSTRAINT uq_messages_sender_client_id UNIQUE (sender_id, client_message_id),
    CONSTRAINT uq_messages_conversation_sequence UNIQUE (conversation_id, sequence_number)
);
--rollback DROP TABLE messages;

--changeset whatsapp:005-message-receipts
-- DELIVERED = the recipient's device stored the message and acknowledged it; READ = the recipient opened it.
-- SENT is not stored here: it is the server's own durable acceptance (the messages row itself).
CREATE TABLE message_receipts (
    message_id BIGINT NOT NULL REFERENCES messages(id),
    user_id BIGINT NOT NULL REFERENCES users(id),
    delivered_at TIMESTAMPTZ NOT NULL,
    read_at TIMESTAMPTZ,
    PRIMARY KEY (message_id, user_id),
    CONSTRAINT chk_message_receipts_order CHECK (read_at IS NULL OR read_at >= delivered_at)
);
--rollback DROP TABLE message_receipts;

--changeset whatsapp:006-outbox
-- Transactional outbox: written in the same transaction as the message, published to Kafka afterwards.
-- kafka_key = conversation id, so all events of one conversation land in one partition, in order.
CREATE TABLE outbox (
    event_id UUID PRIMARY KEY,
    aggregate_id BIGINT NOT NULL,
    kafka_key VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,
    locked_until TIMESTAMPTZ,
    CONSTRAINT chk_outbox_event_type CHECK (length(btrim(event_type)) > 0),
    CONSTRAINT chk_outbox_status CHECK (status IN ('PENDING', 'PUBLISHED')),
    CONSTRAINT chk_outbox_attempts CHECK (attempts >= 0),
    CONSTRAINT chk_outbox_published_at CHECK (
        (status = 'PENDING' AND published_at IS NULL)
        OR (status = 'PUBLISHED' AND published_at IS NOT NULL)
    )
);
CREATE INDEX idx_outbox_pending ON outbox (created_at, event_id) WHERE status = 'PENDING';
CREATE INDEX idx_outbox_published ON outbox (published_at) WHERE status = 'PUBLISHED';
--rollback DROP TABLE outbox;

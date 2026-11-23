--liquibase formatted sql

--changeset whatsapp:007-outbox-created-at-clock
-- CURRENT_TIMESTAMP is the transaction START time. Two senders in one conversation can start in one order and get
-- the conversation row lock (= sequence numbers) in the other, so the publisher, which sends in created_at order,
-- could put them on Kafka out of sequence order. clock_timestamp() is taken at insert time, i.e. after the lock:
-- for one conversation, created_at order = sequence order.
ALTER TABLE outbox ALTER COLUMN created_at SET DEFAULT clock_timestamp();
--rollback ALTER TABLE outbox ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP;

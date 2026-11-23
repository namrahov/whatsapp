# WhatsApp — System Design, Built for Real

Learning project for topic 3/30 (WhatsApp system design): 1-to-1 and group chat, durable-first delivery,
per-conversation ordering, idempotent retries, ACKs, offline sync and presence. The design reference is
[docs/reference/03_WhatsApp_System_Design_Detailed.pdf](docs/reference/03_WhatsApp_System_Design_Detailed.pdf).

Stack: Java 26, Gradle, Spring Boot 4.1, JPA, PostgreSQL, Liquibase, Kafka, Redis, Spring WebSocket
(same as `../twitter`).

## Implemented so far (PDF sections 00–04, receipts from 13)

```
Client A ──WS/REST──> Message Service ── one transaction ──> conversations.last_sequence + 1
   ^                        │                                messages (ciphertext)
   └──── SENT (after commit)┘                                outbox
                                             │
                          OutboxPublisher ──> Kafka "message-events" (key = conversation id)
                                             │
                          MessageDeliveryWorker ──> Redis registry: where is B connected?
                                             │
                          Redis Pub/Sub ws:instance:<id> ──> gateway holding B ──WS──> Client B ──ACK──> DELIVERED
```

- **00 durable first**: a message is acknowledged (SENT) only after it is committed with its outbox row.
  PostgreSQL down → `503` / retryable `ERROR` frame, nothing is acknowledged.
- **01 cross-instance routing**: each gateway has a random instance id and listens on its own Redis channel.
  Registry `ws:user:<userId>` = hash connectionId → instanceId, TTL refreshed in batches. If nobody listens on a
  target channel (instance dead), its stale entries are dropped.
- **02 offline delivery**: offline users get nothing pushed; on reconnect they call
  `GET /api/conversations/{id}/messages?afterSequence=N`. The Kafka offset means "worker handled it",
  not "user read it".
- **03 ordering**: `sequence_number` from the conversation row (row lock, gap-free), `UNIQUE(conversation_id,
  sequence_number)`, Kafka key = conversation id.
- **04 retries**: the client generates `clientMessageId` once and reuses it; `UNIQUE(sender_id, client_message_id)`.
  A retry returns the original id/sequence (`duplicate: true`); the same id with other content is `409`.
- **Receipts as cursors (from 13)**: DELIVERED and READ are cumulative ("everything up to sequence N"), like a
  TCP ACK, and only move forward.
  - `device_sync(user, device, conversation, last_received_sequence)`: per device; also where that device's
    reconnect sync starts. Needs `X-Device-Id`.
  - `conversation_read_state(conversation, user, last_read_sequence)`: per user (read on phone = read on laptop).
    Read implies delivered; sending a message marks the chat read for the sender.
  - A cursor that moves writes a `ReceiptAdvanced` event to the outbox; the worker pushes a `RECEIPT` frame.
    1-to-1: to both members. Group: only "delivered to all / read by all" (the minimum over members) goes to
    everyone, a member's own read cursor goes to their other devices; detail via `GET .../receipts`.
- **Client rules**: display by sequence; drop a `messageId` already stored; on a gap (5 then 7) wait briefly,
  then fetch `afterSequence=5`; the server does not redeliver unacknowledged pushes.

Tables: `users`, `conversations`, `conversation_members`, `messages`, `device_sync`, `conversation_read_state`,
`outbox`.

## Run locally

- JDK 26: `C:\Users\AZINTELECOM\.jdks\temurin-26.0.2.1` (Project SDK and Gradle JVM in IntelliJ).
- PostgreSQL on `localhost:5433`, database `whatsapp`.
- Kafka (`localhost:9094`) and Redis (`localhost:6381`): `docker compose -f docker/docker-compose.yml up -d`.
  The ports differ from twitter's so both stacks can run at once.
- Port `8081` by default; start a second instance with `SERVER_PORT=8082` to see cross-instance delivery.

```
./gradlew bootRun
./gradlew test
```

## API

All calls except sign-up need `X-User-Id`; those marked *device* also need `X-Device-Id` (e.g. `phone`).
Payloads are ciphertext, Base64-encoded.

| Method | Path | |
|---|---|---|
| POST | `/api/users` | `{username, displayName}` |
| POST | `/api/conversations/direct` | `{userId}` — get-or-create the 1-to-1 chat |
| POST | `/api/conversations/group` | `{title, memberIds}` — up to 1000 members |
| GET | `/api/conversations` | `lastSequence`, `lastReadSequence` (unread = difference), with *device* also `lastReceivedSequence` (sync start) |
| POST | `/api/conversations/{id}/messages` | `{clientMessageId, payload}` — 201 new, 200 retry |
| GET | `/api/conversations/{id}/messages?afterSequence=&limit=` | sync, oldest first, max 100 |
| POST | `/api/conversations/{id}/delivered` | *device* · `{upToSequence}` — cumulative ACK |
| POST | `/api/conversations/{id}/read` | *device* · `{upToSequence}` — cumulative READ |
| GET | `/api/conversations/{id}/receipts` | per-member cursors + `deliveredToAllUpTo`, `readByAllUpTo` |

WebSocket `ws://localhost:8081/ws` (handshake headers `X-User-Id` and `X-Device-Id`; one socket = one device):

```
-> {"type":"SEND","conversationId":1,"clientMessageId":"<uuid>","payload":"aGVsbG8="}
<- {"type":"SENT","messageId":1,"sequenceNumber":1,"duplicate":false,...}
<- {"type":"MESSAGE","messageId":1,"conversationId":1,"senderId":1,"sequenceNumber":1,"payload":"..."}
-> {"type":"ACK","conversationId":1,"upToSequence":1}      stored on this device up to 1
-> {"type":"READ","conversationId":1,"upToSequence":1}     seen up to 1
<- {"type":"RECEIPT","conversationId":1,"userId":2,"kind":"READ","upToSequence":1}   userId null = all members
<- {"type":"ERROR","status":409,"reason":"...","retryable":false}
```

## Authentication (development)

Same as twitter: the caller's id comes from the `X-User-Id` header (the API-gateway pattern, where the gateway
validates the token and forwards the user id). A missing, non-numeric or non-positive value returns `401`, for the
WebSocket handshake too. Controllers get the id with `@CurrentUser long userId`; business code depends only on
`CurrentUserProvider`, so the header implementation can later be swapped for Spring Security / JWT.
The device works the same way: `X-Device-Id` (1–64 of `A-Za-z0-9_-`, else `400`), `@CurrentDevice String
deviceId`, `CurrentDeviceProvider`.

# WhatsApp — System Design, Built for Real

Learning project for topic 3/30 (WhatsApp system design): 1-to-1 and group chat, durable-first delivery,
per-conversation ordering, idempotent retries, ACKs, offline sync and presence. The design reference is
[docs/reference/03_WhatsApp_System_Design_Detailed.pdf](docs/reference/03_WhatsApp_System_Design_Detailed.pdf).

Stack: Java 26, Gradle, Spring Boot 4.1, JPA, PostgreSQL, Liquibase, Kafka, Redis, Spring WebSocket
(same as `../twitter`).

## Implemented so far (PDF sections 00–04, 07–10, 15, 21, receipts from 13)

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
- **07 online ordering and gaps**: clients display by sequence, not arrival order, with a reorder buffer, and
  repair a gap (5 then 7) after a short wait with `GET .../messages?afterSequence=5&untilSequence=6`. Sequences
  have no permanent holes (taken under the row lock, commits happen in sequence order), so a gap is always a lost
  or late push. Reference client code with tests: `src/test/java/az/whatsapp/client/ReorderBuffer.java`.
  The server does not redeliver unacknowledged pushes.
- **08–09 presence**: clients send `{"type":"HEARTBEAT"}` every ~15 s (answer `PONG`). The gateway keeps "last
  seen" per connection in memory only, so heartbeats cost no Redis write, and closes sockets silent for 45 s
  (half-open). Liveness in Redis is one **lease per gateway** (`ws:instance:<id>:alive`, 30 s TTL, renewed every
  10 s), not one key per connection: a connection is live while its gateway's lease is.
  - Online = at least one connection on a live gateway (any device).
  - Offline is seen at once on a clean close, after the lease TTL on a gateway crash, after the idle timeout on
    a half-open socket. That lag is accepted on purpose.
  - Stale registry entries of dead gateways are removed when a lookup meets them.
  - If Redis loses its data, each gateway notices (marker key gone) and re-registers its connections.
  - `GET /api/presence?userIds=` only shows people who share a conversation with the caller.
- **10 reconnect storm**: handshake admission control. A full gateway answers `503`; over the handshake rate
  (token bucket, 200/s) or over 5 live connections per user it answers `429`. Sync queries run behind a bulkhead
  (20 at once, else `503`). Every rejection has a **randomised** `Retry-After`, so rejected clients do not all
  return in the same second. Clients add exponential backoff with full jitter on top:
  `delay = Retry-After + random(0, min(30 s, 1 s * 2^attempt))`. Reference:
  `src/test/java/az/whatsapp/client/ReconnectBackoff.java`.
- **15 retention**: a background job deletes messages older than 30 days (example policy, delivered or not) in
  batches of 1000 with `SKIP LOCKED`, and raises the conversation's `first_available_sequence`. Sync answers carry
  `X-First-Available-Sequence` (also in `GET /api/conversations`), so a client behind it skips the expired range
  instead of waiting for it (`ReorderBuffer.skipTo`). Expiry is not delivery: cursors are not touched. The same job
  deletes outbox events published more than 7 days ago.
- **21 edit / delete for everyone**: only the sender, within 15 min (edit) / 48 h (delete). The message keeps its
  sequence; a delete leaves a tombstone (no payload), so it is not mistaken for a gap.
  - `version` per message (+1 per change): clients ignore an event older than what they show.
  - `change_number` per conversation (`conversations.last_change`): an offline device catches up with
    `GET .../changes?afterChange=N`, because edits do not move the sequence it syncs by.
  - Members (the sender's other devices too) get a `MESSAGE_CHANGED` frame through outbox → Kafka.

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
./gradlew test              # unit tests
./gradlew integrationTest   # *IT: real PostgreSQL, Kafka and Redis in Testcontainers (Docker must be running)
```

The integration tests cover sequences and idempotency (including concurrent senders and concurrent retries of one
message), receipt cursors and group aggregates, retention, and edit/delete.

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
| GET | `/api/conversations/{id}/messages?afterSequence=&untilSequence=&limit=` | sync / gap repair, oldest first, max 100; header `X-First-Available-Sequence` |
| PATCH | `/api/conversations/{id}/messages/{messageId}` | `{payload}` — edit (sender, 15 min) |
| DELETE | `/api/conversations/{id}/messages/{messageId}` | delete for everyone (sender, 48 h), leaves a tombstone |
| GET | `/api/conversations/{id}/changes?afterChange=&limit=` | edits/deletes after the client's change cursor |
| POST | `/api/conversations/{id}/delivered` | *device* · `{upToSequence}` — cumulative ACK |
| POST | `/api/conversations/{id}/read` | *device* · `{upToSequence}` — cumulative READ |
| GET | `/api/conversations/{id}/receipts` | per-member cursors + `deliveredToAllUpTo`, `readByAllUpTo` |
| GET | `/api/presence?userIds=2,3` | `online`, `lastSeenAt`; only users sharing a conversation with the caller |

WebSocket `ws://localhost:8081/ws` (handshake headers `X-User-Id` and `X-Device-Id`; one socket = one device):

```
-> {"type":"SEND","conversationId":1,"clientMessageId":"<uuid>","payload":"aGVsbG8="}
<- {"type":"SENT","messageId":1,"sequenceNumber":1,"duplicate":false,...}
<- {"type":"MESSAGE","messageId":1,"conversationId":1,"senderId":1,"sequenceNumber":1,"payload":"..."}
-> {"type":"ACK","conversationId":1,"upToSequence":1}      stored on this device up to 1
-> {"type":"READ","conversationId":1,"upToSequence":1}     seen up to 1
<- {"type":"RECEIPT","conversationId":1,"userId":2,"kind":"READ","upToSequence":1}   userId null = all members
<- {"type":"MESSAGE_CHANGED","messageId":1,"sequenceNumber":1,"version":2,"changeNumber":1,"deleted":false,"payload":"..."}
<- {"type":"ERROR","status":409,"reason":"...","retryable":false}
-> {"type":"HEARTBEAT"}     <- {"type":"PONG"}                every ~15 s
```

A rejected handshake answers `429`/`503` with `Retry-After` and opens no socket.

## Authentication (development)

Same as twitter: the caller's id comes from the `X-User-Id` header (the API-gateway pattern, where the gateway
validates the token and forwards the user id). A missing, non-numeric or non-positive value returns `401`, for the
WebSocket handshake too. Controllers get the id with `@CurrentUser long userId`; business code depends only on
`CurrentUserProvider`, so the header implementation can later be swapped for Spring Security / JWT.
The device works the same way: `X-Device-Id` (1–64 of `A-Za-z0-9_-`, else `400`), `@CurrentDevice String
deviceId`, `CurrentDeviceProvider`.

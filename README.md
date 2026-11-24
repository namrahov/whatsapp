# WhatsApp — System Design, Built for Real

A learning project that implements the core of a WhatsApp-style messenger as a working Spring Boot service:
1-to-1 and group chats, durable-first delivery, per-conversation ordering, idempotent retries, delivery and read
receipts, offline sync, presence, reconnect-storm protection, retention and edit/delete for everyone.

It follows topic 3/30 of the system-design series,
[docs/reference/03_WhatsApp_System_Design_Detailed.pdf](docs/reference/03_WhatsApp_System_Design_Detailed.pdf).
The numbers in brackets below, such as **[03]**, are section numbers in that PDF.

## Contents

- [Features](#features)
- [Architecture](#architecture)
- [How a message travels](#how-a-message-travels)
- [Design decisions](#design-decisions)
- [Data model](#data-model)
- [Project structure](#project-structure)
- [Getting started](#getting-started)
- [Configuration](#configuration)
- [Testing](#testing)
- [API](#api)
- [Authentication](#authentication-development)
- [Not implemented yet](#not-implemented-yet)

## Features

| Area | What it does |
|---|---|
| Messaging | 1-to-1 and group chats (up to 1000 members), payloads stored as opaque ciphertext |
| Reliability | A message is acknowledged only after it is committed; it cannot be lost after `SENT` |
| Ordering | Gap-free sequence number per conversation; clients display by sequence, not arrival |
| Idempotency | Client-generated `clientMessageId`; a retry returns the original message |
| Delivery | Real-time push over WebSocket across many gateway instances; pull-based sync for offline devices |
| Receipts | Cumulative cursors: delivered per device, read per user, group "delivered to all / read by all" |
| Presence | Online / last seen, heartbeats, half-open socket detection, gateway crash detection |
| Overload | Handshake rate limit, connection limits, sync bulkhead, randomised `Retry-After` |
| Retention | Messages expire after 30 days; clients are told where history now starts |
| Edit / delete | Sender can edit (15 min) or delete for everyone (48 h); offline devices catch up via a change stream |

## Architecture

```
                         ┌──────────────── one transaction ────────────────┐
Client A ──WS / REST──►  Message Service ──► conversations.last_sequence + 1 │
    ▲                         │              messages (ciphertext)           │
    └──── SENT (after commit)─┘              outbox                          │
                                             └───────────────────────────────┘
                                                        │
                                OutboxPublisher (scheduled) ──► Kafka "message-events"
                                                        │        key = conversation id
                                                        ▼
                                              MessageDeliveryWorker
                                                │                  │
                         Redis registry ws:user:<id>        Redis Pub/Sub ws:instance:<id>
                         (which gateway holds B?)                  │
                                                                   ▼
                                                     Gateway holding B ──WS──► Client B
                                                                               │
                                                     ACK / READ (cumulative) ◄─┘
```

| Component | Role |
|---|---|
| **Gateway** (`gateway/`) | Holds client WebSockets, registers them in Redis, receives pushes for its own users on its Redis channel |
| **Message Service** (`service/`) | Validates membership, assigns the sequence, stores message + outbox event in one transaction |
| **OutboxPublisher** (`worker/`) | Moves committed outbox rows to Kafka (claim → send → mark published) |
| **Kafka** | Durable event log between "stored" and "delivered"; keyed by conversation so its events stay in order |
| **MessageDeliveryWorker** (`worker/`) | Consumes events, finds recipients and their gateways, pushes frames |
| **Redis** | Ephemeral state only: connection registry, gateway leases, Pub/Sub, last seen. Messages never live here |
| **PostgreSQL** | Source of truth: users, conversations, members, messages, cursors, outbox |

Every gateway instance is interchangeable. Start two instances and a message sent on one is delivered to a
user connected to the other.

## How a message travels

1. **Send.** The client creates a `clientMessageId` (UUID) once and sends
   `{"type":"SEND","conversationId":1,"clientMessageId":"…","payload":"…"}` over WebSocket, or
   `POST /api/conversations/{id}/messages`.
2. **Store.** In one transaction the service:
   - checks that the sender is a member;
   - runs `UPDATE conversations SET last_sequence = last_sequence + 1 RETURNING last_sequence` (the row lock makes
     the number unique and gap-free);
   - inserts the message;
   - inserts a `MessageCreated` event into `outbox`.
3. **Acknowledge.** After the commit the sender gets `SENT` with the message id and sequence (one tick).
   If PostgreSQL is down the sender gets a retryable error and nothing is acknowledged.
4. **Publish.** `OutboxPublisher` sends the event to Kafka and marks the row published.
5. **Route.** `MessageDeliveryWorker` reads the members from `conversation_members`, looks up each member's live
   connections in the Redis registry, and publishes the frame to each gateway's channel.
6. **Push.** The gateway writes a `MESSAGE` frame to the recipient's socket.
7. **Receipt.** The recipient's device stores the message and sends `ACK` (two ticks), later `READ` (blue ticks).
   The cursor moves, a `ReceiptAdvanced` event goes through the outbox, and the sender gets a `RECEIPT` frame.
8. **Offline.** If the recipient is offline, nothing is pushed. The message waits in PostgreSQL; on reconnect the
   device calls `GET /api/conversations/{id}/messages?afterSequence=<its cursor>`.

## Design decisions

### Durable first [00]
A message is acknowledged only after it is committed together with its outbox row. The transactional outbox
removes the "saved to the DB but the Kafka send failed" window: both happen or neither does.

### Cross-instance routing [01]
Each gateway has a random instance id and subscribes to its own Redis channel `ws:instance:<id>`. The registry
`ws:user:<userId>` is a hash `connectionId → instanceId` whose TTL is refreshed in batches. If a `PUBLISH`
reaches no subscriber, that instance is dead and its entries are removed.

### Offline delivery [02]
Push is an optimisation; sync is the guarantee. A Kafka offset means "the worker handled it", not "the user got it".

### Ordering [03, 07]
- `sequence_number` comes from the conversation row under a lock, with `UNIQUE(conversation_id, sequence_number)`.
- Commit order equals sequence order, so sequences have no permanent holes. A gap seen by a client (5, then 7)
  is always a late or lost push.
- Clients keep a reorder buffer and repair a gap after a short wait with
  `GET …/messages?afterSequence=5&untilSequence=6`.
  Reference client: [`ReorderBuffer`](src/test/java/az/whatsapp/client/ReorderBuffer.java).

### Idempotent retries [04]
`UNIQUE(sender_id, client_message_id)`. A retry returns the original id and sequence with `duplicate: true` and
writes no new event. The same id with different content is `409`. Concurrent retries store the message once.

### Receipts as cursors [13]
Receipts are cumulative ("everything up to sequence N") and only move forward, like a TCP ACK. One row per
device or user instead of one row per message per recipient.
- `device_sync` — per device: what this device has received; also where its reconnect sync starts.
- `conversation_read_state` — per user: read on the phone means read on the laptop. Read implies delivered,
  and sending a message marks the chat read for the sender.
- 1-to-1: receipt events go to both members. Groups: only the minimum over members ("delivered to all",
  "read by all") goes to everyone; details via `GET …/receipts`.

### Presence [08, 09]
- Clients send `HEARTBEAT` every ~15 s. The gateway records it in memory only, so heartbeats cost no Redis write,
  and closes sockets that are silent for 45 s (half-open connections).
- Liveness in Redis is one **lease per gateway** (`ws:instance:<id>:alive`, 30 s TTL, renewed every 10 s), not
  one key per connection. A connection is live while its gateway's lease is.
- Offline is seen immediately on a clean close, after the lease TTL when a gateway crashes, and after the idle
  timeout on a half-open socket. That lag is accepted on purpose.
- If Redis loses its data, each gateway notices (its marker key is gone) and re-registers its connections.
- Privacy: presence is visible only to users who share a conversation.

### Reconnect storm [10]
When a gateway dies, its clients all reconnect at once. Protection:
- handshake rate limit (token bucket, 200/s) and at most 5 connections per user → `429`;
- a full gateway → `503`;
- sync queries behind a bulkhead (20 at once) → `503`;
- every rejection carries a **randomised** `Retry-After`, and clients add exponential backoff with full jitter.
  Reference client: [`ReconnectBackoff`](src/test/java/az/whatsapp/client/ReconnectBackoff.java).

### Retention [15]
A background job deletes messages older than 30 days in batches with `SKIP LOCKED` and raises the conversation's
`first_available_sequence`. Sync responses carry `X-First-Available-Sequence`, so a client that is behind skips
the expired range instead of waiting for it. Published outbox rows older than 7 days are deleted too.

### Edit and delete for everyone [21]
- Only the sender, within 15 minutes (edit) or 48 hours (delete).
- The message keeps its sequence. A delete leaves a tombstone (no payload), so it is not mistaken for a gap.
- `version` per message lets clients ignore stale events.
- `change_number` per conversation feeds a change stream: an offline device catches up with
  `GET …/changes?afterChange=N`, because edits do not move the sequence it syncs by.

## Data model

| Table | Purpose |
|---|---|
| `users` | Accounts |
| `conversations` | Chat (direct or group); `last_sequence`, `last_change`, `first_available_sequence` |
| `conversation_members` | Who belongs to which chat; used for authorization, recipients and group aggregates |
| `messages` | Ciphertext, sequence, `client_message_id`, version, edit/delete markers |
| `device_sync` | Delivered cursor per (user, device, conversation) |
| `conversation_read_state` | Read cursor per (conversation, user) |
| `outbox` | Events waiting to be published to Kafka |

Schema changes are Liquibase changesets in
[`src/main/resources/db/changelog/changes`](src/main/resources/db/changelog/changes). Applied changesets are never
edited; every change gets a new one.

## Project structure

```
src/main/java/az/whatsapp
├── config/       Kafka, Redis, WebSocket and web configuration
├── controller/   REST endpoints and error handling
├── dto/          Requests, responses and Kafka event payloads
├── entity/       JPA entities
├── gateway/      WebSocket handler, connection registry, leases, admission control, frames
├── repository/   Spring Data repositories and native SQL (sequences, cursors, outbox, retention)
├── security/     X-User-Id / X-Device-Id resolution
├── service/      Business logic: messages, receipts, conversations, presence
└── worker/       Outbox publisher, delivery worker, retention job
src/test/java/az/whatsapp
├── client/       Reference client logic (reorder buffer, reconnect backoff) with tests
└── …IT.java      Integration tests against real PostgreSQL, Kafka and Redis
```

## Getting started

### Prerequisites
- JDK 26 (set it as the Project SDK and the Gradle JVM in IntelliJ)
- PostgreSQL on `localhost:5433` with a database named `whatsapp`
- Docker (for Kafka and Redis, and for the integration tests)

### Run
```
docker compose -f docker/docker-compose.yml up -d    # Kafka on 9094, Redis on 6381
./gradlew bootRun                                     # http://localhost:8081
```
Liquibase creates the schema on startup. The ports differ from the `twitter` project so both stacks can run at
the same time.

To see cross-instance delivery, start a second instance on another port:
```
SERVER_PORT=8082 ./gradlew bootRun
```

### Quick try
```
curl -X POST localhost:8081/api/users -H "Content-Type: application/json" -d '{"username":"ali","displayName":"Ali"}'
curl -X POST localhost:8081/api/users -H "Content-Type: application/json" -d '{"username":"ahmad","displayName":"Ahmad"}'
curl -X POST localhost:8081/api/conversations/direct -H "X-User-Id: 1" -H "Content-Type: application/json" -d '{"userId":2}'
curl -X POST localhost:8081/api/conversations/1/messages -H "X-User-Id: 1" -H "Content-Type: application/json" \
     -d '{"clientMessageId":"6f1c2c3e-0000-4000-8000-000000000001","payload":"aGVsbG8="}'
curl "localhost:8081/api/conversations/1/messages?afterSequence=0" -H "X-User-Id: 2"
```

## Configuration

Defaults live in [`application.yml`](src/main/resources/application.yml); each can be overridden with an
environment variable.

| Variable | Default | Meaning |
|---|---|---|
| `SERVER_PORT` | `8081` | HTTP / WebSocket port |
| `SPRING_DATASOURCE_PASSWORD` | — | Database password (keep it out of source control) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka brokers |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6381` | Redis |
| `GATEWAY_LEASE_TTL` | `30s` | How long a crashed gateway still looks alive |
| `GATEWAY_IDLE_TIMEOUT` | `45s` | Silent sockets are closed after this |
| `ADMISSION_HANDSHAKES_PER_SECOND` | `200` | WebSocket handshakes accepted per second per instance |
| `ADMISSION_MAX_CONNECTIONS_PER_INSTANCE` | `10000` | Connections per gateway |
| `ADMISSION_MAX_CONNECTIONS_PER_USER` | `5` | Devices per user |
| `ADMISSION_SYNC_MAX_CONCURRENT` | `20` | Concurrent sync queries |
| `RETENTION_MESSAGE_TTL` | `30d` | Message lifetime on the server |
| `RETENTION_OUTBOX_TTL` | `7d` | Lifetime of published outbox rows |
| `MESSAGES_EDIT_WINDOW` / `MESSAGES_DELETE_WINDOW` | `15m` / `48h` | Edit and delete-for-everyone windows |

## Testing

```
./gradlew test              # unit tests
./gradlew integrationTest   # *IT classes: PostgreSQL, Kafka and Redis in Testcontainers (Docker must be running)
```

The integration tests cover gap-free sequences, idempotency (including concurrent senders and concurrent retries
of one message), receipt cursors and group aggregates, retention, and edit/delete.

## API

All calls except sign-up need `X-User-Id`. Calls marked *device* also need `X-Device-Id` (for example `phone`).
Payloads are ciphertext, Base64-encoded.

### REST

| Method | Path | Description |
|---|---|---|
| POST | `/api/users` | `{username, displayName}` |
| GET | `/api/users/{id}` | User by id |
| POST | `/api/conversations/direct` | `{userId}` — get or create the 1-to-1 chat |
| POST | `/api/conversations/group` | `{title, memberIds}` — up to 1000 members |
| GET | `/api/conversations` | My chats: `lastSequence`, `lastReadSequence` (unread = difference); with *device* also `lastReceivedSequence` |
| POST | `/api/conversations/{id}/messages` | `{clientMessageId, payload}` — `201` new, `200` retry |
| GET | `/api/conversations/{id}/messages?afterSequence=&untilSequence=&limit=` | Sync / gap repair, oldest first, max 100; header `X-First-Available-Sequence` |
| PATCH | `/api/conversations/{id}/messages/{messageId}` | `{payload}` — edit (sender, 15 min) |
| DELETE | `/api/conversations/{id}/messages/{messageId}` | Delete for everyone (sender, 48 h), leaves a tombstone |
| GET | `/api/conversations/{id}/changes?afterChange=&limit=` | Edits and deletes after the client's change cursor |
| POST | `/api/conversations/{id}/delivered` | *device* · `{upToSequence}` — cumulative ACK |
| POST | `/api/conversations/{id}/read` | *device* · `{upToSequence}` — cumulative READ |
| GET | `/api/conversations/{id}/receipts` | Per-member cursors + `deliveredToAllUpTo`, `readByAllUpTo` |
| GET | `/api/presence?userIds=2,3` | `online`, `lastSeenAt`; only users sharing a chat with the caller |

### WebSocket

`ws://localhost:8081/ws` with handshake headers `X-User-Id` and `X-Device-Id`. One socket = one device.

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

A rejected handshake answers `429` or `503` with `Retry-After` and opens no socket.

## Authentication (development)

As in the `twitter` project, the caller's id comes from the `X-User-Id` header (the API-gateway pattern: the
gateway validates the token and forwards the user id). A missing, non-numeric or non-positive value returns `401`,
for the WebSocket handshake too. Controllers receive it with `@CurrentUser long userId`; business code depends
only on `CurrentUserProvider`, so the header implementation can later be replaced with Spring Security / JWT.

The device works the same way: `X-Device-Id` (1–64 characters of `A-Za-z0-9_-`, otherwise `400`),
`@CurrentDevice String deviceId`, `CurrentDeviceProvider`.

## Not implemented yet

- Adding and removing group members after creation
- Pipelined fan-out for very large groups [06, 22]
- Pushing a sender's own messages to their other devices [13]
- End-to-end encryption key distribution [14]
- Media upload and storage
- Edit and delete over WebSocket frames (REST only today)
- Observability: metrics, tracing, dashboards [23]
- Real authentication (JWT) instead of trusted headers

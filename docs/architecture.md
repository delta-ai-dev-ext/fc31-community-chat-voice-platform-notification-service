# Architecture — notification_service

Event-driven fan-in service: it consumes five domain-event contracts, turns them into member-facing notifications, and fans out live push events and transactional emails.

## Service responsibilities

- Turn domain events into in-app notifications for five kinds: `mention`, `friend_request_received`, `friend_request_accepted`, `moderation_notice`, `appeal_decided`.
- Persist the in-app **notification inbox** (unread/read) and publish `notification_created` (EVT-011) for live push.
- Send notification emails via Resend according to per-user preferences, only to verified emails of active accounts, with per-recipient throttling (at most one mention email per recipient per 15 minutes; moderation notices always sent).
- Keep a local projection of recipient contact data (email, verification status, display name, account status) from EVT-001.

Out of scope: WebSocket delivery (owned by `realtime_gateway`), notification creation via any synchronous API (everything is event-driven), and email content templating beyond notification title/summary.

## Internal component boundaries

| Component | Responsibility |
|---|---|
| REST API layer | Serves the member-facing inbox and preferences endpoints (see `openapi_spec.yaml`) plus `/healthz`, `/readyz`. Thin; delegates to the domain layer. |
| Event consumer | One shared consumer group reading `identity_events`, `moderation_events` (EVT-004, EVT-006), `social_events` (EVT-007), `message_events` (EVT-009). Validates the mandatory envelope, dedupes on `envelope.event_id`, and dispatches per event type. |
| Notification domain service | Maps each consumed event to zero or more notifications (kind, title, summary, deep link, recipient); applies per-kind preference and safety rules. |
| Email pipeline | Decides whether an email is due (preferences, verification, throttling), renders and sends via Resend, records every outcome on `EmailDelivery`. |
| Outbox publisher | Publishes `notification_created` (EVT-011) from the transactional outbox after the notification is committed. |
| Recipient projection | Upserts recipient contact data from EVT-001 with stale-version rejection. |

## Event flows (consumed → notification)

Every consumed message carries the mandatory envelope (see `engineering_guide.md` §7). Consumers deduplicate on `envelope.event_id`; each incoming event is acknowledged only after its local transaction commits.

### EVT-001 `user_account_changed` (topic `identity_events`, key `user_id`)

- Upserts the `RecipientProjection` (email, username, display_name, email_verified, status).
- Applied only when `payload.account_version` is newer than the stored one; stale snapshots are ignored.
- `change: "email_changed"` means subsequent notification emails go to the new address; deliveries already in flight are not recalled.

### EVT-009 `message_changed` (topic `message_events`, key `container_id`)

- `change: "created"` with non-empty `mentioned_user_ids`: one `mention` notification per mentioned user **except the author**, with title/summary identifying the author and message context and `link` deep-linking to `/channels/{container_id}/messages/{message_id}` (channel messages) or `/conversations/{container_id}/messages/{message_id}` (conversation messages).
- `change: "edited"`, `"deleted"`, `"reactions_changed"`, or `"created"` without mentions: consumed, no-op.

### EVT-007 `friend_request_changed` (topic `social_events`, key `recipient_user_id`)

- `status: "pending"` → `friend_request_received` notification for `recipient_user_id`.
- `status: "accepted"` → `friend_request_accepted` notification for `sender_user_id`.
- `status: "declined"` / `"cancelled"` → consumed, no-op.

### EVT-004 `member_sanction_changed` (topic `moderation_events`, key `community_id`)

- Every action (`mute|unmute|kick|ban|unban`) creates a `moderation_notice` notification for `target_user_id`.
- The notice carries `reason` and `expires_at` when present; when `appealable` is true the summary/link points to the appeal flow.
- `source: "expiry"` (`actor_user_id: null`, automatic unmute): notice still created; the title reflects that the mute expired.
- In-app moderation notices ignore the recipient's preferences (never silenced).

### EVT-006 `appeal_status_changed` (topic `moderation_events`, key `community_id`)

- `status: "upheld"` or `"overturned"` → `appeal_decided` notification for `appellant_user_id`, including `decision_note` in the summary when present.
- `status: "submitted"` → consumed, no-op.

### Processing pipeline (all events)

1. Validate the envelope (reject malformed messages once, log raw `event_id`, count in the error metric — no endless reprocessing).
2. Deduplicate on `envelope.event_id` (`processed_events`).
3. For each derived notification: resolve preferences, resolve the recipient projection, and insert the notification, its `email_delivery` row and an outbox row in **one transaction**.
4. After commit: send the email (if due) and publish EVT-011 from the outbox.

## Email pipeline

- **Eligibility:** an email is attempted only when the recipient projection has `email_verified: true` and `status: "active"`. Other outcomes are recorded, never silently dropped:
  - `skipped_preference` — the recipient turned the kind's email toggle off (moderation notices bypass this).
  - `skipped_unverified`, `skipped_deactivated` — in-app notification still created.
  - `throttled` — mention emails only (see below).
- **Throttling:** at most one mention email per recipient per 15-minute window; the window is persisted in PostgreSQL so it survives restarts. Moderation-notice emails are never throttled.
- **No projection yet:** the notification is created; the email is retried within a bounded retry budget and finally recorded as `failed` without blocking anything else.
- **Resend integration:** official SDK, idempotency key = notification id (safe under at-least-once consumption). 429/5xx retried with bounded exponential back-off; permanent 4xx recorded immediately. Sending rate kept under the shared account limit (identity_service has priority for verification/reset traffic).
- **Preference semantics:** the `moderation_notice` kind's stored toggles are honored in the API response, but moderation notices are always delivered in-app and by email (safety notices).

## Event flow (produced)

### EVT-011 `notification_created` (topic `notification_events`, key `recipient_user_id`)

- Published for every persisted notification, carrying the notification snapshot and the recipient's current `unread_count`.
- **Transactional outbox:** the notification insert and the outbox row commit together; the publisher drains the outbox afterwards and retries until the broker accepts. A publish failure never loses the notification and never double-pushes for a redelivered cause (dedupe prevents the second notification).
- Envelope: `trace_id` and `correlation_id` copied from the causing event; `causation_id` = causing event's `event_id` (EVT-011 is always caused by a consumed event, so it is never `null`).

## Inter-Service Contracts

### Envelope (mandatory, every event)

Every message on every topic is a JSON object with exactly two top-level keys: `envelope` and `payload`.

| Field | Type | Required | Rule |
|---|---|---|---|
| `event_id` | string (uuid) | yes | Unique per event. Consumer deduplication key. |
| `event_type` | string (snake_case) | yes | Must match a registered contract. |
| `event_version` | integer | yes | >= 1. A breaking payload change bumps it. |
| `occurred_at` | string (RFC3339 UTC) | yes | Producer clock at emit time. |
| `producer` | string (snake_case) | yes | Must equal the registered producer. |
| `trace_id` | string (32 lowercase hex) | yes | **Never empty.** Propagated from the originating request; minted at the edge when absent. |
| `correlation_id` | string (uuid) | yes | Groups every event of one business flow. |
| `causation_id` | string (uuid) or null | yes | `event_id` of the causing event. `null` only for the flow's first event. |
| `schema_ref` | string | yes | `EVENT_CONTRACTS.md#<event_type>-v<version>`. |

Rules:
- The envelope is not optional and not per-topic. A topic needing a field the envelope lacks adds it to `payload`, never to the envelope.
- A payload schema MUST NOT declare an envelope field name at its top level.
- `trace_id` is mandatory even when no HTTP request originated the flow: the producer mints one at the edge of its own flow.
- When a consumer emits an event because of another event, it copies `trace_id` and `correlation_id` from the causing event and sets `causation_id` to the causing `event_id`.
- The envelope is also mirrored into Kafka message headers (`trace_id`, `event_type`) for routing and observability. The JSON envelope remains the source of truth.
- Delivery is at-least-once. Every consumer dedupes on `envelope.event_id` and applies changes idempotently. Projections keep an entity version and ignore stale versions.

Envelope schema (the `envelope` object):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["event_id", "event_type", "event_version", "occurred_at", "producer", "trace_id", "correlation_id", "causation_id", "schema_ref"],
  "properties": {
    "event_id": {"type": "string", "format": "uuid"},
    "event_type": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
    "event_version": {"type": "integer", "minimum": 1},
    "occurred_at": {"type": "string", "format": "date-time"},
    "producer": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
    "trace_id": {"type": "string", "pattern": "^[0-9a-f]{32}$"},
    "correlation_id": {"type": "string", "format": "uuid"},
    "causation_id": {"type": ["string", "null"], "format": "uuid"},
    "schema_ref": {"type": "string", "pattern": "^EVENT_CONTRACTS\\.md#[a-z][a-z0-9_]*-v[0-9]+$"}
  }
}
```

### Consumed: EVT-001 — `user_account_changed` v1 (producer `identity_service`, topic `identity_events`, key `user_id`)

Emitted when an account's email is verified (first time it becomes reachable), its profile changes, its email changes, or it is deactivated. The payload is a full snapshot, so consumers upsert.

Delivery: at-least-once; consumers dedupe on `envelope.event_id`; apply only if `account_version` is newer.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["user_id", "change", "email", "username", "display_name", "email_verified", "status", "account_version"],
  "properties": {
    "user_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["email_verified", "profile_updated", "email_changed", "deactivated"]},
    "email": {"type": "string", "format": "email"},
    "username": {"type": "string", "minLength": 2, "maxLength": 32},
    "display_name": {"type": "string", "minLength": 1, "maxLength": 64},
    "email_verified": {"type": "boolean"},
    "status": {"type": "string", "enum": ["active", "deactivated"]},
    "account_version": {"type": "integer", "minimum": 1}
  }
}
```

### Consumed: EVT-004 — `member_sanction_changed` v1 (producer `moderation_service`, topic `moderation_events`, key `community_id`)

Emitted for every sanction issued or lifted. Sources: a moderator action, automatic expiry of a timed mute, or an appeal that was overturned. `community_service` applies it to member state. `notification_service` sends the moderation notice to the affected member.

Delivery: at-least-once; consumers dedupe on `envelope.event_id`.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["sanction_id", "community_id", "target_user_id", "actor_user_id", "action", "source", "reason", "expires_at", "appealable"],
  "properties": {
    "sanction_id": {"type": "string", "format": "uuid"},
    "community_id": {"type": "string", "format": "uuid"},
    "target_user_id": {"type": "string", "format": "uuid"},
    "actor_user_id": {"type": ["string", "null"], "format": "uuid", "description": "Null when source is expiry."},
    "action": {"type": "string", "enum": ["mute", "unmute", "kick", "ban", "unban"]},
    "source": {"type": "string", "enum": ["moderator", "expiry", "appeal"]},
    "reason": {"type": ["string", "null"], "maxLength": 1000},
    "expires_at": {"type": ["string", "null"], "format": "date-time", "description": "Mute end time; null for permanent bans, kicks, and lift actions."},
    "appealable": {"type": "boolean", "description": "True for active mute and ban sanctions the member can appeal."}
  }
}
```

### Consumed: EVT-006 — `appeal_status_changed` v1 (producer `moderation_service`, topic `moderation_events`, key `community_id`)

Emitted when an appeal is submitted and when moderators decide it. An `overturned` decision is always paired with an EVT-004 lift event (`source = appeal`).

Delivery: at-least-once; consumers dedupe on `envelope.event_id`.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["appeal_id", "sanction_id", "community_id", "appellant_user_id", "sanction_type", "status", "reviewer_user_id", "decision_note"],
  "properties": {
    "appeal_id": {"type": "string", "format": "uuid"},
    "sanction_id": {"type": "string", "format": "uuid"},
    "community_id": {"type": "string", "format": "uuid"},
    "appellant_user_id": {"type": "string", "format": "uuid"},
    "sanction_type": {"type": "string", "enum": ["mute", "ban"]},
    "status": {"type": "string", "enum": ["submitted", "upheld", "overturned"]},
    "reviewer_user_id": {"type": ["string", "null"], "format": "uuid"},
    "decision_note": {"type": ["string", "null"], "maxLength": 2000}
  }
}
```

### Consumed: EVT-007 — `friend_request_changed` v1 (producer `social_service`, topic `social_events`, key `recipient_user_id`)

Emitted when a friend request is sent, accepted, declined, or cancelled.

Delivery: at-least-once; consumers dedupe on `envelope.event_id`.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["request_id", "sender_user_id", "recipient_user_id", "status"],
  "properties": {
    "request_id": {"type": "string", "format": "uuid"},
    "sender_user_id": {"type": "string", "format": "uuid"},
    "recipient_user_id": {"type": "string", "format": "uuid"},
    "status": {"type": "string", "enum": ["pending", "accepted", "declined", "cancelled"]}
  }
}
```

### Consumed: EVT-009 — `message_changed` v1 (producer `messaging_service`, topic `message_events`, key `container_id`)

Emitted for every message creation, edit, deletion (by the author or by automod), and reaction change. The payload carries a message snapshot. On deletion, `content` is null and `attachments` is empty. Attachment download URLs are short-lived presigned URLs, and clients refresh them through message history.

Delivery: at-least-once; consumers dedupe on `envelope.event_id`; ordering guaranteed per container; apply only if `message_version` is newer.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["message_id", "change", "container_type", "container_id", "community_id", "author_user_id", "thread_root_id", "content", "attachments", "mentioned_user_ids", "reactions", "deletion_reason", "created_at", "edited_at", "message_version"],
  "properties": {
    "message_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["created", "edited", "deleted", "reactions_changed"]},
    "container_type": {"type": "string", "enum": ["channel", "conversation"]},
    "container_id": {"type": "string", "format": "uuid", "description": "channel_id or conversation_id."},
    "community_id": {"type": ["string", "null"], "format": "uuid", "description": "Set for channel messages; null for conversation messages."},
    "author_user_id": {"type": "string", "format": "uuid"},
    "thread_root_id": {"type": ["string", "null"], "format": "uuid"},
    "content": {"type": ["string", "null"], "maxLength": 4000},
    "attachments": {
      "type": "array",
      "maxItems": 10,
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["attachment_id", "content_type", "size_bytes", "download_url", "url_expires_at"],
        "properties": {
          "attachment_id": {"type": "string", "format": "uuid"},
          "content_type": {"type": "string", "enum": ["image/png", "image/jpeg", "image/gif", "image/webp"]},
          "size_bytes": {"type": "integer", "minimum": 1, "maximum": 10485760},
          "download_url": {"type": "string", "format": "uri"},
          "url_expires_at": {"type": "string", "format": "date-time"}
        }
      }
    },
    "mentioned_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "reactions": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["emoji", "count"],
        "properties": {
          "emoji": {"type": "string", "minLength": 1, "maxLength": 32},
          "count": {"type": "integer", "minimum": 1}
        }
      }
    },
    "deletion_reason": {"type": ["string", "null"], "enum": ["author", "automod", null]},
    "created_at": {"type": "string", "format": "date-time"},
    "edited_at": {"type": ["string", "null"], "format": "date-time"},
    "message_version": {"type": "integer", "minimum": 1}
  }
}
```

### Produced: EVT-011 — `notification_created` v1 (topic `notification_events`, key `recipient_user_id`, consumer `realtime_gateway`)

Emitted when an in-app notification is stored for a user, so open sessions can show it immediately.

Delivery: at-least-once; consumers dedupe on `envelope.event_id`.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["notification_id", "recipient_user_id", "kind", "title", "summary", "link", "unread_count"],
  "properties": {
    "notification_id": {"type": "string", "format": "uuid"},
    "recipient_user_id": {"type": "string", "format": "uuid"},
    "kind": {"type": "string", "enum": ["mention", "friend_request_received", "friend_request_accepted", "moderation_notice", "appeal_decided"]},
    "title": {"type": "string", "maxLength": 200},
    "summary": {"type": "string", "maxLength": 500},
    "link": {"type": "string", "description": "In-app deep link path, e.g. /channels/{channel_id}/messages/{message_id}."},
    "unread_count": {"type": "integer", "minimum": 0}
  }
}
```

Synchronous internal contracts: none — this service has no synchronous dependencies and provides no internal REST endpoints to peers.

## PostgreSQL database schema

All tables are owned and defined by this service. UUID primary keys; timestamps are `timestamptz` (UTC).

### `notifications`

| Column | Type | Constraints |
|---|---|---|
| `id` | `uuid` | PK, default `gen_random_uuid()` |
| `recipient_user_id` | `uuid` | NOT NULL |
| `kind` | `text` | NOT NULL, CHECK in (`mention`, `friend_request_received`, `friend_request_accepted`, `moderation_notice`, `appeal_decided`) |
| `title` | `varchar(200)` | NOT NULL |
| `summary` | `varchar(500)` | NOT NULL |
| `link` | `text` | NOT NULL |
| `created_at` | `timestamptz` | NOT NULL, default `now()` |
| `read_at` | `timestamptz` | NULL (unread while NULL) |

Indexes:
- `idx_notifications_recipient_created` ON (`recipient_user_id`, `created_at` DESC) — inbox listing.
- `idx_notifications_recipient_unread` partial ON (`recipient_user_id`) WHERE `read_at` IS NULL — unread count and `unread_only` filter.

### `notification_preferences`

| Column | Type | Constraints |
|---|---|---|
| `user_id` | `uuid` | PK |
| `mention_in_app` | `boolean` | NOT NULL, default `true` |
| `mention_email` | `boolean` | NOT NULL, default `true` |
| `friend_request_received_in_app` | `boolean` | NOT NULL, default `true` |
| `friend_request_received_email` | `boolean` | NOT NULL, default `true` |
| `friend_request_accepted_in_app` | `boolean` | NOT NULL, default `true` |
| `friend_request_accepted_email` | `boolean` | NOT NULL, default `true` |
| `moderation_notice_in_app` | `boolean` | NOT NULL, default `true` |
| `moderation_notice_email` | `boolean` | NOT NULL, default `true` |
| `appeal_decided_in_app` | `boolean` | NOT NULL, default `true` |
| `appeal_decided_email` | `boolean` | NOT NULL, default `true` |
| `updated_at` | `timestamptz` | NOT NULL, default `now()` |

Rows are created lazily on first access (defaults above); the domain layer treats a missing row as the defaults.

### `email_deliveries`

| Column | Type | Constraints |
|---|---|---|
| `id` | `uuid` | PK, default `gen_random_uuid()` |
| `notification_id` | `uuid` | NOT NULL, UNIQUE, FK → `notifications(id)` |
| `kind` | `text` | NOT NULL (same CHECK domain as `notifications.kind`) |
| `recipient_user_id` | `uuid` | NOT NULL |
| `status` | `text` | NOT NULL, CHECK in (`pending`, `sent`, `throttled`, `skipped_preference`, `skipped_unverified`, `skipped_deactivated`, `failed`) |
| `provider_message_id` | `text` | NULL |
| `attempts` | `integer` | NOT NULL, default 0 |
| `last_error` | `text` | NULL |
| `next_attempt_at` | `timestamptz` | NULL (back-off schedule for retryable failures) |
| `created_at` | `timestamptz` | NOT NULL, default `now()` |
| `updated_at` | `timestamptz` | NOT NULL, default `now()` |

Indexes: `idx_email_deliveries_retry` partial ON (`next_attempt_at`) WHERE `status = 'pending'` AND `next_attempt_at IS NOT NULL` — retry sweep.

### `recipient_projections`

| Column | Type | Constraints |
|---|---|---|
| `user_id` | `uuid` | PK |
| `email` | `text` | NOT NULL |
| `username` | `text` | NOT NULL |
| `display_name` | `text` | NOT NULL |
| `email_verified` | `boolean` | NOT NULL |
| `status` | `text` | NOT NULL, CHECK in (`active`, `deactivated`) |
| `account_version` | `integer` | NOT NULL, minimum 1 |
| `updated_at` | `timestamptz` | NOT NULL, default `now()` |

Upsert rule: apply the EVT-001 snapshot only when the incoming `account_version` is greater than the stored one.

### `mention_email_windows`

Persisted throttle state (survives restarts).

| Column | Type | Constraints |
|---|---|---|
| `recipient_user_id` | `uuid` | PK |
| `last_email_sent_at` | `timestamptz` | NOT NULL |

### `processed_events`

Consumer deduplication (one shared row space per consumer group).

| Column | Type | Constraints |
|---|---|---|
| `event_id` | `uuid` | PK |
| `topic` | `text` | NOT NULL |
| `processed_at` | `timestamptz` | NOT NULL, default `now()` |

A scheduled job prunes rows older than 7 days (dedupe only needs to cover realistic redelivery windows).

### `notification_outbox`

Transactional outbox for EVT-011.

| Column | Type | Constraints |
|---|---|---|
| `id` | `uuid` | PK, default `gen_random_uuid()` |
| `notification_id` | `uuid` | NOT NULL, FK → `notifications(id)` |
| `published_at` | `timestamptz` | NULL (NULL = pending publish) |
| `created_at` | `timestamptz` | NOT NULL, default `now()` |

Indexes: `idx_notification_outbox_pending` partial ON (`created_at`) WHERE `published_at` IS NULL.

### Relationships

- `notifications` 1—1 `email_deliveries` (one delivery decision per notification).
- `notifications` 1—0..1 `notification_outbox` (one pending/finished publish per notification).
- `notification_preferences`, `recipient_projections`, `mention_email_windows` are keyed by user and reference users only logically (user identity is owned by `identity_service`).

## Stateless container operational model

- Strictly stateless: all state lives in PostgreSQL and Kafka; replicas > 1 are safe. Multiple consumer replicas share one consumer group (partitions distribute events; per-container ordering is per partition).
- **Liveness** `GET /healthz`: process-responsive check only; never fails on PostgreSQL/Kafka/Resend hiccups.
- **Readiness** `GET /readyz`: verifies PostgreSQL and Kafka connectivity; returns 503 once shutdown begins.
- **Graceful shutdown:** on `SIGTERM`/`SIGINT` the service marks itself not-ready, stops HTTP intake and consumer fetch loops (committing offsets for completed messages), drains in-flight HTTP requests and email sends within a bounded timeout, then closes the PostgreSQL pool and Kafka producer/client and exits 0.
- **Failure modes & resilience:**
  - At-least-once Kafka delivery → dedupe on `event_id` + idempotent state changes.
  - Malformed/unroutable events → logged once with raw `event_id`, error metric incremented, not reprocessed endlessly.
  - Resend 429/5xx → bounded exponential back-off retries recorded on `email_deliveries.next_attempt_at`; permanent 4xx recorded immediately; in-app notification unaffected either way.
  - Broker publish failure → outbox rows remain pending and are retried; unread counts are recomputed from the committed notification state on every publish, so a retried publish never emits a stale count.

## Observability

Business metrics (low-cardinality labels only):
- `notification_created_total{kind}` — notifications persisted.
- `notification_emails_sent_total{kind,result}` — `result ∈ sent|throttled|skipped_preference|skipped_unverified|skipped_deactivated|failed`.
- `notification_emails_throttled_total{kind}` — mention throttling events.
- `notification_read_total{kind}` — read-state transitions.

Technical metrics (platform-wide conventions):
- `http_requests_total{route,method,status_class}`, `http_request_duration_seconds{route,method}`.
- `kafka_consumer_lag{topic,consumer_group}` for all four consumed topics.
- `external_call_duration_seconds{provider="resend",operation="send_email"}` and `external_call_errors_total{provider="resend",operation="send_email",error_class}`.

SLOs:
- Event → in-app notification p95 < 2 s.
- Event → email accepted by Resend p95 < 60 s.
- Consumer lag < 5 s p95.
- Resend error rate < 0.5 %.

Tracing: `traceparent`/`X-Trace-Id` propagated on HTTP; consumed event envelopes carry `trace_id` into every log line, and produced EVT-011 envelopes copy it from the causing event.

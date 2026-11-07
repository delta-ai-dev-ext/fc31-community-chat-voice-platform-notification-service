# notification_service

In-app notifications and transactional notification emails of the community
platform. The service consumes the four domain event topics of the platform
(`identity_events`, `moderation_events`, `social_events`, `message_events`),
persists notifications per recipient, decides which emails may be sent and
publishes `notification_created` (EVT-011) through a transactional outbox.

Specification: [`docs/user_stories.md`](docs/user_stories.md),
[`docs/openapi_spec.yaml`](docs/openapi_spec.yaml),
[`docs/architecture.md`](docs/architecture.md),
[`docs/decisions/ADR-001-transactional-outbox-for-notification-events.md`](docs/decisions/ADR-001-transactional-outbox-for-notification-events.md).

## REST API

All `/api/v1` endpoints require `Authorization: Bearer <identity access token>`
and derive the caller's `user_id` from the token `sub` claim.

| Method | Path | Success | Notes |
| --- | --- | --- | --- |
| GET | `/api/v1/notifications` | 200 | `unread_only`, `limit` (1–100, default 50), `cursor`; returns `items`, `next_cursor`, `unread_count` |
| POST | `/api/v1/notifications/{notification_id}/read` | 204 | idempotent; 404 `notification_not_found` for foreign/unknown ids |
| POST | `/api/v1/notifications/read-all` | 204 | marks every unread notification of the caller |
| GET | `/api/v1/notification-preferences` | 200 | all five kinds; unset kinds default to in-app + email enabled |
| PUT | `/api/v1/notification-preferences` | 200 | full replacement; 422 `invalid_preferences` when kinds/toggles are missing, extra or mistyped |
| GET | `/healthz`, `/readyz`, `/metrics` | 200 | liveness, dependency readiness (503 `not_ready`), Prometheus metrics |

Errors use the envelope `{"error": "<stable_code>", "detail": "<message>"}` with
`invalid_request`, `invalid_limit`, `invalid_cursor`, `invalid_preferences`,
`unauthorized`, `notification_not_found`, `not_found`, `not_ready`,
`internal_error`. Every response carries `X-Trace-Id` and `X-Request-Id`;
incoming `traceparent` / `X-Trace-Id` headers are propagated.

## Configuration

Environment variables are validated at startup (`internal/config`); see
[`.env.example`](.env.example) for a local development file. A local `.env` is
only loaded when `APP_ENV` is not `production`.

## Delivery pipeline

* Consumed topics are declared in `internal/models` (`ConsumedTopics`) and read
  by a single consumer group; offsets are committed only after the local
  transaction committed (at-least-once).
* Duplicate deliveries are dropped through `processed_events(envelope.event_id)`
  inside the same transaction that writes the notification.
* Each created notification is committed together with its
  `email_deliveries` decision and a `notification_outbox` row; EVT-011 is
  published after commit and re-published from the outbox after a crash.
* Mention emails are throttled per recipient through
  `mention_email_windows` (15 min default); automated mute expiry notices always
  bypass preferences and throttling.
* Resend failures are classified as retryable (429, 5xx, transport) or permanent
  (other 4xx); retryable failures are retried from `email_deliveries` with a
  bounded exponential back-off and are finally recorded as `failed`.

## Development

```bash
make install   # go mod download
make test      # go test ./... -race -count=1 -parallel 4
make cover     # coverage over ./internal/... (>= 80%)
make vet       # go vet ./...
make run       # go run ./cmd/server
make build     # docker build
```

Migrations live in `migrations/` and are embedded (`internal/repository`); they
run at startup unless `RUN_MIGRATIONS=false`.

# Engineering Guide — notification_service

Persistent technical memory for implementing and evolving this service. Architecture-level behavior lives in `docs/architecture.md`; this document governs tooling, libraries, conventions, and operations.

## 1. Runtime & Toolchain

| Aspect | Value |
|---|---|
| Language / runtime | Go (module toolchain `go 1.26`, matching the platform Docker build `golang:1.26-alpine`) |
| Module path | `notification_service` (module name = service name; no VCS host prefix) |
| Package manager | Go modules (`go.mod` / `go.sum`) |
| Build entrypoint | `make build` (Docker image), `go build ./cmd/server` locally |
| Test entrypoint | `make test` → `go test ./... -race -count=1 -parallel 4` |
| Lint entrypoint | `make lint` → `golangci-lint run ./...` |
| Local run | `make run` → `go run ./cmd/server` |

`make install` (`go mod download`) must be run before the first build in a cold workspace.

## 2. Libraries & External Provider SDKs

| Concern | Package / Library | Source | Usage Notes |
|---|---|---|---|
| REST API framework | `github.com/gin-gonic/gin` | language-skill | Handlers stay thin; bind/validate request structs, delegate to services. |
| PostgreSQL driver | `github.com/jackc/pgx/v5` | language-skill | Parameterized queries only; use `pgxpool` for the connection pool. |
| Migrations | `github.com/golang-migrate/migrate/v4` | language-skill | Numbered SQL migration files under the repository migrations directory. |
| Kafka client | `github.com/IBM/sarama` | language-skill | Consumer-group mode for the four consumed topics; producer for `notification_events`. Configure explicit offsets commit after processing (at-least-once semantics). |
| Logging | `go.uber.org/zap` | language-skill | Structured JSON production logger; inject via constructor, not globals. |
| Metrics | `github.com/prometheus/client_golang/prometheus` | language-skill | Register business + RED metrics at startup; low-cardinality labels only. |
| External provider — Resend (transactional email) | `github.com/resend/resend-go` (official SDK; resolve latest major, e.g. v4, via `go get github.com/resend/resend-go@latest` and use the matching versioned import path) | tech-lead-research | Use `NewClient` + `Emails.Send` for single notification emails; pass an idempotency key derived from the notification id; map 429/5xx to retryable, other 4xx to permanent failures. Never reimplement the Resend HTTP protocol with a custom client. |
| Environment loading | `github.com/joho/godotenv` | language-skill | Local development only; never in production. |
| Testing | standard `testing` + `github.com/stretchr/testify` | language-skill | `assert`/`require`; table-driven tests. |
| Mocking | `github.com/stretchr/testify/mock` | language-skill | Mock repositories, Kafka producer/consumer, and the Resend client at interface boundaries. |
| HTTP mocking | `github.com/jarcoal/httpmock` | language-skill | For any direct HTTP call that escapes SDK seams. |

Resolve exact versions with `go get <module>@latest` + `go mod tidy`; never hardcode versions here unless listed in `Pinned Decisions`.

## 3. Container Lifecycle & Probes

- **Statelessness:** no local disk state, no sticky in-memory session state; every replica is interchangeable. All mutable state lives in PostgreSQL; all cross-service signals flow through Kafka.
- **Startup:** load and validate configuration (fail fast with a structured error log), run migrations check, open the `pgxpool`, connect the Kafka consumer group and producer, then register metrics and start serving.
- **`GET /healthz` (liveness):** process responsiveness only. Never fails because PostgreSQL/Kafka/Resend is degraded.
- **`GET /readyz` (readiness):** checks PostgreSQL connectivity (pool ping) and Kafka broker/consumer-group readiness; returns 503 once shutdown starts.
- **`SIGTERM`/`SIGINT` graceful shutdown:**
  1. Mark readiness failing so the load balancer stops routing.
  2. Stop accepting new HTTP requests; stop consumer fetch loops (commit offsets for completed messages only).
  3. Drain in-flight HTTP requests and email sends within a bounded timeout (15–30 s).
  4. Flush/close the Kafka producer, close the consumer group, close the `pgxpool`; exit code 0.
- Trap signals with `signal.NotifyContext`; shutdown via `http.Server.Shutdown(ctx)`.

## 4. Testing Strategy

- **Structure:** unit tests beside the code they cover plus `tests/unit` and `tests/integration` for cross-layer behavior; mocks/fixtures under `tests/mocks`.
- **Unit tests:** all business-logic branches (event → notification mapping per kind and status, preference rules, moderation-notice override, mention throttle window boundaries, email eligibility, retry classification retryable vs permanent). All external dependencies (repository, Kafka, Resend client) are interface mocks via `testify/mock`; no real network calls.
- **Integration tests:** exercise the full consumer pipeline and REST endpoints against mocked PostgreSQL (in-memory implementations or pgx mocks), broker mocks, and a mocked Resend client. No real infrastructure, no Docker.
- **Determinism & parallelism:** `t.Parallel()` in all test functions; no shared mutable state; time-dependent logic (throttle window) uses an injectable clock.
- **Coverage:** ≥ 80 %, focused on the consumer mapping, email decision logic, and REST handlers.
- Run everything through `make test`.

## 5. Coding Conventions & Project Structure

```
cmd/server/main.go          # entrypoint: config, DI, router, consumer loops, signals
internal/config/config.go   # typed Config struct from env, validated at startup
internal/handler/           # HTTP handlers (thin)
internal/service/           # domain logic: notification mapping, preferences, email decisions
internal/repository/        # pgx data access
internal/event/             # consumer wiring, envelope validation, dedupe, outbox publisher
internal/provider/          # Resend client adapter (SDK seam for mocking)
internal/models/            # domain types, request/response structs
tests/unit, tests/integration, tests/mocks
```

- Layering: handler → service → repository; events flow event consumer → service → repository/outbox/publisher.
- All exported types and functions have Go doc comments; return errors, never panic outside init.
- Parameterized SQL only (`pgx/v5`); no `fmt.Sprintf` in SQL.
- Table-driven tests preferred.
- The event envelope is modeled as a single reusable Go struct with all nine fields required — never assembled as ad-hoc maps at publish sites.
- Errors map to stable snake_case codes (`invalid_cursor`, `notification_not_found`, `invalid_preferences`); response bodies are `{"error": "<code>", "detail": "<human text>"}`.

## 6. Configuration & Secrets Management

12-factor env vars, validated at startup (missing required var → fatal structured log):

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string (portable form). |
| `KAFKA_BROKERS` | Comma-separated Kafka bootstrap brokers. |
| `KAFKA_CONSUMER_GROUP` | Consumer group id (one per service). |
| `NOTIFICATION_EVENTS_TOPIC` | Produced topic (`notification_events`). |
| `RESEND_API_KEY` | Resend API secret (secret; never logged). |
| `MAIL_FROM_ADDRESS` | Verified sender identity for notification emails. |
| `MENTION_EMAIL_THROTTLE_WINDOW` | Mention throttle window (default `15m`). |
| `HTTP_PORT` | Listen port for the REST API and probes. |
| `SHUTDOWN_TIMEOUT` | Graceful drain budget (default `20s`). |

No `.env` in production (local development only, gitignored). Secrets come from the platform environment, never baked into images.

## 7. Observability & Logging Conventions

- **Structured JSON logging** (zap): every line carries `timestamp`, `severity`, `service_name` (`notification_service`), `trace_id`, `correlation_id`, `event_id` (consumers), `request_id` (HTTP), `operation`, `status`. Levels: DEBUG dev, INFO normal, WARN degraded, ERROR failures. External calls (Resend) log operation, response status class, and latency; error logs include stack traces and context.
- **Trace propagation:** HTTP requests propagate `traceparent`/`X-Trace-Id`; consumed event envelopes carry `trace_id` into every log line of that processing attempt.
- **Mandatory event envelope (produce and consume):** every Kafka message is `{"envelope": {...}, "payload": {...}}`. The envelope carries `event_id`, `event_type`, `event_version`, `occurred_at`, `producer`, `trace_id`, `correlation_id`, `causation_id`, `schema_ref` — all nine required, none empty. `trace_id` is **never empty**: propagated from the originating request, or minted at the edge of the flow when there is no originating request. When publishing because of a consumed event (always the case for EVT-011), copy `trace_id` and `correlation_id` from the causing event and set `causation_id` to the causing `event_id`. A payload MUST NOT carry an envelope field at its top level. The envelope is mirrored into Kafka message headers (`trace_id`, `event_type`).
- **Metrics registration:** business metrics from `docs/architecture.md` (`notification_created_total{kind}`, `notification_emails_sent_total{kind,result}`, `notification_emails_throttled_total{kind}`, `notification_read_total{kind}`) plus RED metrics, `kafka_consumer_lag{topic,consumer_group}`, and Resend call metrics, all registered at startup with low-cardinality labels only.

## 8. Pinned Decisions

| Dependency / Area | Pin / Constraint | Rationale | Added By | Date |
|---|---|---|---|---|
| — | — | No pinned decisions yet. | — | — |

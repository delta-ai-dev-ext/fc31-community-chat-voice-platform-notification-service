# ADR-001 — Transactional outbox for `notification_created` publication

- **Status:** Accepted
- **Date:** 2026-02-19
- **Deciders:** Tech Lead (notification_service)

## Context

Every persisted notification must be pushed to open sessions via EVT-011 (`notification_created` on `notification_events`). Kafka delivery to us is at-least-once and our publication is also at-least-once, so two failure windows must be handled without either losing a notification or double-pushing it:

1. Crash after committing the notification but before publishing EVT-011 — the incoming event would be deduplicated on redelivery, so the notification would exist but never be pushed.
2. Publish failure (broker hiccup) after the notification is committed.

## Decision

Use a transactional outbox. The notification insert and an `notification_outbox` row (pending) commit in the same PostgreSQL transaction; a publisher drains pending outbox rows after commit and retries until the broker accepts, marking the row published. `unread_count` in the EVT-011 payload is recomputed from committed state at publish time, so a retried publish never emits a stale count.

## Consequences

- No notification is ever created without a guaranteed publish attempt; no double push occurs for redelivered causes (dedupe prevents the second notification, hence the second outbox row).
- Cost: one extra table, one partial index, and a publisher loop; eventual (sub-second) push latency is within the SLO (event → in-app notification p95 < 2 s).
- Alternative rejected: committing the dedupe record only after a successful publish — a crash in between would lose the notification entirely.

# User Stories — notification_service

Derived from the platform system design (notification_service slice: consumers of EVT-001, EVT-004, EVT-006, EVT-007, EVT-009; producer of EVT-011; Resend email integration). Each story is independently testable. The five notification kinds are: `mention`, `friend_request_received`, `friend_request_accepted`, `moderation_notice`, `appeal_decided`.

## Actor glossary
- **Member**: an authenticated user with an access token issued by identity_service.
- **Producer services**: identity_service, moderation_service, social_service, messaging_service (Kafka event sources).

---

### US-001 — View the notification inbox
**As a** member,
**I want** to list my notifications newest-first with my unread count,
**so that** I can catch up on mentions, friend activity, and moderation updates.

**Acceptance criteria**
- `GET /notifications` returns the caller's notifications (id, kind, title, summary, deep link, created_at, read_at), newest first, cursor-paginated (`cursor`, `limit`, default limit 50, max 100), plus the total unread count.
- `unread_only=true` returns only notifications with no `read_at`.
- The unread count reflects all unread notifications, independent of the returned page.
- No notifications yet → empty `items`, `next_cursor: null`, `unread_count: 0`.
- Invalid/stale cursor → `400 invalid_cursor`; missing/expired token → `401`.

### US-002 — Mark one notification as read
**As a** member,
**I want** to mark a single notification as read,
**so that** my unread badge reflects only what I still need to look at.

**Acceptance criteria**
- `POST /notifications/{notification_id}/read` sets `read_at` on the caller's unread notification and returns `204`.
- Repeating the call on an already-read notification returns `204` and does not change `read_at` (idempotent).
- A notification belonging to another user, or nonexistent, returns `404 notification_not_found` — never leaks information.
- The unread count decreases by at most one and is reflected in subsequent `notification_created` events for the recipient.

### US-003 — Mark all notifications as read
**As a** member,
**I want** to mark every unread notification as read at once,
**so that** I can clear my inbox in one action.

**Acceptance criteria**
- `POST /notifications/read-all` sets `read_at` on all of the caller's unread notifications and returns `204`.
- With no unread notifications it returns `204` and changes nothing (idempotent).
- Notifications of other users are never affected.

### US-004 — View notification preferences
**As a** member,
**I want** to see my per-kind in-app and email toggles,
**so that** I know which channels each notification kind reaches.

**Acceptance criteria**
- `GET /notification-preferences` returns all five kinds, each with `in_app` and `email` booleans.
- A user without stored preferences gets the defaults (all kinds `in_app: true`, `email: true`) and the row is created on first update.
- Unauthenticated → `401`.

### US-005 — Update notification preferences
**As a** member,
**I want** to turn in-app and email delivery per kind on or off,
**so that** I control the notification noise I receive.

**Acceptance criteria**
- `PUT /notification-preferences` accepts exactly the five kinds with both toggles each (full replacement) and returns the saved preferences.
- A body omitting a kind or carrying unknown fields → `422 invalid_preferences`.
- Stored preference changes apply from the next notification onward; already-created notifications are not retroactively removed.
- The `moderation_notice` kind's stored toggles are honored in the API response, but moderation notices are always delivered in-app and by email (safety notices).

### US-006 — Be notified when mentioned
**As a** member,
**I want** a notification when someone @mentions me in a channel or conversation,
**so that** I notice messages directed at me.

**Acceptance criteria**
- An EVT-009 `message_changed` with `change: created` and non-empty `mentioned_user_ids` creates one `mention` notification per mentioned user except the author, with a title/summary identifying the author and message context and a deep link to the message.
- `edited`, `deleted`, `reactions_changed`, or `created` without mentions create no notification.
- A mention notification is created even when the recipient has no stored contact projection (in-app delivery does not depend on it).
- Duplicate delivery of the same event (at-least-once) creates no second notification.

### US-007 — Be notified about friend requests
**As a** member,
**I want** a notification when I receive a friend request and when one of my sent requests is accepted,
**so that** I can respond promptly and know my invitations landed.

**Acceptance criteria**
- EVT-007 `status: pending` → `friend_request_received` for the recipient.
- EVT-007 `status: accepted` → `friend_request_accepted` for the sender.
- `declined` / `cancelled` create no notification.
- Deep links point to the friend-requests view.

### US-008 — Receive moderation notices
**As a** sanctioned member,
**I want** to be notified when I am muted, kicked, banned, unmuted, or unbanned, with reason, expiry and appeal instructions,
**so that** I understand the action taken and how to respond.

**Acceptance criteria**
- Every EVT-004 action creates a `moderation_notice` for the target user.
- The notice carries the sanction reason and `expires_at` when present; when `appealable` is true the notice points to the appeal flow.
- A mute expiry (EVT-004 with `source: expiry`, `actor_user_id: null`) still produces a notice whose title reflects the automatic unmute.
- Moderation notices ignore the recipient's preferences: they are always created in-app and always emailed (subject to email eligibility below).

### US-009 — Be notified about appeal decisions
**As a** member who appealed a sanction,
**I want** a notification when my appeal is decided,
**so that** I learn the outcome without polling.

**Acceptance criteria**
- EVT-006 with `status: upheld` or `overturned` creates an `appeal_decided` notification for the appellant, including the `decision_note` in the summary when present.
- EVT-006 with `status: submitted` creates no notification.

### US-010 — Receive notification emails only when eligible
**As a** member,
**I want** notification emails only for my verified, active account and only for kinds I enabled,
**so that** my inbox is not used before my email is confirmed and I keep control of email noise.

**Acceptance criteria**
- An email is attempted only when the recipient projection has `email_verified: true` and `status: active`; otherwise the email is recorded as `skipped_unverified` / `skipped_deactivated` while the in-app notification is still created.
- An email for a kind whose email toggle is off is recorded as `skipped_preference` (moderation notices bypass this).
- If the recipient projection is unknown, the email is retried within a bounded budget and finally recorded as `failed` without blocking the in-app notification.
- Emails are sent via Resend with an idempotency key derived from the notification id; Resend 429/5xx are retried with bounded exponential back-off, permanent 4xx recorded immediately; the send rate stays under the shared account limit.

### US-011 — Mention email batching
**As a** member who gets mentioned often,
**I want** at most one mention email per 15 minutes,
**so that** I am not flooded when a conversation mentions me repeatedly.

**Acceptance criteria**
- At most one `mention` email per recipient per 15-minute window; later mention emails in the window are recorded as `throttled`.
- The throttle window is persisted, so a service restart does not reset it.
- Moderation-notice emails are never throttled.

### US-012 — Live push of new notifications
**As a** member with an open session,
**I want** new notifications pushed to my client immediately,
**so that** badges and lists update without polling.

**Acceptance criteria**
- Every persisted notification publishes an EVT-011 `notification_created` on `notification_events` (keyed by recipient_user_id) carrying the notification snapshot and the recipient's current unread count.
- If the publish fails, the notification is still persisted and the publish is retried from the persisted state within the same processing attempt (at-least-once, deduplicated downstream).
- Envelope rules hold: `trace_id` and `correlation_id` copied from the causing event, `causation_id` = causing event id, `schema_ref` = `EVENT_CONTRACTS.md#notification_created-v1`.

### US-013 — Duplicate and outage resilience
**As a** platform operator,
**I want** duplicate deliveries and provider outages to degrade notifications gracefully,
**so that** users see no duplicates and no lost in-app state.

**Acceptance criteria**
- Redelivery of any consumed event creates no duplicate notification (dedupe on `envelope.event_id`).
- An event whose recipient projection is missing still creates the in-app notification (email retries separately).
- A Resend outage delays or fails only emails; in-app notifications and live pushes are unaffected.
- Malformed events are logged once and counted in error metrics, not reprocessed endlessly.

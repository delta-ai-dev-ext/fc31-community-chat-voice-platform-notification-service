-- notification_service initial schema (docs/architecture.md "PostgreSQL database schema").

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS notifications (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient_user_id uuid NOT NULL,
    kind text NOT NULL CHECK (kind IN (
        'mention',
        'friend_request_received',
        'friend_request_accepted',
        'moderation_notice',
        'appeal_decided'
    )),
    title varchar(200) NOT NULL,
    summary varchar(500) NOT NULL,
    link text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    read_at timestamptz NULL
);

CREATE INDEX IF NOT EXISTS idx_notifications_recipient_created
    ON notifications (recipient_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_notifications_recipient_unread
    ON notifications (recipient_user_id)
    WHERE read_at IS NULL;

CREATE TABLE IF NOT EXISTS notification_preferences (
    user_id uuid PRIMARY KEY,
    mention_in_app boolean NOT NULL DEFAULT true,
    mention_email boolean NOT NULL DEFAULT true,
    friend_request_received_in_app boolean NOT NULL DEFAULT true,
    friend_request_received_email boolean NOT NULL DEFAULT true,
    friend_request_accepted_in_app boolean NOT NULL DEFAULT true,
    friend_request_accepted_email boolean NOT NULL DEFAULT true,
    moderation_notice_in_app boolean NOT NULL DEFAULT true,
    moderation_notice_email boolean NOT NULL DEFAULT true,
    appeal_decided_in_app boolean NOT NULL DEFAULT true,
    appeal_decided_email boolean NOT NULL DEFAULT true,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS email_deliveries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id uuid NOT NULL UNIQUE REFERENCES notifications (id),
    kind text NOT NULL CHECK (kind IN (
        'mention',
        'friend_request_received',
        'friend_request_accepted',
        'moderation_notice',
        'appeal_decided'
    )),
    recipient_user_id uuid NOT NULL,
    status text NOT NULL CHECK (status IN (
        'pending',
        'sent',
        'throttled',
        'skipped_preference',
        'skipped_unverified',
        'skipped_deactivated',
        'failed'
    )),
    provider_message_id text NULL,
    attempts integer NOT NULL DEFAULT 0,
    last_error text NULL,
    next_attempt_at timestamptz NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_email_deliveries_retry
    ON email_deliveries (next_attempt_at)
    WHERE status = 'pending' AND next_attempt_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS recipient_projections (
    user_id uuid PRIMARY KEY,
    email text NOT NULL,
    username text NOT NULL,
    display_name text NOT NULL,
    email_verified boolean NOT NULL,
    status text NOT NULL CHECK (status IN ('active', 'deactivated')),
    account_version integer NOT NULL CHECK (account_version >= 1),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mention_email_windows (
    recipient_user_id uuid PRIMARY KEY,
    last_email_sent_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id uuid PRIMARY KEY,
    topic text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS notification_outbox (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id uuid NOT NULL REFERENCES notifications (id),
    published_at timestamptz NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_outbox_pending
    ON notification_outbox (created_at)
    WHERE published_at IS NULL;

-- Reverse of 000001_init.

DROP INDEX IF EXISTS idx_notification_outbox_pending;
DROP TABLE IF EXISTS notification_outbox;

DROP TABLE IF EXISTS processed_events;

DROP TABLE IF EXISTS mention_email_windows;

DROP TABLE IF EXISTS recipient_projections;

DROP INDEX IF EXISTS idx_email_deliveries_retry;
DROP TABLE IF EXISTS email_deliveries;

DROP TABLE IF EXISTS notification_preferences;

DROP INDEX IF EXISTS idx_notifications_recipient_unread;
DROP INDEX IF EXISTS idx_notifications_recipient_created;
DROP TABLE IF EXISTS notifications;

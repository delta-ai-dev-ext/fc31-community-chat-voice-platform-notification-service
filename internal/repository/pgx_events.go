package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"notification_service/internal/models"
)

// ProcessedEventStore is the pgx implementation of ProcessedEventRepository.
type ProcessedEventStore struct {
	db Querier
}

// NewProcessedEventStore builds a ProcessedEventRepository over db.
func NewProcessedEventStore(db Querier) *ProcessedEventStore { return &ProcessedEventStore{db: db} }

const recordProcessedEventSQL = `INSERT INTO processed_events (event_id, topic, processed_at)
VALUES ($1, $2, $3)
ON CONFLICT (event_id) DO NOTHING`

// Record implements ProcessedEventRepository.
func (s *ProcessedEventStore) Record(ctx context.Context, eventID, topic string, now time.Time) (bool, error) {
	tag, err := querier(ctx, s.db).Exec(ctx, recordProcessedEventSQL, eventID, topic, now)
	if err != nil {
		return false, fmt.Errorf("repository: record processed event: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

const pruneProcessedEventsSQL = `DELETE FROM processed_events WHERE processed_at < $1`

// Prune implements ProcessedEventRepository.
func (s *ProcessedEventStore) Prune(ctx context.Context, before time.Time) (int64, error) {
	tag, err := querier(ctx, s.db).Exec(ctx, pruneProcessedEventsSQL, before)
	if err != nil {
		return 0, fmt.Errorf("repository: prune processed events: %w", err)
	}
	return tag.RowsAffected(), nil
}

// MentionWindowStore is the pgx implementation of MentionWindowRepository.
type MentionWindowStore struct {
	db Querier
}

// NewMentionWindowStore builds a MentionWindowRepository over db.
func NewMentionWindowStore(db Querier) *MentionWindowStore { return &MentionWindowStore{db: db} }

const claimMentionWindowSQL = `INSERT INTO mention_email_windows (recipient_user_id, last_email_sent_at)
VALUES ($1, $2)
ON CONFLICT (recipient_user_id) DO UPDATE SET last_email_sent_at = EXCLUDED.last_email_sent_at
WHERE mention_email_windows.last_email_sent_at <= $3`

// TryClaim implements MentionWindowRepository. The window is claimed only when
// the last recorded mention email is older than window; the comparison happens
// inside the statement so concurrent consumers cannot both win.
func (s *MentionWindowStore) TryClaim(ctx context.Context, recipientUserID string, now time.Time, window time.Duration) (bool, error) {
	tag, err := querier(ctx, s.db).Exec(ctx, claimMentionWindowSQL, recipientUserID, now, now.Add(-window))
	if err != nil {
		return false, fmt.Errorf("repository: claim mention email window: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// OutboxStore is the pgx implementation of OutboxRepository.
type OutboxStore struct {
	db Querier
}

// NewOutboxStore builds an OutboxRepository over db.
func NewOutboxStore(db Querier) *OutboxStore { return &OutboxStore{db: db} }

const enqueueOutboxSQL = `INSERT INTO notification_outbox (notification_id, created_at)
VALUES ($1, $2)
RETURNING id`

// Enqueue implements OutboxRepository.
func (s *OutboxStore) Enqueue(ctx context.Context, notificationID string, now time.Time) (string, error) {
	var outboxID string
	if err := querier(ctx, s.db).QueryRow(ctx, enqueueOutboxSQL, notificationID, now).Scan(&outboxID); err != nil {
		return "", fmt.Errorf("repository: enqueue notification outbox: %w", err)
	}
	return outboxID, nil
}

const listPendingOutboxSQL = `SELECT id, notification_id, published_at, created_at
FROM notification_outbox
WHERE published_at IS NULL
ORDER BY created_at
LIMIT $1`

// ListPending implements OutboxRepository.
func (s *OutboxStore) ListPending(ctx context.Context, limit int) ([]models.OutboxEntry, error) {
	rows, err := querier(ctx, s.db).Query(ctx, listPendingOutboxSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("repository: list pending outbox: %w", err)
	}
	defer rows.Close()

	entries := make([]models.OutboxEntry, 0, limit)
	for rows.Next() {
		var entry models.OutboxEntry
		if err := rows.Scan(&entry.ID, &entry.NotificationID, &entry.PublishedAt, &entry.CreatedAt); err != nil {
			return nil, fmt.Errorf("repository: scan outbox entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: iterate outbox entries: %w", err)
	}
	return entries, nil
}

const markOutboxPublishedSQL = `UPDATE notification_outbox SET published_at = $2 WHERE id = $1`

// MarkPublished implements OutboxRepository.
func (s *OutboxStore) MarkPublished(ctx context.Context, outboxID string, now time.Time) error {
	if _, err := querier(ctx, s.db).Exec(ctx, markOutboxPublishedSQL, outboxID, now); err != nil {
		return fmt.Errorf("repository: mark outbox published: %w", err)
	}
	return nil
}

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

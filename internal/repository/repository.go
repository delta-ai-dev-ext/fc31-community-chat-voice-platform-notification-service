// Package repository holds the data-access layer of notification_service. All
// SQL is parameterized and all state lives in PostgreSQL.
package repository

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"notification_service/internal/models"
)

// ErrNotFound is returned when a requested row does not exist (or is not
// visible to the caller).
var ErrNotFound = errors.New("repository: not found")

// Querier is the subset of pgx used by the repositories. Both *pgxpool.Pool and
// pgx.Tx satisfy it, which lets the same code run inside and outside a
// transaction and makes the layer fakeable in tests.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

type txContextKey struct{}

// WithTx returns a context carrying q as the active transaction. Repositories
// bound to the pool automatically use the transaction carried by the context.
func WithTx(ctx context.Context, q Querier) context.Context {
	return context.WithValue(ctx, txContextKey{}, q)
}

// querier returns the transaction bound to ctx, or fallback when there is none.
func querier(ctx context.Context, fallback Querier) Querier {
	if tx, ok := ctx.Value(txContextKey{}).(Querier); ok && tx != nil {
		return tx
	}
	return fallback
}

// Transactor runs a function inside a single database transaction.
type Transactor interface {
	// InTx commits when fn returns nil and rolls back otherwise.
	InTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// NotificationRepository persists and reads in-app notifications.
type NotificationRepository interface {
	// Insert stores a notification and returns the persisted row.
	Insert(ctx context.Context, n models.NewNotification, createdAt time.Time) (models.Notification, error)
	// Get returns a notification of the given recipient; ErrNotFound when the
	// notification does not exist or belongs to another user.
	Get(ctx context.Context, recipientUserID, notificationID string) (models.Notification, error)
	// GetByID returns a notification regardless of recipient.
	GetByID(ctx context.Context, notificationID string) (models.Notification, error)
	// List returns at most opts.Limit notifications of the recipient, newest
	// first, starting after opts.Cursor.
	List(ctx context.Context, opts models.ListOptions) ([]models.Notification, error)
	// UnreadCount returns the total number of unread notifications of a user.
	UnreadCount(ctx context.Context, recipientUserID string) (int, error)
	// MarkRead sets read_at on one unread notification of the recipient. It
	// reports ErrNotFound for a missing or foreign notification.
	MarkRead(ctx context.Context, recipientUserID, notificationID string, readAt time.Time) (ReadOutcome, error)
	// MarkAllRead sets read_at on every unread notification of the recipient and
	// returns how many transitions happened per kind.
	MarkAllRead(ctx context.Context, recipientUserID string, readAt time.Time) (map[models.Kind]int, error)
}

// ReadOutcome describes the effect of marking a single notification read.
type ReadOutcome struct {
	// Kind is the kind of the addressed notification.
	Kind models.Kind
	// Changed is false when the notification was already read.
	Changed bool
}

// PreferenceRepository stores the per-kind toggles of a user.
type PreferenceRepository interface {
	// Get returns the stored preferences, or ErrNotFound when the user never
	// saved any.
	Get(ctx context.Context, userID string) (models.Preferences, error)
	// Upsert stores the full preference set and returns the saved value.
	Upsert(ctx context.Context, userID string, prefs models.Preferences, now time.Time) (models.Preferences, error)
}

// RecipientRepository maintains the local recipient projection (EVT-001).
type RecipientRepository interface {
	// Upsert applies a projection snapshot only when its account_version is
	// newer than the stored one; it reports whether the snapshot was applied.
	Upsert(ctx context.Context, projection models.RecipientProjection, now time.Time) (bool, error)
	// Get returns the projection of a user, or ErrNotFound when unknown.
	Get(ctx context.Context, userID string) (models.RecipientProjection, error)
}

// EmailDeliveryRepository records one email decision per notification.
type EmailDeliveryRepository interface {
	// Insert stores a delivery decision and returns the persisted row.
	Insert(ctx context.Context, delivery models.EmailDelivery) (models.EmailDelivery, error)
	// Update persists the mutable fields of a delivery (status, attempts,
	// last_error, next_attempt_at, provider_message_id).
	Update(ctx context.Context, delivery models.EmailDelivery, now time.Time) error
	// ListDue returns pending deliveries scheduled at or before due.
	ListDue(ctx context.Context, due time.Time, limit int) ([]models.EmailDelivery, error)
}

// ProcessedEventRepository deduplicates consumed events.
type ProcessedEventRepository interface {
	// Record inserts the event id and reports true when it had not been seen
	// before (the winner of the dedupe race).
	Record(ctx context.Context, eventID, topic string, now time.Time) (bool, error)
	// Prune deletes dedupe records older than before and returns how many rows
	// were removed.
	Prune(ctx context.Context, before time.Time) (int64, error)
}

// MentionWindowRepository is the persisted mention-email throttle store.
type MentionWindowRepository interface {
	// TryClaim reserves the recipient's mention window when no email was sent
	// inside window before now.
	TryClaim(ctx context.Context, recipientUserID string, now time.Time, window time.Duration) (bool, error)
}

// OutboxRepository stores the pending EVT-011 publications (ADR-001).
type OutboxRepository interface {
	// Enqueue records a pending publication for a committed notification and
	// returns the outbox row id.
	Enqueue(ctx context.Context, notificationID string, now time.Time) (string, error)
	// ListPending returns the oldest pending outbox rows.
	ListPending(ctx context.Context, limit int) ([]models.OutboxEntry, error)
	// MarkPublished marks an outbox row as published.
	MarkPublished(ctx context.Context, outboxID string, now time.Time) error
}

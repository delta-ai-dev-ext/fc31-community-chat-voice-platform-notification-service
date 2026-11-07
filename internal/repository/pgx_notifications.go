package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"notification_service/internal/models"
)

// PoolTransactor runs transactions against a pgxpool pool.
type PoolTransactor struct {
	pool *pgxpool.Pool
}

// NewPoolTransactor builds a Transactor over pool.
func NewPoolTransactor(pool *pgxpool.Pool) *PoolTransactor {
	return &PoolTransactor{pool: pool}
}

// InTx implements Transactor.
func (t *PoolTransactor) InTx(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := t.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("repository: begin transaction: %w", err)
	}
	if err := fn(WithTx(ctx, tx)); err != nil {
		if rbErr := tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return fmt.Errorf("repository: rollback after %v: %w", err, rbErr)
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("repository: commit transaction: %w", err)
	}
	return nil
}

// NotificationStore is the pgx implementation of NotificationRepository.
type NotificationStore struct {
	db Querier
}

// NewNotificationStore builds a NotificationRepository over db.
func NewNotificationStore(db Querier) *NotificationStore { return &NotificationStore{db: db} }

const notificationColumns = "id, recipient_user_id, kind, title, summary, link, created_at, read_at"

const insertNotificationSQL = `INSERT INTO notifications (recipient_user_id, kind, title, summary, link, created_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING ` + notificationColumns

// Insert implements NotificationRepository.
func (s *NotificationStore) Insert(ctx context.Context, n models.NewNotification, createdAt time.Time) (models.Notification, error) {
	row := querier(ctx, s.db).QueryRow(ctx, insertNotificationSQL,
		n.RecipientUserID, string(n.Kind), n.Title, n.Summary, n.Link, createdAt)
	notification, err := scanNotification(row)
	if err != nil {
		return models.Notification{}, fmt.Errorf("repository: insert notification: %w", err)
	}
	return notification, nil
}

const getNotificationSQL = `SELECT ` + notificationColumns + `
FROM notifications WHERE recipient_user_id = $1 AND id = $2`

// Get implements NotificationRepository.
func (s *NotificationStore) Get(ctx context.Context, recipientUserID, notificationID string) (models.Notification, error) {
	row := querier(ctx, s.db).QueryRow(ctx, getNotificationSQL, recipientUserID, notificationID)
	notification, err := scanNotification(row)
	if err != nil {
		return models.Notification{}, wrapNotFound("get notification", err)
	}
	return notification, nil
}

const getNotificationByIDSQL = `SELECT ` + notificationColumns + `
FROM notifications WHERE id = $1`

// GetByID implements NotificationRepository.
func (s *NotificationStore) GetByID(ctx context.Context, notificationID string) (models.Notification, error) {
	row := querier(ctx, s.db).QueryRow(ctx, getNotificationByIDSQL, notificationID)
	notification, err := scanNotification(row)
	if err != nil {
		return models.Notification{}, wrapNotFound("get notification by id", err)
	}
	return notification, nil
}

const listNotificationsSQL = `SELECT ` + notificationColumns + `
FROM notifications
WHERE recipient_user_id = $1
  AND ($2::boolean IS NOT TRUE OR read_at IS NULL)
  AND ($3::timestamptz IS NULL OR (created_at, id) < ($3::timestamptz, $4::uuid))
ORDER BY created_at DESC, id DESC
LIMIT $5`

// List implements NotificationRepository.
func (s *NotificationStore) List(ctx context.Context, opts models.ListOptions) ([]models.Notification, error) {
	var cursorAt *time.Time
	var cursorID *string
	if opts.Cursor != nil {
		at := opts.Cursor.CreatedAt
		id := opts.Cursor.ID
		cursorAt, cursorID = &at, &id
	}
	rows, err := querier(ctx, s.db).Query(ctx, listNotificationsSQL,
		opts.RecipientUserID, opts.UnreadOnly, cursorAt, cursorID, opts.Limit)
	if err != nil {
		return nil, fmt.Errorf("repository: list notifications: %w", err)
	}
	defer rows.Close()

	notifications := make([]models.Notification, 0, opts.Limit)
	for rows.Next() {
		notification, err := scanNotification(rows)
		if err != nil {
			return nil, fmt.Errorf("repository: scan notification: %w", err)
		}
		notifications = append(notifications, notification)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: iterate notifications: %w", err)
	}
	return notifications, nil
}

const unreadCountSQL = `SELECT count(*) FROM notifications
WHERE recipient_user_id = $1 AND read_at IS NULL`

// UnreadCount implements NotificationRepository.
func (s *NotificationStore) UnreadCount(ctx context.Context, recipientUserID string) (int, error) {
	var count int
	if err := querier(ctx, s.db).QueryRow(ctx, unreadCountSQL, recipientUserID).Scan(&count); err != nil {
		return 0, fmt.Errorf("repository: count unread notifications: %w", err)
	}
	return count, nil
}

const markReadSQL = `UPDATE notifications SET read_at = $3
WHERE id = $1 AND recipient_user_id = $2 AND read_at IS NULL
RETURNING kind`

const notificationKindSQL = `SELECT kind FROM notifications
WHERE id = $1 AND recipient_user_id = $2`

// MarkRead implements NotificationRepository.
func (s *NotificationStore) MarkRead(ctx context.Context, recipientUserID, notificationID string, readAt time.Time) (ReadOutcome, error) {
	var kind string
	err := querier(ctx, s.db).QueryRow(ctx, markReadSQL, notificationID, recipientUserID, readAt).Scan(&kind)
	if err == nil {
		return ReadOutcome{Kind: models.Kind(kind), Changed: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ReadOutcome{}, fmt.Errorf("repository: mark notification read: %w", err)
	}

	// Either the notification does not belong to this user, or it was already
	// read. Both cases are indistinguishable from the caller's perspective.
	var existing string
	if err := querier(ctx, s.db).QueryRow(ctx, notificationKindSQL, notificationID, recipientUserID).Scan(&existing); err != nil {
		return ReadOutcome{}, wrapNotFound("mark notification read", err)
	}
	return ReadOutcome{Kind: models.Kind(existing), Changed: false}, nil
}

const markAllReadSQL = `UPDATE notifications SET read_at = $2
WHERE recipient_user_id = $1 AND read_at IS NULL
RETURNING kind`

// MarkAllRead implements NotificationRepository.
func (s *NotificationStore) MarkAllRead(ctx context.Context, recipientUserID string, readAt time.Time) (map[models.Kind]int, error) {
	rows, err := querier(ctx, s.db).Query(ctx, markAllReadSQL, recipientUserID, readAt)
	if err != nil {
		return nil, fmt.Errorf("repository: mark all notifications read: %w", err)
	}
	defer rows.Close()

	counts := make(map[models.Kind]int)
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			return nil, fmt.Errorf("repository: scan marked kind: %w", err)
		}
		counts[models.Kind(kind)]++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: iterate marked kinds: %w", err)
	}
	return counts, nil
}

func scanNotification(row pgx.Row) (models.Notification, error) {
	var (
		notification models.Notification
		kind         string
	)
	err := row.Scan(
		&notification.ID,
		&notification.RecipientUserID,
		&kind,
		&notification.Title,
		&notification.Summary,
		&notification.Link,
		&notification.CreatedAt,
		&notification.ReadAt,
	)
	if err != nil {
		return models.Notification{}, err
	}
	notification.Kind = models.Kind(kind)
	return notification, nil
}

func wrapNotFound(action string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return fmt.Errorf("repository: %s: %w", action, err)
}

package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"notification_service/internal/models"
)

// EmailDeliveryStore is the pgx implementation of EmailDeliveryRepository.
type EmailDeliveryStore struct {
	db Querier
}

// NewEmailDeliveryStore builds an EmailDeliveryRepository over db.
func NewEmailDeliveryStore(db Querier) *EmailDeliveryStore { return &EmailDeliveryStore{db: db} }

const emailDeliveryColumns = "id, notification_id, kind, recipient_user_id, status, provider_message_id, attempts, last_error, next_attempt_at, created_at, updated_at"

const insertEmailDeliverySQL = `INSERT INTO email_deliveries (
    notification_id, kind, recipient_user_id, status, provider_message_id, attempts, last_error, next_attempt_at, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
RETURNING ` + emailDeliveryColumns

// Insert implements EmailDeliveryRepository.
func (s *EmailDeliveryStore) Insert(ctx context.Context, delivery models.EmailDelivery) (models.EmailDelivery, error) {
	now := delivery.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	row := querier(ctx, s.db).QueryRow(ctx, insertEmailDeliverySQL,
		delivery.NotificationID,
		string(delivery.Kind),
		delivery.RecipientUserID,
		string(delivery.Status),
		delivery.ProviderMessageID,
		delivery.Attempts,
		delivery.LastError,
		delivery.NextAttemptAt,
		now,
		now,
	)
	saved, err := scanEmailDelivery(row)
	if err != nil {
		return models.EmailDelivery{}, fmt.Errorf("repository: insert email delivery: %w", err)
	}
	return saved, nil
}

const updateEmailDeliverySQL = `UPDATE email_deliveries SET
    status = $2,
    provider_message_id = $3,
    attempts = $4,
    last_error = $5,
    next_attempt_at = $6,
    updated_at = $7
WHERE id = $1`

// Update implements EmailDeliveryRepository.
func (s *EmailDeliveryStore) Update(ctx context.Context, delivery models.EmailDelivery, now time.Time) error {
	if _, err := querier(ctx, s.db).Exec(ctx, updateEmailDeliverySQL,
		delivery.ID,
		string(delivery.Status),
		delivery.ProviderMessageID,
		delivery.Attempts,
		delivery.LastError,
		delivery.NextAttemptAt,
		now,
	); err != nil {
		return fmt.Errorf("repository: update email delivery: %w", err)
	}
	return nil
}

const listDueEmailDeliveriesSQL = `SELECT ` + emailDeliveryColumns + `
FROM email_deliveries
WHERE status = 'pending' AND next_attempt_at IS NOT NULL AND next_attempt_at <= $1
ORDER BY next_attempt_at
LIMIT $2`

// ListDue implements EmailDeliveryRepository.
func (s *EmailDeliveryStore) ListDue(ctx context.Context, due time.Time, limit int) ([]models.EmailDelivery, error) {
	rows, err := querier(ctx, s.db).Query(ctx, listDueEmailDeliveriesSQL, due, limit)
	if err != nil {
		return nil, fmt.Errorf("repository: list due email deliveries: %w", err)
	}
	defer rows.Close()

	deliveries := make([]models.EmailDelivery, 0, limit)
	for rows.Next() {
		delivery, err := scanEmailDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("repository: scan email delivery: %w", err)
		}
		deliveries = append(deliveries, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: iterate email deliveries: %w", err)
	}
	return deliveries, nil
}

func scanEmailDelivery(row pgx.Row) (models.EmailDelivery, error) {
	var (
		delivery models.EmailDelivery
		kind     string
		status   string
	)
	err := row.Scan(
		&delivery.ID,
		&delivery.NotificationID,
		&kind,
		&delivery.RecipientUserID,
		&status,
		&delivery.ProviderMessageID,
		&delivery.Attempts,
		&delivery.LastError,
		&delivery.NextAttemptAt,
		&delivery.CreatedAt,
		&delivery.UpdatedAt,
	)
	if err != nil {
		return models.EmailDelivery{}, err
	}
	delivery.Kind = models.Kind(kind)
	delivery.Status = models.DeliveryStatus(status)
	return delivery, nil
}

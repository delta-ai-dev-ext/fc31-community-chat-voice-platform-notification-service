package repository

import (
	"context"
	"fmt"
	"time"

	"notification_service/internal/models"
)

// RecipientStore is the pgx implementation of RecipientRepository.
type RecipientStore struct {
	db Querier
}

// NewRecipientStore builds a RecipientRepository over db.
func NewRecipientStore(db Querier) *RecipientStore { return &RecipientStore{db: db} }

const upsertRecipientSQL = `INSERT INTO recipient_projections (
    user_id, email, username, display_name, email_verified, status, account_version, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (user_id) DO UPDATE SET
    email = EXCLUDED.email,
    username = EXCLUDED.username,
    display_name = EXCLUDED.display_name,
    email_verified = EXCLUDED.email_verified,
    status = EXCLUDED.status,
    account_version = EXCLUDED.account_version,
    updated_at = EXCLUDED.updated_at
WHERE recipient_projections.account_version < EXCLUDED.account_version
RETURNING user_id`

// Upsert implements RecipientRepository: stale snapshots (account_version not
// newer than the stored one) are ignored.
func (s *RecipientStore) Upsert(ctx context.Context, projection models.RecipientProjection, now time.Time) (bool, error) {
	var appliedUserID string
	err := querier(ctx, s.db).QueryRow(ctx, upsertRecipientSQL,
		projection.UserID,
		projection.Email,
		projection.Username,
		projection.DisplayName,
		projection.EmailVerified,
		string(projection.Status),
		projection.AccountVersion,
		now,
	).Scan(&appliedUserID)
	if err != nil {
		if isNoRows(err) {
			return false, nil
		}
		return false, fmt.Errorf("repository: upsert recipient projection: %w", err)
	}
	return true, nil
}

const getRecipientSQL = `SELECT user_id, email, username, display_name, email_verified, status, account_version, updated_at
FROM recipient_projections WHERE user_id = $1`

// Get implements RecipientRepository.
func (s *RecipientStore) Get(ctx context.Context, userID string) (models.RecipientProjection, error) {
	var (
		projection models.RecipientProjection
		status     string
	)
	err := querier(ctx, s.db).QueryRow(ctx, getRecipientSQL, userID).Scan(
		&projection.UserID,
		&projection.Email,
		&projection.Username,
		&projection.DisplayName,
		&projection.EmailVerified,
		&status,
		&projection.AccountVersion,
		&projection.UpdatedAt,
	)
	if err != nil {
		return models.RecipientProjection{}, wrapNotFound("get recipient projection", err)
	}
	projection.Status = models.AccountStatus(status)
	return projection, nil
}

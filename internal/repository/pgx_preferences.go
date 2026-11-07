package repository

import (
	"context"
	"fmt"
	"time"

	"notification_service/internal/models"
)

// PreferenceStore is the pgx implementation of PreferenceRepository.
type PreferenceStore struct {
	db Querier
}

// NewPreferenceStore builds a PreferenceRepository over db.
func NewPreferenceStore(db Querier) *PreferenceStore { return &PreferenceStore{db: db} }

const getPreferencesSQL = `SELECT
    mention_in_app, mention_email,
    friend_request_received_in_app, friend_request_received_email,
    friend_request_accepted_in_app, friend_request_accepted_email,
    moderation_notice_in_app, moderation_notice_email,
    appeal_decided_in_app, appeal_decided_email
FROM notification_preferences WHERE user_id = $1`

// Get implements PreferenceRepository.
func (s *PreferenceStore) Get(ctx context.Context, userID string) (models.Preferences, error) {
	var prefs models.Preferences
	err := querier(ctx, s.db).QueryRow(ctx, getPreferencesSQL, userID).Scan(
		&prefs.Mention.InApp, &prefs.Mention.Email,
		&prefs.FriendRequestReceived.InApp, &prefs.FriendRequestReceived.Email,
		&prefs.FriendRequestAccepted.InApp, &prefs.FriendRequestAccepted.Email,
		&prefs.ModerationNotice.InApp, &prefs.ModerationNotice.Email,
		&prefs.AppealDecided.InApp, &prefs.AppealDecided.Email,
	)
	if err != nil {
		return models.Preferences{}, wrapNotFound("get notification preferences", err)
	}
	return prefs, nil
}

const upsertPreferencesSQL = `INSERT INTO notification_preferences (
    user_id,
    mention_in_app, mention_email,
    friend_request_received_in_app, friend_request_received_email,
    friend_request_accepted_in_app, friend_request_accepted_email,
    moderation_notice_in_app, moderation_notice_email,
    appeal_decided_in_app, appeal_decided_email,
    updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (user_id) DO UPDATE SET
    mention_in_app = EXCLUDED.mention_in_app,
    mention_email = EXCLUDED.mention_email,
    friend_request_received_in_app = EXCLUDED.friend_request_received_in_app,
    friend_request_received_email = EXCLUDED.friend_request_received_email,
    friend_request_accepted_in_app = EXCLUDED.friend_request_accepted_in_app,
    friend_request_accepted_email = EXCLUDED.friend_request_accepted_email,
    moderation_notice_in_app = EXCLUDED.moderation_notice_in_app,
    moderation_notice_email = EXCLUDED.moderation_notice_email,
    appeal_decided_in_app = EXCLUDED.appeal_decided_in_app,
    appeal_decided_email = EXCLUDED.appeal_decided_email,
    updated_at = EXCLUDED.updated_at`

// Upsert implements PreferenceRepository.
func (s *PreferenceStore) Upsert(ctx context.Context, userID string, prefs models.Preferences, now time.Time) (models.Preferences, error) {
	_, err := querier(ctx, s.db).Exec(ctx, upsertPreferencesSQL,
		userID,
		prefs.Mention.InApp, prefs.Mention.Email,
		prefs.FriendRequestReceived.InApp, prefs.FriendRequestReceived.Email,
		prefs.FriendRequestAccepted.InApp, prefs.FriendRequestAccepted.Email,
		prefs.ModerationNotice.InApp, prefs.ModerationNotice.Email,
		prefs.AppealDecided.InApp, prefs.AppealDecided.Email,
		now,
	)
	if err != nil {
		return models.Preferences{}, fmt.Errorf("repository: upsert notification preferences: %w", err)
	}
	return prefs, nil
}

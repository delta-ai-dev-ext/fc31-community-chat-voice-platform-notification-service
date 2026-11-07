package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"notification_service/internal/models"
	"notification_service/internal/repository"
	"notification_service/internal/service"
	"notification_service/tests/mocks"
)

const fullPreferencesBody = `{
  "mention": {"in_app": false, "email": true},
  "friend_request_received": {"in_app": true, "email": true},
  "friend_request_accepted": {"in_app": true, "email": false},
  "moderation_notice": {"in_app": false, "email": false},
  "appeal_decided": {"in_app": true, "email": true}
}`

func TestPreferencesGet(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 2, 1, 8, 0, 0, 0, time.UTC)

	t.Run("stored preferences", func(t *testing.T) {
		repo := &mocks.PreferenceRepository{}
		svc := service.NewPreferenceService(repo, fixedClock(now))
		stored := models.DefaultPreferences()
		stored.Mention.Email = false
		repo.On("Get", mock.Anything, "user-1").Return(stored, nil).Once()

		prefs, err := svc.Get(context.Background(), "user-1")
		require.NoError(t, err)
		require.False(t, prefs.Mention.Email)
	})

	t.Run("defaults when no row exists", func(t *testing.T) {
		repo := &mocks.PreferenceRepository{}
		svc := service.NewPreferenceService(repo, fixedClock(now))
		repo.On("Get", mock.Anything, "user-1").Return(models.Preferences{}, repository.ErrNotFound).Once()

		prefs, err := svc.Get(context.Background(), "user-1")
		require.NoError(t, err)
		require.Equal(t, models.DefaultPreferences(), prefs)
		for _, kind := range models.AllKinds {
			require.True(t, prefs.For(kind).InApp, string(kind))
			require.True(t, prefs.For(kind).Email, string(kind))
		}
	})

	t.Run("repository failure", func(t *testing.T) {
		repo := &mocks.PreferenceRepository{}
		svc := service.NewPreferenceService(repo, fixedClock(now))
		repo.On("Get", mock.Anything, "user-1").Return(models.Preferences{}, errors.New("db down")).Once()

		_, err := svc.Get(context.Background(), "user-1")
		require.Error(t, err)
	})
}

func TestPreferencesReplace(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 2, 1, 8, 0, 0, 0, time.UTC)

	expected := models.DefaultPreferences()
	expected.Mention.InApp = false
	expected.FriendRequestAccepted.Email = false
	expected.ModerationNotice = models.KindPreferences{InApp: false, Email: false}

	repo := &mocks.PreferenceRepository{}
	svc := service.NewPreferenceService(repo, fixedClock(now))
	repo.On("Upsert", mock.Anything, "user-1", expected, now).Return(expected, nil).Once()

	saved, err := svc.Replace(context.Background(), "user-1", []byte(fullPreferencesBody))
	require.NoError(t, err)
	require.Equal(t, expected, saved)
}

func TestPreferencesReplaceRejectsInvalidBodies(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		body     string
		wantCode string
		wantStat int
	}{
		{
			name:     "missing kind",
			body:     `{"mention":{"in_app":true,"email":true},"friend_request_received":{"in_app":true,"email":true},"friend_request_accepted":{"in_app":true,"email":true},"moderation_notice":{"in_app":true,"email":true}}`,
			wantCode: models.CodeInvalidPreferences,
			wantStat: 422,
		},
		{
			name:     "unknown kind",
			body:     `{"mention":{"in_app":true,"email":true},"friend_request_received":{"in_app":true,"email":true},"friend_request_accepted":{"in_app":true,"email":true},"moderation_notice":{"in_app":true,"email":true},"appeal_decided":{"in_app":true,"email":true},"system_message":{"in_app":true,"email":true}}`,
			wantCode: models.CodeInvalidPreferences,
			wantStat: 422,
		},
		{
			name:     "missing toggle",
			body:     `{"mention":{"in_app":true},"friend_request_received":{"in_app":true,"email":true},"friend_request_accepted":{"in_app":true,"email":true},"moderation_notice":{"in_app":true,"email":true},"appeal_decided":{"in_app":true,"email":true}}`,
			wantCode: models.CodeInvalidPreferences,
			wantStat: 422,
		},
		{
			name:     "extra toggle",
			body:     `{"mention":{"in_app":true,"email":true,"push":true},"friend_request_received":{"in_app":true,"email":true},"friend_request_accepted":{"in_app":true,"email":true},"moderation_notice":{"in_app":true,"email":true},"appeal_decided":{"in_app":true,"email":true}}`,
			wantCode: models.CodeInvalidPreferences,
			wantStat: 422,
		},
		{
			name:     "non boolean toggle",
			body:     `{"mention":{"in_app":"yes","email":true},"friend_request_received":{"in_app":true,"email":true},"friend_request_accepted":{"in_app":true,"email":true},"moderation_notice":{"in_app":true,"email":true},"appeal_decided":{"in_app":true,"email":true}}`,
			wantCode: models.CodeInvalidPreferences,
			wantStat: 422,
		},
		{
			name:     "kind is not an object",
			body:     `{"mention":true,"friend_request_received":{"in_app":true,"email":true},"friend_request_accepted":{"in_app":true,"email":true},"moderation_notice":{"in_app":true,"email":true},"appeal_decided":{"in_app":true,"email":true}}`,
			wantCode: models.CodeInvalidPreferences,
			wantStat: 422,
		},
		{
			name:     "empty body",
			body:     ``,
			wantCode: models.CodeInvalidRequest,
			wantStat: 400,
		},
		{
			name:     "malformed json",
			body:     `{"mention":`,
			wantCode: models.CodeInvalidRequest,
			wantStat: 400,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			repo := &mocks.PreferenceRepository{}
			svc := service.NewPreferenceService(repo, fixedClock(time.Now().UTC()))

			_, err := svc.Replace(context.Background(), "user-1", []byte(test.body))
			require.Error(t, err)
			var apiErr *models.APIError
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, test.wantCode, apiErr.Code)
			require.Equal(t, test.wantStat, apiErr.Status)
			repo.AssertNotCalled(t, "Upsert", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

func TestPreferencesReplaceRepositoryFailure(t *testing.T) {
	t.Parallel()
	repo := &mocks.PreferenceRepository{}
	svc := service.NewPreferenceService(repo, nil)
	repo.On("Upsert", mock.Anything, "user-1", mock.Anything, mock.Anything).
		Return(models.Preferences{}, errors.New("db down")).Once()

	_, err := svc.Replace(context.Background(), "user-1", []byte(fullPreferencesBody))
	require.Error(t, err)
}

func TestPreferencesModerationNoticeIsAlwaysDelivered(t *testing.T) {
	t.Parallel()
	prefs := models.DefaultPreferences()
	prefs.ModerationNotice = models.KindPreferences{InApp: false, Email: false}
	require.True(t, prefs.EmailAllowed(models.KindModerationNotice))
	require.True(t, prefs.InAppAllowed(models.KindModerationNotice))

	prefs.Mention = models.KindPreferences{InApp: false, Email: false}
	require.False(t, prefs.EmailAllowed(models.KindMention))
	require.False(t, prefs.InAppAllowed(models.KindMention))
}

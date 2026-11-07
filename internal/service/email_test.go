package service_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"notification_service/internal/models"
	"notification_service/internal/provider"
	"notification_service/internal/repository"
	"notification_service/internal/service"
)

func dueDelivery(status models.DeliveryStatus, attempts int) models.EmailDelivery {
	due := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	return models.EmailDelivery{
		ID:              "delivery-1",
		NotificationID:  notifID,
		Kind:            models.KindMention,
		RecipientUserID: userBob,
		Status:          status,
		Attempts:        attempts,
		NextAttemptAt:   &due,
	}
}

func storedMention() models.Notification {
	return models.Notification{
		ID:              notifID,
		RecipientUserID: userBob,
		Kind:            models.KindMention,
		Title:           "Alice mentioned you",
		Summary:         "in a channel",
		Link:            "/channels/c1/messages/m1",
		CreatedAt:       time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC),
	}
}

func TestRetryDueSendsPendingDelivery(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.deliveries.On("ListDue", mock.Anything, harness.now, 25).Return([]models.EmailDelivery{dueDelivery(models.DeliveryPending, 1)}, nil).Once()
	harness.notifications.On("GetByID", mock.Anything, notifID).Return(storedMention(), nil).Once()
	harness.recipients.On("Get", mock.Anything, userBob).Return(activeProjection(userBob), nil).Once()
	harness.sender.On("Send", mock.Anything, mock.MatchedBy(func(message provider.EmailMessage) bool {
		return message.IdempotencyKey == notifID
	})).Return("resend-2", nil).Once()
	harness.deliveries.On("Update", mock.Anything, mock.MatchedBy(func(d models.EmailDelivery) bool {
		return d.Status == models.DeliverySent && d.Attempts == 2
	}), mock.Anything).Return(nil).Once()

	require.NoError(t, harness.svc.RetryDue(context.Background(), 25))
	require.Equal(t, 1, harness.observer.emails["mention/sent"])
}

func TestRetryDueReschedulesRetryableFailureWithBackoff(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.deliveries.On("ListDue", mock.Anything, harness.now, 10).Return([]models.EmailDelivery{dueDelivery(models.DeliveryPending, 1)}, nil).Once()
	harness.notifications.On("GetByID", mock.Anything, notifID).Return(storedMention(), nil).Once()
	harness.recipients.On("Get", mock.Anything, userBob).Return(activeProjection(userBob), nil).Once()
	harness.sender.On("Send", mock.Anything, mock.Anything).
		Return("", &provider.SendError{Kind: provider.FailureRetryable, StatusCode: 503, Err: errors.New("provider unavailable")}).Once()
	harness.deliveries.On("Update", mock.Anything, mock.MatchedBy(func(d models.EmailDelivery) bool {
		expectedNext := harness.now.Add(service.DefaultRetryBackoff(2))
		return d.Status == models.DeliveryPending && d.Attempts == 2 &&
			d.NextAttemptAt != nil && d.NextAttemptAt.Equal(expectedNext) && d.LastError != nil
	}), harness.now).Return(nil).Once()

	require.NoError(t, harness.svc.RetryDue(context.Background(), 10))
	require.Empty(t, harness.observer.emails, "a scheduled retry is not counted as a final result")
}

func TestRetryDueHonoursProviderRetryAfter(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.deliveries.On("ListDue", mock.Anything, harness.now, 10).Return([]models.EmailDelivery{dueDelivery(models.DeliveryPending, 1)}, nil).Once()
	harness.notifications.On("GetByID", mock.Anything, notifID).Return(storedMention(), nil).Once()
	harness.recipients.On("Get", mock.Anything, userBob).Return(activeProjection(userBob), nil).Once()
	harness.sender.On("Send", mock.Anything, mock.Anything).
		Return("", &provider.SendError{Kind: provider.FailureRetryable, StatusCode: 429, RetryAfter: 10 * time.Minute, Err: errors.New("rate limited")}).Once()
	harness.deliveries.On("Update", mock.Anything, mock.MatchedBy(func(d models.EmailDelivery) bool {
		return d.Status == models.DeliveryPending && d.NextAttemptAt != nil &&
			d.NextAttemptAt.Equal(harness.now.Add(10*time.Minute))
	}), harness.now).Return(nil).Once()

	require.NoError(t, harness.svc.RetryDue(context.Background(), 10))
}

func TestRetryDueStopsAfterTheBoundedBudget(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	spent := dueDelivery(models.DeliveryPending, service.DefaultMaxEmailAttempts)
	harness.deliveries.On("ListDue", mock.Anything, harness.now, 10).Return([]models.EmailDelivery{spent}, nil).Once()
	harness.notifications.On("GetByID", mock.Anything, notifID).Return(storedMention(), nil).Once()
	harness.recipients.On("Get", mock.Anything, userBob).Return(activeProjection(userBob), nil).Once()
	harness.sender.On("Send", mock.Anything, mock.Anything).
		Return("", &provider.SendError{Kind: provider.FailureRetryable, StatusCode: 500, Err: errors.New("still down")}).Once()
	harness.deliveries.On("Update", mock.Anything, mock.MatchedBy(func(d models.EmailDelivery) bool {
		return d.Status == models.DeliveryFailed && d.Attempts == service.DefaultMaxEmailAttempts && d.NextAttemptAt == nil
	}), harness.now).Return(nil).Once()

	require.NoError(t, harness.svc.RetryDue(context.Background(), 10))
	require.Equal(t, 1, harness.observer.emails["mention/failed"])
}

func TestRetryDueRecordsPermanentFailureImmediately(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.deliveries.On("ListDue", mock.Anything, harness.now, 10).Return([]models.EmailDelivery{dueDelivery(models.DeliveryPending, 0)}, nil).Once()
	harness.notifications.On("GetByID", mock.Anything, notifID).Return(storedMention(), nil).Once()
	harness.recipients.On("Get", mock.Anything, userBob).Return(activeProjection(userBob), nil).Once()
	harness.sender.On("Send", mock.Anything, mock.Anything).
		Return("", &provider.SendError{Kind: provider.FailurePermanent, StatusCode: 422, Err: errors.New("invalid recipient")}).Once()
	harness.deliveries.On("Update", mock.Anything, mock.MatchedBy(func(d models.EmailDelivery) bool {
		return d.Status == models.DeliveryFailed && d.Attempts == 1 && d.NextAttemptAt == nil
	}), harness.now).Return(nil).Once()

	require.NoError(t, harness.svc.RetryDue(context.Background(), 10))
	require.Equal(t, 1, harness.observer.emails["mention/failed"])
}

func TestRetryDueReEvaluatesEligibility(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		projection models.RecipientProjection
		wantStatus models.DeliveryStatus
	}{
		{
			name: "became unverified",
			projection: func() models.RecipientProjection {
				projection := activeProjection(userBob)
				projection.EmailVerified = false
				return projection
			}(),
			wantStatus: models.DeliverySkippedUnverified,
		},
		{
			name: "became deactivated",
			projection: func() models.RecipientProjection {
				projection := activeProjection(userBob)
				projection.Status = models.AccountDeactivated
				return projection
			}(),
			wantStatus: models.DeliverySkippedDeactivated,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			harness := newIngestHarness(t)
			harness.deliveries.On("ListDue", mock.Anything, harness.now, 10).Return([]models.EmailDelivery{dueDelivery(models.DeliveryPending, 1)}, nil).Once()
			harness.notifications.On("GetByID", mock.Anything, notifID).Return(storedMention(), nil).Once()
			harness.recipients.On("Get", mock.Anything, userBob).Return(test.projection, nil).Once()
			harness.deliveries.On("Update", mock.Anything, mock.MatchedBy(func(d models.EmailDelivery) bool {
				return d.Status == test.wantStatus && d.NextAttemptAt == nil
			}), harness.now).Return(nil).Once()

			require.NoError(t, harness.svc.RetryDue(context.Background(), 10))
			harness.sender.AssertNotCalled(t, "Send", mock.Anything, mock.Anything)
			require.Equal(t, 1, harness.observer.emails["mention/"+string(test.wantStatus)])
		})
	}
}

func TestRetryDueHandlesMissingProjectionWithinBudget(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.deliveries.On("ListDue", mock.Anything, harness.now, 10).Return([]models.EmailDelivery{dueDelivery(models.DeliveryPending, 0)}, nil).Once()
	harness.notifications.On("GetByID", mock.Anything, notifID).Return(storedMention(), nil).Once()
	harness.recipients.On("Get", mock.Anything, userBob).Return(models.RecipientProjection{}, repository.ErrNotFound).Once()
	harness.deliveries.On("Update", mock.Anything, mock.MatchedBy(func(d models.EmailDelivery) bool {
		return d.Status == models.DeliveryPending && d.Attempts == 1 && d.NextAttemptAt != nil
	}), harness.now).Return(nil).Once()

	require.NoError(t, harness.svc.RetryDue(context.Background(), 10))
}

func TestRetryDueSkipsUnknownNotifications(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.deliveries.On("ListDue", mock.Anything, harness.now, 10).Return([]models.EmailDelivery{dueDelivery(models.DeliveryPending, 1)}, nil).Once()
	harness.notifications.On("GetByID", mock.Anything, notifID).Return(models.Notification{}, repository.ErrNotFound).Once()

	require.NoError(t, harness.svc.RetryDue(context.Background(), 10))
	harness.sender.AssertNotCalled(t, "Send", mock.Anything, mock.Anything)
}

func TestRetryDueListFailure(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)
	harness.deliveries.On("ListDue", mock.Anything, mock.Anything, mock.Anything).Return(([]models.EmailDelivery)(nil), errors.New("db down")).Once()

	require.Error(t, harness.svc.RetryDue(context.Background(), 0))
}

func TestRetryDueRecordsUpdateFailure(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.deliveries.On("ListDue", mock.Anything, harness.now, 10).Return([]models.EmailDelivery{dueDelivery(models.DeliveryPending, 1)}, nil).Once()
	harness.notifications.On("GetByID", mock.Anything, notifID).Return(storedMention(), nil).Once()
	harness.recipients.On("Get", mock.Anything, userBob).Return(activeProjection(userBob), nil).Once()
	harness.sender.On("Send", mock.Anything, mock.Anything).Return("resend-3", nil).Once()
	harness.deliveries.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("db down")).Once()

	require.NoError(t, harness.svc.RetryDue(context.Background(), 10))
	require.Empty(t, harness.observer.emails, "an unrecorded outcome is not counted as sent")
}

func TestRenderNotificationEmail(t *testing.T) {
	t.Parallel()
	subject, body := service.RenderNotificationEmail(storedMention())
	require.Equal(t, "Alice mentioned you", subject)
	require.True(t, strings.Contains(body, "in a channel"))
	require.True(t, strings.Contains(body, "/channels/c1/messages/m1"))
	require.True(t, strings.Contains(body, "notification settings"))
	_ = zap.NewNop()
}

func TestDefaultRetryBackoffIsBounded(t *testing.T) {
	t.Parallel()
	require.Equal(t, 30*time.Second, service.DefaultRetryBackoff(1))
	require.Equal(t, time.Minute, service.DefaultRetryBackoff(2))
	require.Equal(t, 8*time.Minute, service.DefaultRetryBackoff(5))
	require.Equal(t, 15*time.Minute, service.DefaultRetryBackoff(9), "the back-off is capped")
	require.Equal(t, 30*time.Second, service.DefaultRetryBackoff(0))
}

func TestMentionThrottleWindowBoundary(t *testing.T) {
	t.Parallel()
	// The claim comparison happens in SQL against now-window; the service must
	// hand over the injected clock and the configured window only.
	harness := newIngestHarness(t)
	harness.processed.On("Record", mock.Anything, eventID, models.TopicMessageEvents, harness.now).Return(true, nil).Once()
	harness.preferences.On("Get", mock.Anything, userBob).Return(models.DefaultPreferences(), nil).Once()
	harness.recipients.On("Get", mock.Anything, mock.Anything).Return(activeProjection(userBob), nil).Maybe()
	harness.windows.On("TryClaim", mock.Anything, userBob, harness.now, 15*time.Minute).Return(true, nil).Once()
	harness.expectInsert(t, models.KindMention, userBob, models.DeliveryPending)
	harness.outbox.On("Enqueue", mock.Anything, notifID, harness.now).Return(outboxID, nil).Once()
	harness.sender.On("Send", mock.Anything, mock.Anything).Return("resend-1", nil).Once()
	harness.deliveries.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	harness.publisher.On("Publish", mock.Anything, outboxID, mock.Anything).Return(nil).Once()

	message := buildMessage(t, models.EventTypeMessageChanged, "messaging_service", models.MessageChanged{
		MessageID: "99999999-9999-4999-8999-999999999999", Change: models.MessageChangeCreated,
		ContainerType: models.ContainerTypeConversation, ContainerID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		AuthorUserID: userAlice, MentionedUserIDs: []string{userBob}, MessageVersion: 1,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicMessageEvents, message))
	require.Equal(t, 1, harness.observer.emails["mention/sent"])
}

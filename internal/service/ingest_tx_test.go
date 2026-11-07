package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"notification_service/internal/models"
	"notification_service/internal/service"
)

// trackingTransactor records whether repository calls happen inside the
// transaction callback.
type trackingTransactor struct {
	active    bool
	completed int
}

func (t *trackingTransactor) InTx(ctx context.Context, fn func(context.Context) error) error {
	t.active = true
	defer func() { t.active = false }()
	if err := fn(ctx); err != nil {
		return err
	}
	t.completed++
	return nil
}

func TestIngestCommitsNotificationAndOutboxRowInOneTransaction(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)
	transactor := &trackingTransactor{}

	harness.svc = service.NewIngestService(service.IngestDeps{
		Transactor:            transactor,
		Notifications:         harness.notifications,
		Preferences:           harness.preferences,
		Recipients:            harness.recipients,
		Deliveries:            harness.deliveries,
		Windows:               harness.windows,
		Processed:             harness.processed,
		Outbox:                harness.outbox,
		Publisher:             harness.publisher,
		Sender:                harness.sender,
		Observer:              harness.observer,
		Logger:                zap.NewNop(),
		Clock:                 fixedClock(harness.now),
		MentionThrottleWindow: 15 * 60 * 1e9,
	})

	inTx := func(t *testing.T) func(mock.Arguments) {
		return func(mock.Arguments) { require.True(t, transactor.active, "call must run inside the transaction") }
	}

	harness.processed.On("Record", mock.Anything, eventID, models.TopicSocialEvents, harness.now).
		Run(inTx(t)).Return(true, nil).Once()
	harness.preferences.On("Get", mock.Anything, userBob).Return(models.DefaultPreferences(), nil).Once()
	harness.recipients.On("Get", mock.Anything, mock.Anything).Return(activeProjection(userBob), nil).Maybe()
	harness.notifications.On("Insert", mock.Anything, mock.MatchedBy(func(n models.NewNotification) bool {
		return n.Kind == models.KindFriendRequestReceived && n.RecipientUserID == userBob
	}), harness.now).
		Run(inTx(t)).
		Return(models.Notification{
			ID: notifID, RecipientUserID: userBob, Kind: models.KindFriendRequestReceived,
			Title: "title", Summary: "summary", Link: "/friends/requests", CreatedAt: harness.now,
		}, nil).Once()
	harness.deliveries.On("Insert", mock.Anything, mock.Anything).
		Run(inTx(t)).
		Return(models.EmailDelivery{
			ID: "delivery-1", NotificationID: notifID, Kind: models.KindFriendRequestReceived,
			RecipientUserID: userBob, Status: models.DeliveryPending,
		}, nil).Once()
	harness.outbox.On("Enqueue", mock.Anything, notifID, harness.now).
		Run(inTx(t)).
		Return(outboxID, nil).Once()
	harness.sender.On("Send", mock.Anything, mock.Anything).Return("resend-1", nil).Once()
	harness.deliveries.On("Update", mock.Anything, mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { require.False(t, transactor.active, "email sending happens after commit") }).
		Return(nil).Once()
	harness.publisher.On("Publish", mock.Anything, outboxID, mock.Anything).
		Run(func(mock.Arguments) { require.False(t, transactor.active, "publishing happens after commit") }).
		Return(nil).Once()

	message := buildMessage(t, models.EventTypeFriendRequestChanged, "social_service", models.FriendRequestChanged{
		RequestID: "88888888-8888-4888-8888-888888888888", SenderUserID: userAlice,
		RecipientUserID: userBob, Status: models.FriendRequestPending,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicSocialEvents, message))
	require.Equal(t, 1, transactor.completed)
}

func TestIngestRollsBackWhenOutboxEnqueueFails(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)
	transactor := &trackingTransactor{}

	harness.svc = service.NewIngestService(service.IngestDeps{
		Transactor:    transactor,
		Notifications: harness.notifications,
		Preferences:   harness.preferences,
		Recipients:    harness.recipients,
		Deliveries:    harness.deliveries,
		Windows:       harness.windows,
		Processed:     harness.processed,
		Outbox:        harness.outbox,
		Publisher:     harness.publisher,
		Sender:        harness.sender,
		Observer:      harness.observer,
		Logger:        zap.NewNop(),
		Clock:         fixedClock(harness.now),
	})

	harness.processed.On("Record", mock.Anything, eventID, models.TopicSocialEvents, harness.now).Return(true, nil).Once()
	harness.preferences.On("Get", mock.Anything, userBob).Return(models.DefaultPreferences(), nil).Once()
	harness.recipients.On("Get", mock.Anything, mock.Anything).Return(activeProjection(userBob), nil).Maybe()
	harness.expectInsert(t, models.KindFriendRequestReceived, userBob, models.DeliveryPending)
	harness.outbox.On("Enqueue", mock.Anything, notifID, harness.now).Return("", errors.New("outbox unavailable")).Once()

	message := buildMessage(t, models.EventTypeFriendRequestChanged, "social_service", models.FriendRequestChanged{
		RequestID: "88888888-8888-4888-8888-888888888888", SenderUserID: userAlice,
		RecipientUserID: userBob, Status: models.FriendRequestPending,
	})
	require.Error(t, harness.svc.HandleMessage(context.Background(), models.TopicSocialEvents, message),
		"the transaction must fail so the event is retried instead of losing the push")
	require.Zero(t, transactor.completed)
	harness.publisher.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything)
	harness.sender.AssertNotCalled(t, "Send", mock.Anything, mock.Anything)
}

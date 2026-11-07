package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"notification_service/internal/models"
	"notification_service/internal/provider"
	"notification_service/internal/repository"
	"notification_service/internal/service"
	"notification_service/tests/mocks"
)

const (
	userAlice = "11111111-1111-4111-8111-111111111111"
	userBob   = "22222222-2222-4222-8222-222222222222"
	userCarol = "33333333-3333-4333-8333-333333333333"
	traceID   = "4bf92f3577b34da6a3ce929d0e0e4736"
	corrID    = "44444444-4444-4444-8444-444444444444"
	eventID   = "55555555-5555-4555-8555-555555555555"
	notifID   = "66666666-6666-4666-8666-666666666666"
	outboxID  = "77777777-7777-4777-8777-777777777777"
)

type ingestHarness struct {
	svc           *service.IngestService
	notifications *mocks.NotificationRepository
	preferences   *mocks.PreferenceRepository
	recipients    *mocks.RecipientRepository
	deliveries    *mocks.EmailDeliveryRepository
	windows       *mocks.MentionWindowRepository
	processed     *mocks.ProcessedEventRepository
	outbox        *mocks.OutboxRepository
	publisher     *mocks.Publisher
	sender        *mocks.EmailSender
	observer      *recordingObserver
	now           time.Time
}

func newIngestHarness(t *testing.T) *ingestHarness {
	t.Helper()
	harness := &ingestHarness{
		notifications: &mocks.NotificationRepository{},
		preferences:   &mocks.PreferenceRepository{},
		recipients:    &mocks.RecipientRepository{},
		deliveries:    &mocks.EmailDeliveryRepository{},
		windows:       &mocks.MentionWindowRepository{},
		processed:     &mocks.ProcessedEventRepository{},
		outbox:        &mocks.OutboxRepository{},
		publisher:     &mocks.Publisher{},
		sender:        &mocks.EmailSender{},
		observer:      newRecordingObserver(),
		now:           time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC),
	}
	harness.svc = service.NewIngestService(service.IngestDeps{
		Transactor:            &mocks.Transactor{},
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
		NewID:                 func() string { return notifID },
		MailFrom:              "notifications@example.com",
		MentionThrottleWindow: 15 * time.Minute,
	})
	return harness
}

// expectInsert wires the notification + delivery insert expectations.
func (h *ingestHarness) expectInsert(t *testing.T, kind models.Kind, recipient string, status models.DeliveryStatus) {
	t.Helper()
	h.notifications.On("Insert", mock.Anything, mock.MatchedBy(func(n models.NewNotification) bool {
		return n.Kind == kind && n.RecipientUserID == recipient
	}), h.now).Return(models.Notification{
		ID:              notifID,
		RecipientUserID: recipient,
		Kind:            kind,
		Title:           "title",
		Summary:         "summary",
		Link:            "/link",
		CreatedAt:       h.now,
	}, nil).Once()

	h.deliveries.On("Insert", mock.Anything, mock.MatchedBy(func(d models.EmailDelivery) bool {
		return d.NotificationID == notifID && d.Kind == kind && d.RecipientUserID == recipient && d.Status == status
	})).Return(models.EmailDelivery{
		ID:              "delivery-1",
		NotificationID:  notifID,
		Kind:            kind,
		RecipientUserID: recipient,
		Status:          status,
		CreatedAt:       h.now,
	}, nil).Once()
}

func buildMessage(t *testing.T, eventType, producer string, payload any) []byte {
	t.Helper()
	envelope := models.Envelope{
		EventID:       eventID,
		EventType:     eventType,
		EventVersion:  1,
		OccurredAt:    time.Date(2026, 2, 1, 9, 59, 0, 0, time.UTC),
		Producer:      producer,
		TraceID:       traceID,
		CorrelationID: corrID,
		SchemaRef:     "EVENT_CONTRACTS.md#" + eventType + "-v1",
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	encoded, err := json.Marshal(models.Message{Envelope: envelope, Payload: payloadBytes})
	require.NoError(t, err)
	return encoded
}

func activeProjection(userID string) models.RecipientProjection {
	return models.RecipientProjection{
		UserID:         userID,
		Email:          userID + "@example.com",
		Username:       "user_" + userID[:4],
		DisplayName:    "User " + userID[:4],
		EmailVerified:  true,
		Status:         models.AccountActive,
		AccountVersion: 2,
	}
}

func TestIngestCreatesNotificationWithEmailAndOutboxRow(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.processed.On("Record", mock.Anything, eventID, models.TopicSocialEvents, harness.now).Return(true, nil).Once()
	harness.preferences.On("Get", mock.Anything, userBob).Return(models.DefaultPreferences(), nil).Once()
	harness.recipients.On("Get", mock.Anything, userAlice).Return(activeProjection(userAlice), nil).Maybe()
	harness.recipients.On("Get", mock.Anything, userBob).Return(activeProjection(userBob), nil).Maybe()
	harness.expectInsert(t, models.KindFriendRequestReceived, userBob, models.DeliveryPending)
	harness.outbox.On("Enqueue", mock.Anything, notifID, harness.now).Return(outboxID, nil).Once()
	harness.sender.On("Send", mock.Anything, mock.MatchedBy(func(message provider.EmailMessage) bool {
		return message.IdempotencyKey == notifID && message.To == userBob+"@example.com" && message.From == "notifications@example.com"
	})).Return("resend-1", nil).Once()
	harness.deliveries.On("Update", mock.Anything, mock.MatchedBy(func(d models.EmailDelivery) bool {
		return d.Status == models.DeliverySent && d.ProviderMessageID != nil && *d.ProviderMessageID == "resend-1"
	}), mock.Anything).Return(nil).Once()
	harness.publisher.On("Publish", mock.Anything, outboxID, mock.MatchedBy(func(cause models.Envelope) bool {
		return cause.EventID == eventID && cause.TraceID == traceID && cause.CorrelationID == corrID
	})).Return(nil).Once()

	message := buildMessage(t, models.EventTypeFriendRequestChanged, "social_service", models.FriendRequestChanged{
		RequestID:       "88888888-8888-4888-8888-888888888888",
		SenderUserID:    userAlice,
		RecipientUserID: userBob,
		Status:          models.FriendRequestPending,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicSocialEvents, message))

	harness.notifications.AssertNumberOfCalls(t, "Insert", 1)
	require.Equal(t, 1, harness.observer.created[models.KindFriendRequestReceived])
	require.Equal(t, 1, harness.observer.emails["friend_request_received/sent"])
}

func TestIngestDeduplicatesRedeliveredEvent(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.processed.On("Record", mock.Anything, eventID, models.TopicSocialEvents, harness.now).Return(false, nil).Once()
	harness.recipients.On("Get", mock.Anything, mock.Anything).Return(activeProjection(userAlice), nil).Maybe()

	message := buildMessage(t, models.EventTypeFriendRequestChanged, "social_service", models.FriendRequestChanged{
		RequestID:       "88888888-8888-4888-8888-888888888888",
		SenderUserID:    userAlice,
		RecipientUserID: userBob,
		Status:          models.FriendRequestPending,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicSocialEvents, message))

	harness.notifications.AssertNotCalled(t, "Insert", mock.Anything, mock.Anything, mock.Anything)
	harness.outbox.AssertNotCalled(t, "Enqueue", mock.Anything, mock.Anything, mock.Anything)
	harness.publisher.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything)
	require.Empty(t, harness.observer.created)
}

func TestIngestDropsMalformedMessages(t *testing.T) {
	t.Parallel()
	envelope := models.Envelope{
		EventID:       eventID,
		EventType:     models.EventTypeFriendRequestChanged,
		EventVersion:  1,
		OccurredAt:    time.Now().UTC(),
		Producer:      "social_service",
		TraceID:       "not-hex",
		CorrelationID: corrID,
		SchemaRef:     "EVENT_CONTRACTS.md#friend_request_changed-v1",
	}
	badTrace, err := json.Marshal(models.Message{Envelope: envelope, Payload: json.RawMessage(`{}`)})
	require.NoError(t, err)

	goodEnvelope := envelope
	goodEnvelope.TraceID = traceID
	badPayload, err := json.Marshal(models.Message{Envelope: goodEnvelope, Payload: json.RawMessage(`"nope"`)})
	require.NoError(t, err)

	missingKind := goodEnvelope
	missingKind.EventType = models.EventTypeFriendRequestChanged
	emptyRecipient, err := json.Marshal(models.Message{Envelope: missingKind, Payload: mustJSON(t, models.FriendRequestChanged{
		RequestID:    "88888888-8888-4888-8888-888888888888",
		SenderUserID: userAlice,
		Status:       models.FriendRequestPending,
	})})
	require.NoError(t, err)

	tests := []struct {
		name string
		raw  []byte
	}{
		{name: "invalid envelope", raw: badTrace},
		{name: "invalid json", raw: []byte("{")},
		{name: "invalid payload", raw: badPayload},
		{name: "missing required field", raw: emptyRecipient},
		{name: "unregistered event type", raw: buildMessage(t, "unknown_event", "other_service", map[string]string{})},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			harness := newIngestHarness(t)
			require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicSocialEvents, test.raw),
				"malformed events must not be retried")
			require.Equal(t, 1, harness.observer.eventErrs)
			harness.notifications.AssertNotCalled(t, "Insert", mock.Anything, mock.Anything, mock.Anything)
			harness.processed.AssertNotCalled(t, "Record", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

func TestIngestUserAccountChangedAppliesNewestSnapshot(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.processed.On("Record", mock.Anything, eventID, models.TopicIdentityEvents, harness.now).Return(true, nil).Once()
	harness.recipients.On("Upsert", mock.Anything, mock.MatchedBy(func(p models.RecipientProjection) bool {
		return p.UserID == userAlice && p.AccountVersion == 4 && p.EmailVerified && p.Status == models.AccountActive
	}), harness.now).Return(true, nil).Once()

	message := buildMessage(t, models.EventTypeUserAccountChanged, "identity_service", models.UserAccountChanged{
		UserID:         userAlice,
		Change:         models.AccountChangeEmailVerified,
		Email:          "alice@example.com",
		Username:       "alice",
		DisplayName:    "Alice",
		EmailVerified:  true,
		Status:         models.AccountActive,
		AccountVersion: 4,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicIdentityEvents, message))
	harness.notifications.AssertNotCalled(t, "Insert", mock.Anything, mock.Anything, mock.Anything)
}

func TestIngestUserAccountChangedIgnoresStaleSnapshot(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.processed.On("Record", mock.Anything, eventID, models.TopicIdentityEvents, harness.now).Return(true, nil).Once()
	harness.recipients.On("Upsert", mock.Anything, mock.Anything, harness.now).Return(false, nil).Once()

	message := buildMessage(t, models.EventTypeUserAccountChanged, "identity_service", models.UserAccountChanged{
		UserID:         userAlice,
		Change:         models.AccountChangeProfileUpdated,
		Email:          "old@example.com",
		Username:       "alice",
		DisplayName:    "Alice",
		EmailVerified:  true,
		Status:         models.AccountActive,
		AccountVersion: 1,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicIdentityEvents, message))
	harness.notifications.AssertNotCalled(t, "Insert", mock.Anything, mock.Anything, mock.Anything)
}

func TestIngestNoOpEvents(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		topic string
		raw   func(t *testing.T) []byte
	}{
		{
			name:  "message edited",
			topic: models.TopicMessageEvents,
			raw: func(t *testing.T) []byte {
				return buildMessage(t, models.EventTypeMessageChanged, "messaging_service", models.MessageChanged{
					MessageID: "99999999-9999-4999-8999-999999999999", Change: models.MessageChangeEdited,
					ContainerType: models.ContainerTypeChannel, ContainerID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
					AuthorUserID: userAlice, MessageVersion: 2,
				})
			},
		},
		{
			name:  "message created without mentions",
			topic: models.TopicMessageEvents,
			raw: func(t *testing.T) []byte {
				return buildMessage(t, models.EventTypeMessageChanged, "messaging_service", models.MessageChanged{
					MessageID: "99999999-9999-4999-8999-999999999999", Change: models.MessageChangeCreated,
					ContainerType: models.ContainerTypeChannel, ContainerID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
					AuthorUserID: userAlice, MessageVersion: 1,
				})
			},
		},
		{
			name:  "appeal submitted",
			topic: models.TopicModerationEvents,
			raw: func(t *testing.T) []byte {
				return buildMessage(t, models.EventTypeAppealStatusChanged, "moderation_service", models.AppealStatusChanged{
					AppealID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", SanctionID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
					CommunityID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", AppellantUserID: userAlice,
					SanctionType: "mute", Status: models.AppealStatusSubmitted,
				})
			},
		},
		{
			name:  "friend request declined",
			topic: models.TopicSocialEvents,
			raw: func(t *testing.T) []byte {
				return buildMessage(t, models.EventTypeFriendRequestChanged, "social_service", models.FriendRequestChanged{
					RequestID: "88888888-8888-4888-8888-888888888888", SenderUserID: userAlice,
					RecipientUserID: userBob, Status: models.FriendRequestDeclined,
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			harness := newIngestHarness(t)
			harness.processed.On("Record", mock.Anything, eventID, test.topic, harness.now).Return(true, nil).Once()

			require.NoError(t, harness.svc.HandleMessage(context.Background(), test.topic, test.raw(t)))
			harness.notifications.AssertNotCalled(t, "Insert", mock.Anything, mock.Anything, mock.Anything)
			harness.processed.AssertNumberOfCalls(t, "Record", 1)
		})
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return encoded
}

func TestIngestTransactionErrorIsRetried(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	transactor := &mocks.Transactor{Err: errors.New("postgres is down")}
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
	harness.recipients.On("Get", mock.Anything, mock.Anything).Return(activeProjection(userAlice), nil).Maybe()

	message := buildMessage(t, models.EventTypeFriendRequestChanged, "social_service", models.FriendRequestChanged{
		RequestID: "88888888-8888-4888-8888-888888888888", SenderUserID: userAlice,
		RecipientUserID: userBob, Status: models.FriendRequestPending,
	})
	require.Error(t, harness.svc.HandleMessage(context.Background(), models.TopicSocialEvents, message),
		"a failed transaction must keep the offset uncommitted")
}

func TestIngestMentionThrottlesSecondEmailInWindow(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.processed.On("Record", mock.Anything, eventID, models.TopicMessageEvents, harness.now).Return(true, nil).Once()
	harness.preferences.On("Get", mock.Anything, userBob).Return(models.DefaultPreferences(), nil).Once()
	harness.recipients.On("Get", mock.Anything, mock.Anything).Return(activeProjection(userAlice), nil).Maybe()
	harness.recipients.On("Get", mock.Anything, userBob).Return(activeProjection(userBob), nil).Maybe()
	harness.windows.On("TryClaim", mock.Anything, userBob, harness.now, 15*time.Minute).Return(false, nil).Once()
	harness.expectInsert(t, models.KindMention, userBob, models.DeliveryThrottled)
	harness.outbox.On("Enqueue", mock.Anything, notifID, harness.now).Return(outboxID, nil).Once()
	harness.publisher.On("Publish", mock.Anything, outboxID, mock.Anything).Return(nil).Once()

	message := buildMessage(t, models.EventTypeMessageChanged, "messaging_service", models.MessageChanged{
		MessageID: "99999999-9999-4999-8999-999999999999", Change: models.MessageChangeCreated,
		ContainerType: models.ContainerTypeChannel, ContainerID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		AuthorUserID: userAlice, MentionedUserIDs: []string{userBob}, MessageVersion: 1,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicMessageEvents, message))

	harness.sender.AssertNotCalled(t, "Send", mock.Anything, mock.Anything)
	require.Equal(t, 1, harness.observer.throttled[models.KindMention])
	require.Equal(t, 1, harness.observer.emails["mention/throttled"])
}

func TestIngestSkipsEmailWhenPreferenceDisabled(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	prefs := models.DefaultPreferences()
	prefs.Mention.Email = false

	harness.processed.On("Record", mock.Anything, eventID, models.TopicMessageEvents, harness.now).Return(true, nil).Once()
	harness.preferences.On("Get", mock.Anything, userBob).Return(prefs, nil).Once()
	harness.recipients.On("Get", mock.Anything, mock.Anything).Return(activeProjection(userAlice), nil).Maybe()
	harness.expectInsert(t, models.KindMention, userBob, models.DeliverySkippedPreference)
	harness.outbox.On("Enqueue", mock.Anything, notifID, harness.now).Return(outboxID, nil).Once()
	harness.publisher.On("Publish", mock.Anything, outboxID, mock.Anything).Return(nil).Once()

	message := buildMessage(t, models.EventTypeMessageChanged, "messaging_service", models.MessageChanged{
		MessageID: "99999999-9999-4999-8999-999999999999", Change: models.MessageChangeCreated,
		ContainerType: models.ContainerTypeChannel, ContainerID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		AuthorUserID: userAlice, MentionedUserIDs: []string{userBob}, MessageVersion: 1,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicMessageEvents, message))

	harness.sender.AssertNotCalled(t, "Send", mock.Anything, mock.Anything)
	harness.windows.AssertNotCalled(t, "TryClaim", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	require.Equal(t, 1, harness.observer.emails["mention/skipped_preference"])
}

func TestIngestEmailEligibilitySkips(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		projection func() (models.RecipientProjection, error)
		wantStatus models.DeliveryStatus
	}{
		{
			name: "unverified",
			projection: func() (models.RecipientProjection, error) {
				projection := activeProjection(userBob)
				projection.EmailVerified = false
				return projection, nil
			},
			wantStatus: models.DeliverySkippedUnverified,
		},
		{
			name: "deactivated",
			projection: func() (models.RecipientProjection, error) {
				projection := activeProjection(userBob)
				projection.Status = models.AccountDeactivated
				return projection, nil
			},
			wantStatus: models.DeliverySkippedDeactivated,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			harness := newIngestHarness(t)
			harness.processed.On("Record", mock.Anything, eventID, models.TopicSocialEvents, harness.now).Return(true, nil).Once()
			harness.preferences.On("Get", mock.Anything, userBob).Return(models.DefaultPreferences(), nil).Once()
			harness.recipients.On("Get", mock.Anything, userAlice).Return(activeProjection(userAlice), nil).Maybe()
			first, firstErr := test.projection()
			harness.recipients.On("Get", mock.Anything, userBob).Return(first, firstErr).Maybe()
			harness.expectInsert(t, models.KindFriendRequestReceived, userBob, test.wantStatus)
			harness.outbox.On("Enqueue", mock.Anything, notifID, harness.now).Return(outboxID, nil).Once()
			harness.publisher.On("Publish", mock.Anything, outboxID, mock.Anything).Return(nil).Once()

			message := buildMessage(t, models.EventTypeFriendRequestChanged, "social_service", models.FriendRequestChanged{
				RequestID: "88888888-8888-4888-8888-888888888888", SenderUserID: userAlice,
				RecipientUserID: userBob, Status: models.FriendRequestPending,
			})
			require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicSocialEvents, message))
			harness.sender.AssertNotCalled(t, "Send", mock.Anything, mock.Anything)
			require.Equal(t, 1, harness.observer.emails["friend_request_received/"+string(test.wantStatus)])
		})
	}
}

func TestIngestPublishFailureDoesNotLoseNotification(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.processed.On("Record", mock.Anything, eventID, models.TopicModerationEvents, harness.now).Return(true, nil).Once()
	harness.preferences.On("Get", mock.Anything, userAlice).Return(models.DefaultPreferences(), nil).Once()
	harness.recipients.On("Get", mock.Anything, userAlice).Return(activeProjection(userAlice), nil).Maybe()
	harness.expectInsert(t, models.KindModerationNotice, userAlice, models.DeliveryPending)
	harness.outbox.On("Enqueue", mock.Anything, notifID, harness.now).Return(outboxID, nil).Once()
	harness.sender.On("Send", mock.Anything, mock.Anything).Return("resend-1", nil).Once()
	harness.deliveries.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	harness.publisher.On("Publish", mock.Anything, outboxID, mock.Anything).Return(errors.New("broker unavailable")).Once()

	message := buildMessage(t, models.EventTypeMemberSanctionChanged, "moderation_service", models.MemberSanctionChanged{
		SanctionID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", CommunityID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		TargetUserID: userAlice, Action: models.SanctionActionMute, Source: models.SanctionSourceModerator, Appealable: true,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicModerationEvents, message),
		"a publish failure is retried by the outbox and must not fail the event")
	harness.notifications.AssertNumberOfCalls(t, "Insert", 1)
}

func TestIngestInAppToggleSuppressesOnlyThePush(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	prefs := models.DefaultPreferences()
	prefs.Mention.InApp = false

	harness.processed.On("Record", mock.Anything, eventID, models.TopicMessageEvents, harness.now).Return(true, nil).Once()
	harness.preferences.On("Get", mock.Anything, userBob).Return(prefs, nil).Once()
	harness.recipients.On("Get", mock.Anything, mock.Anything).Return(activeProjection(userAlice), nil).Maybe()
	harness.recipients.On("Get", mock.Anything, userBob).Return(activeProjection(userBob), nil).Maybe()
	harness.windows.On("TryClaim", mock.Anything, userBob, harness.now, 15*time.Minute).Return(true, nil).Once()
	harness.expectInsert(t, models.KindMention, userBob, models.DeliveryPending)
	harness.sender.On("Send", mock.Anything, mock.Anything).Return("resend-1", nil).Once()
	harness.deliveries.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	message := buildMessage(t, models.EventTypeMessageChanged, "messaging_service", models.MessageChanged{
		MessageID: "99999999-9999-4999-8999-999999999999", Change: models.MessageChangeCreated,
		ContainerType: models.ContainerTypeChannel, ContainerID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		AuthorUserID: userAlice, MentionedUserIDs: []string{userBob}, MessageVersion: 1,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicMessageEvents, message))

	harness.outbox.AssertNotCalled(t, "Enqueue", mock.Anything, mock.Anything, mock.Anything)
	harness.publisher.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything)
	harness.notifications.AssertNumberOfCalls(t, "Insert", 1)
}

func TestIngestPreferenceLookupFailureRetries(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)
	harness.processed.On("Record", mock.Anything, eventID, models.TopicSocialEvents, harness.now).Return(true, nil).Once()
	harness.preferences.On("Get", mock.Anything, userBob).Return(models.Preferences{}, errors.New("db down")).Once()
	harness.recipients.On("Get", mock.Anything, mock.Anything).Return(activeProjection(userAlice), nil).Maybe()

	message := buildMessage(t, models.EventTypeFriendRequestChanged, "social_service", models.FriendRequestChanged{
		RequestID: "88888888-8888-4888-8888-888888888888", SenderUserID: userAlice,
		RecipientUserID: userBob, Status: models.FriendRequestPending,
	})
	require.Error(t, harness.svc.HandleMessage(context.Background(), models.TopicSocialEvents, message))
}

func TestIngestPreferencesDefaultsWhenMissing(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)
	harness.processed.On("Record", mock.Anything, eventID, models.TopicSocialEvents, harness.now).Return(true, nil).Once()
	harness.preferences.On("Get", mock.Anything, userAlice).Return(models.Preferences{}, repository.ErrNotFound).Once()
	harness.recipients.On("Get", mock.Anything, userAlice).Return(activeProjection(userAlice), nil).Maybe()
	harness.recipients.On("Get", mock.Anything, userBob).Return(activeProjection(userBob), nil).Maybe()
	harness.expectInsert(t, models.KindFriendRequestAccepted, userAlice, models.DeliveryPending)
	harness.outbox.On("Enqueue", mock.Anything, notifID, harness.now).Return(outboxID, nil).Once()
	harness.sender.On("Send", mock.Anything, mock.Anything).Return("resend-1", nil).Once()
	harness.deliveries.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	harness.publisher.On("Publish", mock.Anything, outboxID, mock.Anything).Return(nil).Once()

	message := buildMessage(t, models.EventTypeFriendRequestChanged, "social_service", models.FriendRequestChanged{
		RequestID: "88888888-8888-4888-8888-888888888888", SenderUserID: userAlice,
		RecipientUserID: userBob, Status: models.FriendRequestAccepted,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicSocialEvents, message))
	harness.notifications.AssertNumberOfCalls(t, "Insert", 1)
}

func TestIngestUnknownProjectionSchedulesRetry(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.processed.On("Record", mock.Anything, eventID, models.TopicSocialEvents, harness.now).Return(true, nil).Once()
	harness.preferences.On("Get", mock.Anything, userBob).Return(models.DefaultPreferences(), nil).Once()
	harness.recipients.On("Get", mock.Anything, userAlice).Return(activeProjection(userAlice), nil).Maybe()
	harness.recipients.On("Get", mock.Anything, userBob).Return(models.RecipientProjection{}, repository.ErrNotFound).Maybe()
	harness.expectInsert(t, models.KindFriendRequestReceived, userBob, models.DeliveryPending)
	harness.outbox.On("Enqueue", mock.Anything, notifID, harness.now).Return(outboxID, nil).Once()
	harness.publisher.On("Publish", mock.Anything, outboxID, mock.Anything).Return(nil).Once()
	harness.deliveries.On("Update", mock.Anything, mock.MatchedBy(func(d models.EmailDelivery) bool {
		return d.Status == models.DeliveryPending && d.Attempts == 1 && d.NextAttemptAt != nil
	}), mock.Anything).Return(nil).Once()

	message := buildMessage(t, models.EventTypeFriendRequestChanged, "social_service", models.FriendRequestChanged{
		RequestID: "88888888-8888-4888-8888-888888888888", SenderUserID: userAlice,
		RecipientUserID: userBob, Status: models.FriendRequestPending,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicSocialEvents, message))
	harness.sender.AssertNotCalled(t, "Send", mock.Anything, mock.Anything)
}

func TestIngestSenderFailureIsRecordedAsFailed(t *testing.T) {
	t.Parallel()
	harness := newIngestHarness(t)

	harness.processed.On("Record", mock.Anything, eventID, models.TopicSocialEvents, harness.now).Return(true, nil).Once()
	harness.preferences.On("Get", mock.Anything, userBob).Return(models.DefaultPreferences(), nil).Once()
	harness.recipients.On("Get", mock.Anything, userAlice).Return(activeProjection(userAlice), nil).Maybe()
	harness.recipients.On("Get", mock.Anything, userBob).Return(activeProjection(userBob), nil).Maybe()
	harness.expectInsert(t, models.KindFriendRequestReceived, userBob, models.DeliveryPending)
	harness.outbox.On("Enqueue", mock.Anything, notifID, harness.now).Return(outboxID, nil).Once()
	harness.publisher.On("Publish", mock.Anything, outboxID, mock.Anything).Return(nil).Once()
	harness.sender.On("Send", mock.Anything, mock.Anything).Return("", &provider.SendError{Kind: provider.FailurePermanent, StatusCode: 422, Err: errors.New("invalid recipient")}).Once()
	harness.deliveries.On("Update", mock.Anything, mock.MatchedBy(func(d models.EmailDelivery) bool {
		return d.Status == models.DeliveryFailed && d.Attempts == 1 && d.LastError != nil
	}), mock.Anything).Return(nil).Once()

	message := buildMessage(t, models.EventTypeFriendRequestChanged, "social_service", models.FriendRequestChanged{
		RequestID: "88888888-8888-4888-8888-888888888888", SenderUserID: userAlice,
		RecipientUserID: userBob, Status: models.FriendRequestPending,
	})
	require.NoError(t, harness.svc.HandleMessage(context.Background(), models.TopicSocialEvents, message))
	require.Equal(t, 1, harness.observer.emails["friend_request_received/failed"])
}

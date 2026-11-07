package event_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"notification_service/internal/event"
	"notification_service/internal/models"
	"notification_service/tests/mocks"
)

// recordedPublish is one captured producer call.
type recordedPublish struct {
	topic   string
	key     string
	value   []byte
	headers map[string]string
}

// recordingProducer captures published messages.
type recordingProducer struct {
	mu      sync.Mutex
	publish []recordedPublish
	err     error
	closed  bool
}

func (p *recordingProducer) Publish(_ context.Context, topic string, key, value []byte, headers map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.publish = append(p.publish, recordedPublish{topic: topic, key: string(key), value: value, headers: headers})
	return nil
}

func (p *recordingProducer) Close() error {
	p.closed = true
	return nil
}

func (p *recordingProducer) last() recordedPublish {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.publish[len(p.publish)-1]
}

const (
	publishNow          = "2026-02-01T10:00:00Z"
	publishNotification = "66666666-6666-4666-8666-666666666666"
	publishRecipient    = "22222222-2222-4222-8222-222222222222"
	publishOutbox       = "77777777-7777-4777-8777-777777777777"
	publishCauseEvent   = "55555555-5555-4555-8555-555555555555"
	publishTraceID      = "4bf92f3577b34da6a3ce929d0e0e4736"
	publishCorrelation  = "44444444-4444-4444-8444-444444444444"
)

func publisherHarness(t *testing.T) (*event.OutboxPublisher, *mocks.OutboxRepository, *mocks.NotificationRepository, *recordingProducer, time.Time) {
	t.Helper()
	now, err := time.Parse(time.RFC3339, publishNow)
	require.NoError(t, err)

	outbox := &mocks.OutboxRepository{}
	notifications := &mocks.NotificationRepository{}
	producer := &recordingProducer{}

	publisher := event.NewOutboxPublisher(event.OutboxPublisherOptions{
		Outbox:        outbox,
		Notifications: notifications,
		Producer:      producer,
		Topic:         models.TopicNotificationEvents,
		Batch:         10,
		Logger:        zap.NewNop(),
		Clock:         func() time.Time { return now },
		NewID:         func() string { return publishCauseEvent },
	})
	return publisher, outbox, notifications, producer, now
}

func pendingEntry() models.OutboxEntry {
	createdAt, _ := time.Parse(time.RFC3339, publishNow)
	return models.OutboxEntry{ID: publishOutbox, NotificationID: publishNotification, CreatedAt: createdAt}
}

func storedNotification() models.Notification {
	createdAt, _ := time.Parse(time.RFC3339, publishNow)
	return models.Notification{
		ID:              publishNotification,
		RecipientUserID: publishRecipient,
		Kind:            models.KindMention,
		Title:           "Alice mentioned you",
		Summary:         "in a channel",
		Link:            "/channels/c1/messages/m1",
		CreatedAt:       createdAt,
	}
}

func TestOutboxPublishCopiesCausingEnvelopeAndRecomputesUnreadCount(t *testing.T) {
	t.Parallel()
	publisher, outbox, notifications, producer, now := publisherHarness(t)

	cause := models.Envelope{
		EventID:       publishCauseEvent,
		EventType:     models.EventTypeMessageChanged,
		EventVersion:  1,
		OccurredAt:    now.Add(-time.Second),
		Producer:      "messaging_service",
		TraceID:       publishTraceID,
		CorrelationID: publishCorrelation,
		SchemaRef:     "EVENT_CONTRACTS.md#message_changed-v1",
	}
	outbox.On("ListPending", mock.Anything, 10).Return([]models.OutboxEntry{pendingEntry()}, nil).Once()
	notifications.On("GetByID", mock.Anything, publishNotification).Return(storedNotification(), nil).Once()
	notifications.On("UnreadCount", mock.Anything, publishRecipient).Return(5, nil).Once()
	outbox.On("MarkPublished", mock.Anything, publishOutbox, now).Return(nil).Once()

	require.NoError(t, publisher.Publish(context.Background(), publishOutbox, cause))

	published := producer.last()
	require.Equal(t, models.TopicNotificationEvents, published.topic)
	require.Equal(t, publishRecipient, published.key, "EVT-011 is keyed by recipient_user_id")
	require.Equal(t, publishTraceID, published.headers[models.HeaderTraceID])
	require.Equal(t, models.EventTypeNotificationCreated, published.headers[models.HeaderEventType])

	message, err := models.DecodeMessage(published.value)
	require.NoError(t, err)
	require.NoError(t, models.ValidateEnvelope(message.Envelope))
	require.Equal(t, publishTraceID, message.Envelope.TraceID, "trace_id is copied from the causing event")
	require.Equal(t, publishCorrelation, message.Envelope.CorrelationID, "correlation_id is copied from the causing event")
	require.NotNil(t, message.Envelope.CausationID)
	require.Equal(t, publishCauseEvent, *message.Envelope.CausationID)
	require.Equal(t, models.SchemaRefNotificationCreated, message.Envelope.SchemaRef)
	require.Equal(t, models.ProducerNotificationService, message.Envelope.Producer)

	var payload models.NotificationCreated
	require.NoError(t, json.Unmarshal(message.Payload, &payload))
	require.Equal(t, publishNotification, payload.NotificationID)
	require.Equal(t, publishRecipient, payload.RecipientUserID)
	require.Equal(t, models.KindMention, payload.Kind)
	require.Equal(t, 5, payload.UnreadCount, "unread_count is recomputed from committed state")
}

func TestOutboxPublishRecoversWithoutTheCausingEnvelope(t *testing.T) {
	t.Parallel()
	publisher, outbox, notifications, producer, _ := publisherHarness(t)

	outbox.On("ListPending", mock.Anything, 10).Return([]models.OutboxEntry{pendingEntry()}, nil).Once()
	notifications.On("GetByID", mock.Anything, publishNotification).Return(storedNotification(), nil).Once()
	notifications.On("UnreadCount", mock.Anything, publishRecipient).Return(1, nil).Once()
	outbox.On("MarkPublished", mock.Anything, publishOutbox, mock.Anything).Return(nil).Once()

	require.NoError(t, publisher.Drain(context.Background()))

	message, err := models.DecodeMessage(producer.last().value)
	require.NoError(t, err)
	require.NoError(t, models.ValidateEnvelope(message.Envelope), "the recovery envelope is still schema-valid")
	require.Len(t, message.Envelope.TraceID, 32)
	require.Equal(t, publishNotification, message.Envelope.CorrelationID)
	require.NotNil(t, message.Envelope.CausationID)
}

func TestOutboxPublishFailuresKeepTheRowPending(t *testing.T) {
	t.Parallel()
	now, err := time.Parse(time.RFC3339, publishNow)
	require.NoError(t, err)

	tests := []struct {
		name  string
		setup func(outbox *mocks.OutboxRepository, notifications *mocks.NotificationRepository, producer *recordingProducer)
	}{
		{
			name: "list pending fails",
			setup: func(outbox *mocks.OutboxRepository, _ *mocks.NotificationRepository, _ *recordingProducer) {
				outbox.On("ListPending", mock.Anything, 10).Return(([]models.OutboxEntry)(nil), errors.New("db down")).Once()
			},
		},
		{
			name: "notification load fails",
			setup: func(outbox *mocks.OutboxRepository, notifications *mocks.NotificationRepository, _ *recordingProducer) {
				outbox.On("ListPending", mock.Anything, 10).Return([]models.OutboxEntry{pendingEntry()}, nil).Once()
				notifications.On("GetByID", mock.Anything, publishNotification).Return(models.Notification{}, errors.New("db down")).Once()
			},
		},
		{
			name: "unread count fails",
			setup: func(outbox *mocks.OutboxRepository, notifications *mocks.NotificationRepository, _ *recordingProducer) {
				outbox.On("ListPending", mock.Anything, 10).Return([]models.OutboxEntry{pendingEntry()}, nil).Once()
				notifications.On("GetByID", mock.Anything, publishNotification).Return(storedNotification(), nil).Once()
				notifications.On("UnreadCount", mock.Anything, publishRecipient).Return(0, errors.New("db down")).Once()
			},
		},
		{
			name: "broker rejects the publish",
			setup: func(outbox *mocks.OutboxRepository, notifications *mocks.NotificationRepository, producer *recordingProducer) {
				outbox.On("ListPending", mock.Anything, 10).Return([]models.OutboxEntry{pendingEntry()}, nil).Once()
				notifications.On("GetByID", mock.Anything, publishNotification).Return(storedNotification(), nil).Once()
				notifications.On("UnreadCount", mock.Anything, publishRecipient).Return(2, nil).Once()
				producer.err = errors.New("broker unavailable")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			outbox := &mocks.OutboxRepository{}
			notifications := &mocks.NotificationRepository{}
			producer := &recordingProducer{}
			publisher := event.NewOutboxPublisher(event.OutboxPublisherOptions{
				Outbox: outbox, Notifications: notifications, Producer: producer,
				Topic: models.TopicNotificationEvents, Batch: 10, Logger: zap.NewNop(),
				Clock: func() time.Time { return now }, NewID: func() string { return publishCauseEvent },
			})
			test.setup(outbox, notifications, producer)

			require.Error(t, publisher.Drain(context.Background()))
			outbox.AssertNotCalled(t, "MarkPublished", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

func TestOutboxDrainEmpty(t *testing.T) {
	t.Parallel()
	publisher, outbox, _, producer, _ := publisherHarness(t)
	outbox.On("ListPending", mock.Anything, 10).Return([]models.OutboxEntry{}, nil).Once()

	require.NoError(t, publisher.Drain(context.Background()))
	require.Empty(t, producer.publish)
}

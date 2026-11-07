package event

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"notification_service/internal/models"
	"notification_service/internal/repository"
)

// DefaultPublishBatch bounds one outbox drain.
const DefaultPublishBatch = 100

// OutboxPublisher drains the transactional outbox and publishes EVT-011
// `notification_created` messages (ADR-001).
type OutboxPublisher struct {
	outbox        repository.OutboxRepository
	notifications repository.NotificationRepository
	producer      EventProducer
	topic         string
	batch         int
	logger        *zap.Logger
	clock         func() time.Time
	newID         func() string

	mu     sync.Mutex
	causes map[string]models.Envelope
}

// OutboxPublisherOptions are the dependencies of the outbox publisher.
type OutboxPublisherOptions struct {
	Outbox        repository.OutboxRepository
	Notifications repository.NotificationRepository
	Producer      EventProducer
	Topic         string
	Batch         int
	Logger        *zap.Logger
	Clock         func() time.Time
	NewID         func() string
}

// NewOutboxPublisher builds an OutboxPublisher.
func NewOutboxPublisher(opts OutboxPublisherOptions) *OutboxPublisher {
	batch := opts.Batch
	if batch <= 0 {
		batch = DefaultPublishBatch
	}
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	clock := opts.Clock
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	newID := opts.NewID
	if newID == nil {
		newID = models.NewUUID
	}
	return &OutboxPublisher{
		outbox:        opts.Outbox,
		notifications: opts.Notifications,
		producer:      opts.Producer,
		topic:         opts.Topic,
		batch:         batch,
		logger:        logger,
		clock:         clock,
		newID:         newID,
		causes:        make(map[string]models.Envelope),
	}
}

// Publish registers the causing envelope of a freshly committed outbox row and
// drains pending rows. The publish is retried from the persisted outbox state on
// failure.
func (p *OutboxPublisher) Publish(ctx context.Context, outboxID string, cause models.Envelope) error {
	p.register(outboxID, cause)
	return p.Drain(ctx)
}

// Drain publishes every pending outbox row, oldest first. It stops at the first
// failure so the remaining rows are retried by the next sweep.
func (p *OutboxPublisher) Drain(ctx context.Context) error {
	entries, err := p.outbox.ListPending(ctx, p.batch)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := p.publishEntry(ctx, entry); err != nil {
			return err
		}
	}
	return nil
}

// publishEntry publishes one outbox row with the notification snapshot and the
// recipient's unread count recomputed from committed state.
func (p *OutboxPublisher) publishEntry(ctx context.Context, entry models.OutboxEntry) error {
	notification, err := p.notifications.GetByID(ctx, entry.NotificationID)
	if err != nil {
		return fmt.Errorf("event: load notification %s for outbox row %s: %w", entry.NotificationID, entry.ID, err)
	}
	unreadCount, err := p.notifications.UnreadCount(ctx, notification.RecipientUserID)
	if err != nil {
		return fmt.Errorf("event: count unread notifications of %s: %w", notification.RecipientUserID, err)
	}

	now := p.clock()
	envelope := p.envelopeFor(entry, notification, now)
	payload := models.NotificationCreated{
		NotificationID:  notification.ID,
		RecipientUserID: notification.RecipientUserID,
		Kind:            notification.Kind,
		Title:           notification.Title,
		Summary:         notification.Summary,
		Link:            notification.Link,
		UnreadCount:     unreadCount,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("event: encode notification_created payload: %w", err)
	}
	encoded, err := json.Marshal(models.Message{Envelope: envelope, Payload: payloadBytes})
	if err != nil {
		return fmt.Errorf("event: encode notification_created: %w", err)
	}
	headers := map[string]string{
		models.HeaderTraceID:   envelope.TraceID,
		models.HeaderEventType: envelope.EventType,
	}
	if err := p.producer.Publish(ctx, p.topic, []byte(notification.RecipientUserID), encoded, headers); err != nil {
		return err
	}
	if err := p.outbox.MarkPublished(ctx, entry.ID, now); err != nil {
		return err
	}
	p.forget(entry.ID)
	p.logger.Info("notification_created published",
		zap.String("service_name", "notification_service"),
		zap.String("operation", "publish_notification_created"),
		zap.String("trace_id", envelope.TraceID),
		zap.String("correlation_id", envelope.CorrelationID),
		zap.String("event_id", envelope.EventID),
		zap.String("notification_id", notification.ID),
		zap.Int("unread_count", unreadCount),
	)
	return nil
}

// envelopeFor derives the EVT-011 envelope. The causing envelope supplies
// trace_id, correlation_id and causation_id; orphaned rows (crash between commit
// and publish) fall back to a recovery envelope derived from the notification.
func (p *OutboxPublisher) envelopeFor(entry models.OutboxEntry, notification models.Notification, now time.Time) models.Envelope {
	cause, known := p.lookup(entry.ID)
	if known {
		causationID := cause.EventID
		return models.Envelope{
			EventID:       p.newID(),
			EventType:     models.EventTypeNotificationCreated,
			EventVersion:  1,
			OccurredAt:    now.UTC(),
			Producer:      models.ProducerNotificationService,
			TraceID:       cause.TraceID,
			CorrelationID: cause.CorrelationID,
			CausationID:   &causationID,
			SchemaRef:     models.SchemaRefNotificationCreated,
		}
	}

	causationID := notification.ID
	p.logger.Warn("publishing notification_created without the causing envelope; deriving a recovery envelope",
		zap.String("service_name", "notification_service"),
		zap.String("operation", "publish_notification_created"),
		zap.String("notification_id", notification.ID),
		zap.String("outbox_id", entry.ID),
	)
	return models.Envelope{
		EventID:       p.newID(),
		EventType:     models.EventTypeNotificationCreated,
		EventVersion:  1,
		OccurredAt:    now.UTC(),
		Producer:      models.ProducerNotificationService,
		TraceID:       models.NewTraceID(),
		CorrelationID: notification.ID,
		CausationID:   &causationID,
		SchemaRef:     models.SchemaRefNotificationCreated,
	}
}

func (p *OutboxPublisher) register(outboxID string, cause models.Envelope) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.causes[outboxID] = cause
}

func (p *OutboxPublisher) lookup(outboxID string) (models.Envelope, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cause, ok := p.causes[outboxID]
	return cause, ok
}

func (p *OutboxPublisher) forget(outboxID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.causes, outboxID)
}

var _ interface {
	Publish(ctx context.Context, outboxID string, cause models.Envelope) error
	Drain(ctx context.Context) error
} = (*OutboxPublisher)(nil)

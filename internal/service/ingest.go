package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"notification_service/internal/models"
	"notification_service/internal/provider"
	"notification_service/internal/repository"
)

// Publisher publishes EVT-011 from the transactional outbox (ADR-001).
type Publisher interface {
	// Publish marks the causing envelope of a freshly committed outbox row and
	// drains every pending row.
	Publish(ctx context.Context, outboxID string, cause models.Envelope) error
	// Drain publishes pending rows whose causing envelope is unknown (crash
	// recovery and background sweeps).
	Drain(ctx context.Context) error
}

// IngestDeps are the dependencies of the event ingest service.
type IngestDeps struct {
	Transactor    repository.Transactor
	Notifications repository.NotificationRepository
	Preferences   repository.PreferenceRepository
	Recipients    repository.RecipientRepository
	Deliveries    repository.EmailDeliveryRepository
	Windows       repository.MentionWindowRepository
	Processed     repository.ProcessedEventRepository
	Outbox        repository.OutboxRepository
	Publisher     Publisher
	Sender        provider.EmailSender
	Observer      Observer
	Logger        *zap.Logger
	Clock         Clock
	NewID         IDGenerator
	// MailFrom is the verified sender address of notification emails.
	MailFrom string
	// MentionThrottleWindow is the per-recipient mention email window.
	MentionThrottleWindow time.Duration
	// MaxEmailAttempts bounds the email retry budget.
	MaxEmailAttempts int
	// Backoff returns the retry delay after the given attempt number.
	Backoff func(attempt int) time.Duration
}

// IngestService turns consumed domain events into notifications, email
// decisions and EVT-011 outbox rows.
type IngestService struct {
	transactor       repository.Transactor
	notifications    repository.NotificationRepository
	preferences      repository.PreferenceRepository
	recipients       repository.RecipientRepository
	deliveries       repository.EmailDeliveryRepository
	windows          repository.MentionWindowRepository
	processed        repository.ProcessedEventRepository
	outbox           repository.OutboxRepository
	publisher        Publisher
	sender           provider.EmailSender
	observer         Observer
	logger           *zap.Logger
	clock            Clock
	newID            IDGenerator
	mailFrom         string
	throttleWindow   time.Duration
	maxEmailAttempts int
	backoff          func(int) time.Duration
}

// NewIngestService builds an IngestService, applying the documented defaults
// for optional dependencies.
func NewIngestService(deps IngestDeps) *IngestService {
	svc := &IngestService{
		transactor:       deps.Transactor,
		notifications:    deps.Notifications,
		preferences:      deps.Preferences,
		recipients:       deps.Recipients,
		deliveries:       deps.Deliveries,
		windows:          deps.Windows,
		processed:        deps.Processed,
		outbox:           deps.Outbox,
		publisher:        deps.Publisher,
		sender:           deps.Sender,
		observer:         deps.Observer,
		logger:           deps.Logger,
		clock:            deps.Clock,
		newID:            deps.NewID,
		mailFrom:         deps.MailFrom,
		throttleWindow:   deps.MentionThrottleWindow,
		maxEmailAttempts: deps.MaxEmailAttempts,
		backoff:          deps.Backoff,
	}
	if svc.observer == nil {
		svc.observer = NopObserver{}
	}
	if svc.logger == nil {
		svc.logger = zap.NewNop()
	}
	if svc.clock == nil {
		svc.clock = SystemClock
	}
	if svc.newID == nil {
		svc.newID = models.NewUUID
	}
	if svc.throttleWindow <= 0 {
		svc.throttleWindow = 15 * time.Minute
	}
	if svc.maxEmailAttempts <= 0 {
		svc.maxEmailAttempts = DefaultMaxEmailAttempts
	}
	if svc.backoff == nil {
		svc.backoff = DefaultRetryBackoff
	}
	return svc
}

// persistedNotification is a notification committed together with its email
// decision and (optional) outbox row.
type persistedNotification struct {
	notification models.Notification
	delivery     models.EmailDelivery
	outboxID     string
}

// HandleMessage processes one raw Kafka message. A nil return means the message
// was handled (or dropped as malformed) and its offset may be committed; a
// non-nil return means the attempt should be retried.
func (s *IngestService) HandleMessage(ctx context.Context, topic string, raw []byte) error {
	message, err := models.DecodeMessage(raw)
	if err != nil {
		s.dropMalformed(topic, raw, "malformed_event", err)
		return nil
	}

	logger := s.logger.With(
		zap.String("service_name", "notification_service"),
		zap.String("trace_id", message.Envelope.TraceID),
		zap.String("correlation_id", message.Envelope.CorrelationID),
		zap.String("event_id", message.Envelope.EventID),
		zap.String("event_type", message.Envelope.EventType),
		zap.String("topic", topic),
		zap.String("operation", "consume_event"),
	)

	switch message.Envelope.EventType {
	case models.EventTypeUserAccountChanged:
		return s.handleUserAccountChanged(ctx, logger, topic, message)
	case models.EventTypeMemberSanctionChanged:
		return s.handleMemberSanctionChanged(ctx, logger, topic, message)
	case models.EventTypeAppealStatusChanged:
		return s.handleAppealStatusChanged(ctx, logger, topic, message)
	case models.EventTypeFriendRequestChanged:
		return s.handleFriendRequestChanged(ctx, logger, topic, message)
	case models.EventTypeMessageChanged:
		return s.handleMessageChanged(ctx, logger, topic, message)
	default:
		s.observer.EventError(topic, "unregistered_event_type")
		logger.Warn("unregistered event type dropped", zap.String("error_class", "unregistered_event_type"))
		return nil
	}
}

// handleUserAccountChanged applies the EVT-001 recipient projection snapshot.
func (s *IngestService) handleUserAccountChanged(ctx context.Context, logger *zap.Logger, topic string, message models.Message) error {
	var payload models.UserAccountChanged
	if !s.decodePayload(topic, message, &payload) {
		return nil
	}
	if !models.IsUUID(payload.UserID) || payload.AccountVersion < 1 ||
		(payload.Status != models.AccountActive && payload.Status != models.AccountDeactivated) {
		s.dropMalformed(topic, message.Envelope, "invalid_payload", errors.New("user_account_changed payload is incomplete"))
		return nil
	}

	now := s.clock()
	projection := models.RecipientProjection{
		UserID:         payload.UserID,
		Email:          payload.Email,
		Username:       payload.Username,
		DisplayName:    payload.DisplayName,
		EmailVerified:  payload.EmailVerified,
		Status:         payload.Status,
		AccountVersion: payload.AccountVersion,
	}

	return s.transactor.InTx(ctx, func(txCtx context.Context) error {
		fresh, err := s.processed.Record(txCtx, message.Envelope.EventID, topic, now)
		if err != nil || !fresh {
			return err
		}
		applied, err := s.recipients.Upsert(txCtx, projection, now)
		if err != nil {
			return err
		}
		if !applied {
			logger.Debug("stale account snapshot ignored",
				zap.Int("account_version", projection.AccountVersion),
				zap.String("user_id", projection.UserID),
			)
		}
		return nil
	})
}

// handleMemberSanctionChanged derives the EVT-004 moderation notice.
func (s *IngestService) handleMemberSanctionChanged(ctx context.Context, logger *zap.Logger, topic string, message models.Message) error {
	var payload models.MemberSanctionChanged
	if !s.decodePayload(topic, message, &payload) {
		return nil
	}
	if !models.IsUUID(payload.TargetUserID) || !validSanctionAction(payload.Action) || !validSanctionSource(payload.Source) {
		s.dropMalformed(topic, message.Envelope, "invalid_payload", errors.New("member_sanction_changed payload is incomplete"))
		return nil
	}
	return s.ingestNotifications(ctx, logger, topic, message.Envelope, mapMemberSanctionChanged(payload))
}

// handleAppealStatusChanged derives the EVT-006 appeal decision notice.
func (s *IngestService) handleAppealStatusChanged(ctx context.Context, logger *zap.Logger, topic string, message models.Message) error {
	var payload models.AppealStatusChanged
	if !s.decodePayload(topic, message, &payload) {
		return nil
	}
	switch payload.Status {
	case models.AppealStatusSubmitted:
		return s.recordNoop(ctx, topic, message.Envelope)
	case models.AppealStatusUpheld, models.AppealStatusOverturned:
		if !models.IsUUID(payload.AppellantUserID) {
			s.dropMalformed(topic, message.Envelope, "invalid_payload", errors.New("appeal_status_changed payload is incomplete"))
			return nil
		}
		return s.ingestNotifications(ctx, logger, topic, message.Envelope, mapAppealStatusChanged(payload))
	default:
		s.dropMalformed(topic, message.Envelope, "invalid_payload", fmt.Errorf("unknown appeal status %q", payload.Status))
		return nil
	}
}

// handleFriendRequestChanged derives the EVT-007 friend notifications.
func (s *IngestService) handleFriendRequestChanged(ctx context.Context, logger *zap.Logger, topic string, message models.Message) error {
	var payload models.FriendRequestChanged
	if !s.decodePayload(topic, message, &payload) {
		return nil
	}
	switch payload.Status {
	case models.FriendRequestDeclined, models.FriendRequestCancelled:
		return s.recordNoop(ctx, topic, message.Envelope)
	case models.FriendRequestPending, models.FriendRequestAccepted:
		if !models.IsUUID(payload.SenderUserID) || !models.IsUUID(payload.RecipientUserID) {
			s.dropMalformed(topic, message.Envelope, "invalid_payload", errors.New("friend_request_changed payload is incomplete"))
			return nil
		}
		return s.ingestNotifications(ctx, logger, topic, message.Envelope, s.mapFriendRequestChanged(ctx, payload))
	default:
		s.dropMalformed(topic, message.Envelope, "invalid_payload", fmt.Errorf("unknown friend request status %q", payload.Status))
		return nil
	}
}

// handleMessageChanged derives the EVT-009 mention notifications.
func (s *IngestService) handleMessageChanged(ctx context.Context, logger *zap.Logger, topic string, message models.Message) error {
	var payload models.MessageChanged
	if !s.decodePayload(topic, message, &payload) {
		return nil
	}
	switch payload.Change {
	case models.MessageChangeCreated:
		if len(payload.MentionedUserIDs) == 0 {
			return s.recordNoop(ctx, topic, message.Envelope)
		}
		if !models.IsUUID(payload.MessageID) || !models.IsUUID(payload.ContainerID) || !models.IsUUID(payload.AuthorUserID) ||
			(payload.ContainerType != models.ContainerTypeChannel && payload.ContainerType != models.ContainerTypeConversation) {
			s.dropMalformed(topic, message.Envelope, "invalid_payload", errors.New("message_changed payload is incomplete"))
			return nil
		}
		return s.ingestNotifications(ctx, logger, topic, message.Envelope, s.mapMessageChanged(ctx, payload))
	case models.MessageChangeEdited, models.MessageChangeDeleted, models.MessageChangeReactionsChanged:
		return s.recordNoop(ctx, topic, message.Envelope)
	default:
		s.dropMalformed(topic, message.Envelope, "invalid_payload", fmt.Errorf("unknown message change %q", payload.Change))
		return nil
	}
}

// ingestNotifications persists every derived notification with its email
// decision and outbox row in one transaction, then performs the post-commit
// work (email send, EVT-011 publish).
func (s *IngestService) ingestNotifications(ctx context.Context, logger *zap.Logger, topic string, cause models.Envelope, drafts []models.NewNotification) error {
	if len(drafts) == 0 {
		return s.recordNoop(ctx, topic, cause)
	}

	now := s.clock()
	var (
		created   []persistedNotification
		duplicate bool
	)

	err := s.transactor.InTx(ctx, func(txCtx context.Context) error {
		created = created[:0]
		fresh, err := s.processed.Record(txCtx, cause.EventID, topic, now)
		if err != nil {
			return err
		}
		if !fresh {
			duplicate = true
			return nil
		}

		for _, draft := range drafts {
			prefs, err := s.loadPreferences(txCtx, draft.RecipientUserID)
			if err != nil {
				return err
			}
			notification, err := s.notifications.Insert(txCtx, draft, now)
			if err != nil {
				return err
			}
			delivery, err := s.decideDelivery(txCtx, notification, prefs, now)
			if err != nil {
				return err
			}
			delivery, err = s.deliveries.Insert(txCtx, delivery)
			if err != nil {
				return err
			}

			item := persistedNotification{notification: notification, delivery: delivery}
			if prefs.InAppAllowed(notification.Kind) {
				outboxID, err := s.outbox.Enqueue(txCtx, notification.ID, now)
				if err != nil {
					return err
				}
				item.outboxID = outboxID
			}
			created = append(created, item)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if duplicate {
		logger.Debug("duplicate event ignored")
		return nil
	}

	s.dispatchAfterCommit(ctx, logger, cause, created)
	return nil
}

// dispatchAfterCommit sends due emails and publishes EVT-011 for committed
// notifications. Failures never undo the in-app notification.
func (s *IngestService) dispatchAfterCommit(ctx context.Context, logger *zap.Logger, cause models.Envelope, created []persistedNotification) {
	for _, item := range created {
		itemLogger := logger.With(
			zap.String("notification_id", item.notification.ID),
			zap.String("kind", string(item.notification.Kind)),
			zap.String("recipient_user_id", item.notification.RecipientUserID),
		)
		s.observer.NotificationCreated(item.notification.Kind)
		itemLogger.Info("notification created")

		switch item.delivery.Status {
		case models.DeliveryPending:
			s.attemptSend(ctx, itemLogger, item.delivery, item.notification)
		case models.DeliveryThrottled:
			s.observer.EmailThrottled(item.delivery.Kind)
			s.observer.EmailResult(item.delivery.Kind, models.DeliveryThrottled)
			itemLogger.Info("mention email throttled", zap.Duration("window", s.throttleWindow))
		default:
			s.observer.EmailResult(item.delivery.Kind, item.delivery.Status)
			itemLogger.Info("notification email skipped", zap.String("result", string(item.delivery.Status)))
		}

		if item.outboxID != "" {
			if err := s.publisher.Publish(ctx, item.outboxID, cause); err != nil {
				itemLogger.Warn("notification_created publish deferred to the outbox drainer", zap.Error(err))
			}
		}
	}
}

// recordNoop deduplicates an event that produces no notification.
func (s *IngestService) recordNoop(ctx context.Context, topic string, cause models.Envelope) error {
	now := s.clock()
	return s.transactor.InTx(ctx, func(txCtx context.Context) error {
		_, err := s.processed.Record(txCtx, cause.EventID, topic, now)
		return err
	})
}

// decodePayload unmarshals an event payload, dropping the event once when it is
// malformed.
func (s *IngestService) decodePayload(topic string, message models.Message, out any) bool {
	if err := models.DecodePayload(message.Payload, out); err != nil {
		s.dropMalformed(topic, message.Envelope, "invalid_payload", err)
		return false
	}
	return true
}

// dropMalformed logs an unusable event exactly once and counts it; the offset is
// committed so the event is never reprocessed endlessly.
func (s *IngestService) dropMalformed(topic string, source any, class string, cause error) {
	eventID := ""
	switch value := source.(type) {
	case models.Envelope:
		eventID = value.EventID
	case []byte:
		eventID = rawEventID(value)
	}
	s.observer.EventError(topic, class)
	s.logger.Warn("dropping event",
		zap.String("service_name", "notification_service"),
		zap.String("operation", "consume_event"),
		zap.String("topic", topic),
		zap.String("event_id", eventID),
		zap.String("error_class", class),
		zap.Error(cause),
	)
}

// rawEventID best-effort extracts envelope.event_id from a raw message.
func rawEventID(raw []byte) string {
	var partial struct {
		Envelope struct {
			EventID string `json:"event_id"`
		} `json:"envelope"`
	}
	if err := json.Unmarshal(raw, &partial); err != nil {
		return ""
	}
	return partial.Envelope.EventID
}

func validSanctionAction(action string) bool {
	switch action {
	case models.SanctionActionMute, models.SanctionActionUnmute, models.SanctionActionKick,
		models.SanctionActionBan, models.SanctionActionUnban:
		return true
	}
	return false
}

func validSanctionSource(source string) bool {
	switch source {
	case models.SanctionSourceModerator, models.SanctionSourceExpiry, models.SanctionSourceAppeal:
		return true
	}
	return false
}

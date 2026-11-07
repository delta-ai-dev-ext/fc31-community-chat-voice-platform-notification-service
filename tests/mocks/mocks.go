// Package mocks provides testify mocks for the interface boundaries of
// notification_service: repositories, the broker publisher, the Resend adapter
// and the use-case seams used by handler tests.
package mocks

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"

	"notification_service/internal/auth"
	"notification_service/internal/models"
	"notification_service/internal/provider"
	"notification_service/internal/repository"
)

// Verifier is a mock handler.TokenVerifier.
type Verifier struct{ mock.Mock }

// Verify implements handler.TokenVerifier.
func (m *Verifier) Verify(ctx context.Context, rawToken string) (auth.Claims, error) {
	args := m.Called(ctx, rawToken)
	claims, _ := args.Get(0).(auth.Claims)
	return claims, args.Error(1)
}

// Inbox is a mock handler.Inbox.
type Inbox struct{ mock.Mock }

// List implements handler.Inbox.
func (m *Inbox) List(ctx context.Context, recipientUserID string, unreadOnly bool, limit int, cursor string) (models.NotificationPage, error) {
	args := m.Called(ctx, recipientUserID, unreadOnly, limit, cursor)
	page, _ := args.Get(0).(models.NotificationPage)
	return page, args.Error(1)
}

// MarkRead implements handler.Inbox.
func (m *Inbox) MarkRead(ctx context.Context, recipientUserID, notificationID string) error {
	args := m.Called(ctx, recipientUserID, notificationID)
	return args.Error(0)
}

// MarkAllRead implements handler.Inbox.
func (m *Inbox) MarkAllRead(ctx context.Context, recipientUserID string) error {
	args := m.Called(ctx, recipientUserID)
	return args.Error(0)
}

// Preferences is a mock handler.Preferences.
type Preferences struct{ mock.Mock }

// Get implements handler.Preferences.
func (m *Preferences) Get(ctx context.Context, userID string) (models.Preferences, error) {
	args := m.Called(ctx, userID)
	prefs, _ := args.Get(0).(models.Preferences)
	return prefs, args.Error(1)
}

// Replace implements handler.Preferences.
func (m *Preferences) Replace(ctx context.Context, userID string, body []byte) (models.Preferences, error) {
	args := m.Called(ctx, userID, body)
	prefs, _ := args.Get(0).(models.Preferences)
	return prefs, args.Error(1)
}

// ReadyChecker is a mock handler.ReadyChecker.
type ReadyChecker struct{ mock.Mock }

// Ready implements handler.ReadyChecker.
func (m *ReadyChecker) Ready(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// NotificationRepository is a mock repository.NotificationRepository.
type NotificationRepository struct{ mock.Mock }

// Insert implements repository.NotificationRepository.
func (m *NotificationRepository) Insert(ctx context.Context, n models.NewNotification, createdAt time.Time) (models.Notification, error) {
	args := m.Called(ctx, n, createdAt)
	notification, _ := args.Get(0).(models.Notification)
	return notification, args.Error(1)
}

// Get implements repository.NotificationRepository.
func (m *NotificationRepository) Get(ctx context.Context, recipientUserID, notificationID string) (models.Notification, error) {
	args := m.Called(ctx, recipientUserID, notificationID)
	notification, _ := args.Get(0).(models.Notification)
	return notification, args.Error(1)
}

// GetByID implements repository.NotificationRepository.
func (m *NotificationRepository) GetByID(ctx context.Context, notificationID string) (models.Notification, error) {
	args := m.Called(ctx, notificationID)
	notification, _ := args.Get(0).(models.Notification)
	return notification, args.Error(1)
}

// List implements repository.NotificationRepository.
func (m *NotificationRepository) List(ctx context.Context, opts models.ListOptions) ([]models.Notification, error) {
	args := m.Called(ctx, opts)
	notifications, _ := args.Get(0).([]models.Notification)
	return notifications, args.Error(1)
}

// UnreadCount implements repository.NotificationRepository.
func (m *NotificationRepository) UnreadCount(ctx context.Context, recipientUserID string) (int, error) {
	args := m.Called(ctx, recipientUserID)
	return args.Int(0), args.Error(1)
}

// MarkRead implements repository.NotificationRepository.
func (m *NotificationRepository) MarkRead(ctx context.Context, recipientUserID, notificationID string, readAt time.Time) (repository.ReadOutcome, error) {
	args := m.Called(ctx, recipientUserID, notificationID, readAt)
	outcome, _ := args.Get(0).(repository.ReadOutcome)
	return outcome, args.Error(1)
}

// MarkAllRead implements repository.NotificationRepository.
func (m *NotificationRepository) MarkAllRead(ctx context.Context, recipientUserID string, readAt time.Time) (map[models.Kind]int, error) {
	args := m.Called(ctx, recipientUserID, readAt)
	counts, _ := args.Get(0).(map[models.Kind]int)
	return counts, args.Error(1)
}

// PreferenceRepository is a mock repository.PreferenceRepository.
type PreferenceRepository struct{ mock.Mock }

// Get implements repository.PreferenceRepository.
func (m *PreferenceRepository) Get(ctx context.Context, userID string) (models.Preferences, error) {
	args := m.Called(ctx, userID)
	prefs, _ := args.Get(0).(models.Preferences)
	return prefs, args.Error(1)
}

// Upsert implements repository.PreferenceRepository.
func (m *PreferenceRepository) Upsert(ctx context.Context, userID string, prefs models.Preferences, now time.Time) (models.Preferences, error) {
	args := m.Called(ctx, userID, prefs, now)
	saved, _ := args.Get(0).(models.Preferences)
	return saved, args.Error(1)
}

// RecipientRepository is a mock repository.RecipientRepository.
type RecipientRepository struct{ mock.Mock }

// Upsert implements repository.RecipientRepository.
func (m *RecipientRepository) Upsert(ctx context.Context, projection models.RecipientProjection, now time.Time) (bool, error) {
	args := m.Called(ctx, projection, now)
	return args.Bool(0), args.Error(1)
}

// Get implements repository.RecipientRepository.
func (m *RecipientRepository) Get(ctx context.Context, userID string) (models.RecipientProjection, error) {
	args := m.Called(ctx, userID)
	projection, _ := args.Get(0).(models.RecipientProjection)
	return projection, args.Error(1)
}

// EmailDeliveryRepository is a mock repository.EmailDeliveryRepository.
type EmailDeliveryRepository struct{ mock.Mock }

// Insert implements repository.EmailDeliveryRepository.
func (m *EmailDeliveryRepository) Insert(ctx context.Context, delivery models.EmailDelivery) (models.EmailDelivery, error) {
	args := m.Called(ctx, delivery)
	saved, _ := args.Get(0).(models.EmailDelivery)
	return saved, args.Error(1)
}

// Update implements repository.EmailDeliveryRepository.
func (m *EmailDeliveryRepository) Update(ctx context.Context, delivery models.EmailDelivery, now time.Time) error {
	args := m.Called(ctx, delivery, now)
	return args.Error(0)
}

// ListDue implements repository.EmailDeliveryRepository.
func (m *EmailDeliveryRepository) ListDue(ctx context.Context, due time.Time, limit int) ([]models.EmailDelivery, error) {
	args := m.Called(ctx, due, limit)
	deliveries, _ := args.Get(0).([]models.EmailDelivery)
	return deliveries, args.Error(1)
}

// ProcessedEventRepository is a mock repository.ProcessedEventRepository.
type ProcessedEventRepository struct{ mock.Mock }

// Record implements repository.ProcessedEventRepository.
func (m *ProcessedEventRepository) Record(ctx context.Context, eventID, topic string, now time.Time) (bool, error) {
	args := m.Called(ctx, eventID, topic, now)
	return args.Bool(0), args.Error(1)
}

// Prune implements repository.ProcessedEventRepository.
func (m *ProcessedEventRepository) Prune(ctx context.Context, before time.Time) (int64, error) {
	args := m.Called(ctx, before)
	return args.Get(0).(int64), args.Error(1)
}

// MentionWindowRepository is a mock repository.MentionWindowRepository.
type MentionWindowRepository struct{ mock.Mock }

// TryClaim implements repository.MentionWindowRepository.
func (m *MentionWindowRepository) TryClaim(ctx context.Context, recipientUserID string, now time.Time, window time.Duration) (bool, error) {
	args := m.Called(ctx, recipientUserID, now, window)
	return args.Bool(0), args.Error(1)
}

// OutboxRepository is a mock repository.OutboxRepository.
type OutboxRepository struct{ mock.Mock }

// Enqueue implements repository.OutboxRepository.
func (m *OutboxRepository) Enqueue(ctx context.Context, notificationID string, now time.Time) (string, error) {
	args := m.Called(ctx, notificationID, now)
	return args.String(0), args.Error(1)
}

// ListPending implements repository.OutboxRepository.
func (m *OutboxRepository) ListPending(ctx context.Context, limit int) ([]models.OutboxEntry, error) {
	args := m.Called(ctx, limit)
	entries, _ := args.Get(0).([]models.OutboxEntry)
	return entries, args.Error(1)
}

// MarkPublished implements repository.OutboxRepository.
func (m *OutboxRepository) MarkPublished(ctx context.Context, outboxID string, now time.Time) error {
	args := m.Called(ctx, outboxID, now)
	return args.Error(0)
}

// Transactor is a fake repository.Transactor. By default it runs the callback
// inline (no real database transaction); set Err to simulate a transaction
// failure.
type Transactor struct {
	// Err, when non-nil, is returned instead of running the callback.
	Err error
	// Calls counts how often InTx ran.
	Calls int
}

// InTx implements repository.Transactor.
func (m *Transactor) InTx(ctx context.Context, fn func(ctx context.Context) error) error {
	m.Calls++
	if m.Err != nil {
		return m.Err
	}
	return fn(ctx)
}

// Publisher is a mock service.Publisher (the transactional outbox drainer).
type Publisher struct{ mock.Mock }

// Publish implements service.Publisher.
func (m *Publisher) Publish(ctx context.Context, outboxID string, cause models.Envelope) error {
	args := m.Called(ctx, outboxID, cause)
	return args.Error(0)
}

// Drain implements service.Publisher.
func (m *Publisher) Drain(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// EmailSender is a mock provider.EmailSender.
type EmailSender struct{ mock.Mock }

// Send implements provider.EmailSender.
func (m *EmailSender) Send(ctx context.Context, message provider.EmailMessage) (string, error) {
	args := m.Called(ctx, message)
	return args.String(0), args.Error(1)
}

// Observer is a mock service.Observer.
type Observer struct{ mock.Mock }

// NotificationCreated implements service.Observer.
func (m *Observer) NotificationCreated(kind models.Kind) { m.Called(kind) }

// NotificationRead implements service.Observer.
func (m *Observer) NotificationRead(kind models.Kind, n int) { m.Called(kind, n) }

// EmailResult implements service.Observer.
func (m *Observer) EmailResult(kind models.Kind, result models.DeliveryStatus) {
	m.Called(kind, result)
}

// EmailThrottled implements service.Observer.
func (m *Observer) EmailThrottled(kind models.Kind) { m.Called(kind) }

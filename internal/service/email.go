package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"notification_service/internal/models"
	"notification_service/internal/provider"
	"notification_service/internal/repository"
)

// DefaultMaxEmailAttempts bounds how often one delivery is retried before it is
// recorded as failed.
const DefaultMaxEmailAttempts = 5

// DefaultRetryBatch bounds one retry sweep.
const DefaultRetryBatch = 50

// maxEmailBackoff caps the exponential back-off between Resend retries.
const maxEmailBackoff = 15 * time.Minute

// DefaultRetryBackoff is the bounded exponential back-off (30s, 1m, 2m, 4m, 8m,
// capped at 15m) used after retryable provider failures.
func DefaultRetryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	backoff := 30 * time.Second << (attempt - 1)
	if backoff > maxEmailBackoff || backoff <= 0 {
		return maxEmailBackoff
	}
	return backoff
}

// decideDelivery computes the initial email delivery row of a notification.
// The decision is persisted in the same transaction as the notification.
func (s *IngestService) decideDelivery(ctx context.Context, notification models.Notification, prefs models.Preferences, now time.Time) (models.EmailDelivery, error) {
	delivery := models.EmailDelivery{
		NotificationID:  notification.ID,
		Kind:            notification.Kind,
		RecipientUserID: notification.RecipientUserID,
		CreatedAt:       now,
	}

	if !prefs.EmailAllowed(notification.Kind) {
		delivery.Status = models.DeliverySkippedPreference
		return delivery, nil
	}

	projection, err := s.recipients.Get(ctx, notification.RecipientUserID)
	switch {
	case errors.Is(err, repository.ErrNotFound):
		// The recipient projection has not arrived yet: schedule the send and
		// let the bounded retry budget decide (recorded as failed when spent).
		delivery.Status = models.DeliveryPending
		attemptAt := now
		delivery.NextAttemptAt = &attemptAt
		return delivery, nil
	case err != nil:
		return models.EmailDelivery{}, fmt.Errorf("service: load recipient projection: %w", err)
	}

	if !projection.EmailVerified || strings.TrimSpace(projection.Email) == "" {
		delivery.Status = models.DeliverySkippedUnverified
		return delivery, nil
	}
	if projection.Status != models.AccountActive {
		delivery.Status = models.DeliverySkippedDeactivated
		return delivery, nil
	}

	if notification.Kind == models.KindMention {
		claimed, claimErr := s.windows.TryClaim(ctx, notification.RecipientUserID, now, s.throttleWindow)
		if claimErr != nil {
			return models.EmailDelivery{}, fmt.Errorf("service: claim mention email window: %w", claimErr)
		}
		if !claimed {
			delivery.Status = models.DeliveryThrottled
			return delivery, nil
		}
	}

	delivery.Status = models.DeliveryPending
	attemptAt := now
	delivery.NextAttemptAt = &attemptAt
	return delivery, nil
}

// attemptSend tries to deliver one pending email delivery and records the
// outcome. It never blocks the in-app notification.
func (s *IngestService) attemptSend(ctx context.Context, logger *zap.Logger, delivery models.EmailDelivery, notification models.Notification) {
	now := s.clock()

	projection, err := s.recipients.Get(ctx, delivery.RecipientUserID)
	if err != nil {
		class := "recipient_projection_lookup_failed"
		if errors.Is(err, repository.ErrNotFound) {
			class = "recipient_projection_missing"
		} else {
			logger.Warn("recipient projection lookup failed", zap.String("operation", "send_notification_email"), zap.Error(err))
		}
		// The projection may still arrive: retry within the bounded budget and
		// record failed only when it is spent.
		s.recordFailure(ctx, logger, delivery, now, class, true, err)
		return
	}
	if !projection.EmailVerified || strings.TrimSpace(projection.Email) == "" {
		delivery.Status = models.DeliverySkippedUnverified
		delivery.NextAttemptAt = nil
		s.persistDelivery(ctx, logger, delivery, now)
		return
	}
	if projection.Status != models.AccountActive {
		delivery.Status = models.DeliverySkippedDeactivated
		delivery.NextAttemptAt = nil
		s.persistDelivery(ctx, logger, delivery, now)
		return
	}

	subject, text := RenderNotificationEmail(notification)
	messageID, err := s.sender.Send(ctx, provider.EmailMessage{
		IdempotencyKey: notification.ID,
		From:           s.mailFrom,
		To:             projection.Email,
		Subject:        subject,
		Text:           text,
	})
	if err != nil {
		class := "permanent"
		retryable := provider.IsRetryable(err)
		if retryable {
			class = "retryable"
		}
		s.recordFailure(ctx, logger, delivery, now, class, retryable, err)
		return
	}

	delivery.Attempts++
	delivery.Status = models.DeliverySent
	delivery.ProviderMessageID = &messageID
	delivery.LastError = nil
	delivery.NextAttemptAt = nil
	s.persistDelivery(ctx, logger, delivery, now)
}

// recordFailure applies the retry policy: retryable failures are re-scheduled on
// email_deliveries.next_attempt_at until the bounded budget is spent, after
// which they are recorded as failed.
func (s *IngestService) recordFailure(ctx context.Context, logger *zap.Logger, delivery models.EmailDelivery, now time.Time, class string, retryable bool, cause error) {
	delivery.Attempts++
	if cause != nil {
		message := truncate(cause.Error(), MaxSummaryLength)
		delivery.LastError = &message
	} else {
		message := class
		delivery.LastError = &message
	}

	if retryable && delivery.Attempts < s.maxEmailAttempts {
		delay := s.backoff(delivery.Attempts)
		if retryAfter := provider.RetryAfter(cause); retryAfter > delay {
			delay = retryAfter
		}
		delivery.Status = models.DeliveryPending
		nextAttempt := now.Add(delay)
		delivery.NextAttemptAt = &nextAttempt
		logger.Warn("notification email send failed, retry scheduled",
			zap.String("operation", "send_notification_email"),
			zap.String("error_class", class),
			zap.Int("attempts", delivery.Attempts),
			zap.Time("next_attempt_at", nextAttempt),
		)
	} else {
		delivery.Status = models.DeliveryFailed
		delivery.NextAttemptAt = nil
		logger.Warn("notification email send failed permanently",
			zap.String("operation", "send_notification_email"),
			zap.String("error_class", class),
			zap.Int("attempts", delivery.Attempts),
		)
	}
	s.persistDelivery(ctx, logger, delivery, now)
}

// persistDelivery stores the delivery outcome and counts it once it is no longer
// pending.
func (s *IngestService) persistDelivery(ctx context.Context, logger *zap.Logger, delivery models.EmailDelivery, now time.Time) {
	if err := s.deliveries.Update(ctx, delivery, now); err != nil {
		logger.Error("could not record email delivery outcome",
			zap.String("operation", "record_email_delivery"),
			zap.String("notification_id", delivery.NotificationID),
			zap.Error(err),
		)
		return
	}
	if delivery.Status != models.DeliveryPending {
		s.observer.EmailResult(delivery.Kind, delivery.Status)
	}
}

// RetryDue retries pending email deliveries whose back-off elapsed.
func (s *IngestService) RetryDue(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = DefaultRetryBatch
	}
	due, err := s.deliveries.ListDue(ctx, s.clock(), limit)
	if err != nil {
		return fmt.Errorf("service: list due email deliveries: %w", err)
	}
	for _, delivery := range due {
		notification, err := s.notifications.GetByID(ctx, delivery.NotificationID)
		if err != nil {
			s.logger.Warn("notification of due email delivery is unavailable",
				zap.String("operation", "retry_notification_email"),
				zap.String("notification_id", delivery.NotificationID),
				zap.Error(err),
			)
			continue
		}
		if existing := delivery.Attempts; existing >= s.maxEmailAttempts {
			delivery.Attempts = s.maxEmailAttempts - 1
		}
		s.attemptSend(ctx, s.logger, delivery, notification)
	}
	return nil
}

// RenderNotificationEmail renders the subject and plain-text body of a
// notification email from the notification title, summary and deep link.
func RenderNotificationEmail(notification models.Notification) (string, string) {
	subject := truncate(notification.Title, MaxTitleLength)
	body := strings.Join([]string{
		notification.Title,
		"",
		notification.Summary,
		"",
		"Open in the app: " + notification.Link,
		"",
		"You receive this email because of your notification settings.",
	}, "\n")
	return subject, body
}

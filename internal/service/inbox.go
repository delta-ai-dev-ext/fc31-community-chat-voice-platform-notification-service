package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"notification_service/internal/models"
	"notification_service/internal/repository"
)

// InboxService serves the member-facing notification inbox.
type InboxService struct {
	notifications repository.NotificationRepository
	observed      Observer
	clock         Clock
}

// NewInboxService builds an InboxService.
func NewInboxService(notifications repository.NotificationRepository, observed Observer, clock Clock) *InboxService {
	if observed == nil {
		observed = NopObserver{}
	}
	if clock == nil {
		clock = SystemClock
	}
	return &InboxService{notifications: notifications, observed: observed, clock: clock}
}

// List returns one cursor-paginated page of the caller's notifications,
// newest-first, together with the caller's total unread count.
func (s *InboxService) List(ctx context.Context, recipientUserID string, unreadOnly bool, limit int, rawCursor string) (models.NotificationPage, error) {
	pageLimit, err := normalizeLimit(limit)
	if err != nil {
		return models.NotificationPage{}, err
	}

	var cursor *models.Cursor
	if rawCursor != "" {
		parsed, parseErr := models.ParseCursor(rawCursor)
		if parseErr != nil {
			return models.NotificationPage{}, apiInvalidCursor("cursor is not a valid pagination cursor")
		}
		cursor = &parsed
	}

	// One extra row tells us whether another page exists.
	rows, err := s.notifications.List(ctx, models.ListOptions{
		RecipientUserID: recipientUserID,
		UnreadOnly:      unreadOnly,
		Limit:           pageLimit + 1,
		Cursor:          cursor,
	})
	if err != nil {
		return models.NotificationPage{}, err
	}

	nextCursor := (*string)(nil)
	if len(rows) > pageLimit {
		rows = rows[:pageLimit]
		last := rows[len(rows)-1]
		encoded := models.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}.Encode()
		nextCursor = &encoded
	}
	if rows == nil {
		rows = []models.Notification{}
	}

	unreadCount, err := s.notifications.UnreadCount(ctx, recipientUserID)
	if err != nil {
		return models.NotificationPage{}, err
	}

	return models.NotificationPage{Items: rows, NextCursor: nextCursor, UnreadCount: unreadCount}, nil
}

// MarkRead marks one notification of the caller as read. It is idempotent and
// reports notification_not_found for a missing or foreign notification.
func (s *InboxService) MarkRead(ctx context.Context, recipientUserID, notificationID string) error {
	if !models.IsUUID(notificationID) {
		return models.NewAPIError(http.StatusBadRequest, models.CodeInvalidNotificationID,
			"notification_id must be a UUID")
	}
	outcome, err := s.notifications.MarkRead(ctx, recipientUserID, notificationID, s.clock())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return models.ErrNotificationNotFound
		}
		return fmt.Errorf("service: mark notification read: %w", err)
	}
	if outcome.Changed {
		s.observed.NotificationRead(outcome.Kind, 1)
	}
	return nil
}

// MarkAllRead marks every unread notification of the caller as read. It is a
// no-op (still successful) when the inbox has no unread items.
func (s *InboxService) MarkAllRead(ctx context.Context, recipientUserID string) error {
	counts, err := s.notifications.MarkAllRead(ctx, recipientUserID, s.clock())
	if err != nil {
		return fmt.Errorf("service: mark all notifications read: %w", err)
	}
	for kind, count := range counts {
		if count > 0 {
			s.observed.NotificationRead(kind, count)
		}
	}
	return nil
}

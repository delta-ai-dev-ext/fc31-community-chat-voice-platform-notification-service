// Package service holds the domain logic of notification_service: inbox
// queries, preference rules, event-to-notification mapping and the email
// pipeline. It depends on repository and provider interfaces only.
package service

import (
	"net/http"
	"strings"
	"time"

	"notification_service/internal/models"
)

// Clock returns the current UTC time. It is injected so that time-dependent
// rules (the mention throttle window) are deterministic in tests.
type Clock func() time.Time

// IDGenerator returns a fresh UUID. It is injected so that produced event ids
// and notification ids are deterministic in tests.
type IDGenerator func() string

// SystemClock reads the wall clock in UTC.
func SystemClock() time.Time { return time.Now().UTC() }

// Observer records business metrics. The metrics package implements it; tests
// use a recording fake.
type Observer interface {
	// NotificationCreated counts a persisted notification of kind.
	NotificationCreated(kind models.Kind)
	// NotificationRead counts n read transitions of kind.
	NotificationRead(kind models.Kind, n int)
	// EmailResult counts one recorded email outcome of kind.
	EmailResult(kind models.Kind, result models.DeliveryStatus)
	// EmailThrottled counts one throttled mention email of kind.
	EmailThrottled(kind models.Kind)
	// EventError counts a consumed event that could not be processed.
	EventError(topic, errorClass string)
}

// NopObserver discards every metric (used when no registry is wired).
type NopObserver struct{}

// NotificationCreated implements Observer.
func (NopObserver) NotificationCreated(models.Kind) {}

// NotificationRead implements Observer.
func (NopObserver) NotificationRead(models.Kind, int) {}

// EmailResult implements Observer.
func (NopObserver) EmailResult(models.Kind, models.DeliveryStatus) {}

// EmailThrottled implements Observer.
func (NopObserver) EmailThrottled(models.Kind) {}

// EventError implements Observer.
func (NopObserver) EventError(string, string) {}

// normalizeLimit applies the documented page-size rules (1–100, default 50).
func normalizeLimit(limit int) (int, error) {
	switch {
	case limit == 0:
		return models.DefaultPageLimit, nil
	case limit < 1 || limit > models.MaxPageLimit:
		return 0, models.NewAPIError(http.StatusBadRequest, models.CodeInvalidLimit,
			"limit must be between 1 and 100")
	default:
		return limit, nil
	}
}

// apiInvalidCursor builds the documented invalid_cursor error.
func apiInvalidCursor(detail string) *models.APIError {
	return models.NewAPIError(http.StatusBadRequest, models.CodeInvalidCursor, detail)
}

// truncate shortens s to at most n runes, appending an ellipsis when it was cut.
func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	if n <= 1 {
		return string(runes[:n])
	}
	return string(runes[:n-1]) + "…"
}

// MaxTitleLength and MaxSummaryLength mirror the notifications table constraints.
const (
	MaxTitleLength   = 200
	MaxSummaryLength = 500
)

// Package models holds the domain types, event contracts and API shapes of
// notification_service. It has no dependencies on infrastructure packages.
package models

import "time"

// Kind is the discriminator of a notification.
type Kind string

// Supported notification kinds (docs/architecture.md, openapi NotificationKind).
const (
	KindMention               Kind = "mention"
	KindFriendRequestReceived Kind = "friend_request_received"
	KindFriendRequestAccepted Kind = "friend_request_accepted"
	KindModerationNotice      Kind = "moderation_notice"
	KindAppealDecided         Kind = "appeal_decided"
)

// AllKinds lists every notification kind in the canonical order used by the
// preferences API.
var AllKinds = []Kind{
	KindMention,
	KindFriendRequestReceived,
	KindFriendRequestAccepted,
	KindModerationNotice,
	KindAppealDecided,
}

// Valid reports whether k is one of the registered notification kinds.
func (k Kind) Valid() bool {
	for _, known := range AllKinds {
		if k == known {
			return true
		}
	}
	return false
}

// Notification is a persisted in-app notification of one recipient.
type Notification struct {
	ID              string     `json:"id"`
	RecipientUserID string     `json:"-"`
	Kind            Kind       `json:"kind"`
	Title           string     `json:"title"`
	Summary         string     `json:"summary"`
	Link            string     `json:"link"`
	CreatedAt       time.Time  `json:"created_at"`
	ReadAt          *time.Time `json:"read_at"`
}

// Unread reports whether the notification has not been read yet.
func (n Notification) Unread() bool { return n.ReadAt == nil }

// NewNotification is the insert payload for a derived notification, before it
// receives an id and a creation timestamp from the database.
type NewNotification struct {
	RecipientUserID string
	Kind            Kind
	Title           string
	Summary         string
	Link            string
}

// NotificationPage is one cursor-paginated page of the caller's inbox.
type NotificationPage struct {
	Items       []Notification `json:"items"`
	NextCursor  *string        `json:"next_cursor"`
	UnreadCount int            `json:"unread_count"`
}

// ListOptions controls inbox listing.
type ListOptions struct {
	RecipientUserID string
	UnreadOnly      bool
	Limit           int
	Cursor          *Cursor
}

// Page limits from openapi_spec.yaml.
const (
	DefaultPageLimit = 50
	MaxPageLimit     = 100
)

// KindPreferences holds the two delivery toggles of a single notification kind.
type KindPreferences struct {
	InApp bool `json:"in_app"`
	Email bool `json:"email"`
}

// Preferences is the per-kind in-app/email toggle set of one user.
type Preferences struct {
	Mention               KindPreferences `json:"mention"`
	FriendRequestReceived KindPreferences `json:"friend_request_received"`
	FriendRequestAccepted KindPreferences `json:"friend_request_accepted"`
	ModerationNotice      KindPreferences `json:"moderation_notice"`
	AppealDecided         KindPreferences `json:"appeal_decided"`
}

// DefaultPreferences returns the preferences applied to users that never saved
// any: every kind enabled in-app and by email.
func DefaultPreferences() Preferences {
	on := KindPreferences{InApp: true, Email: true}
	return Preferences{
		Mention:               on,
		FriendRequestReceived: on,
		FriendRequestAccepted: on,
		ModerationNotice:      on,
		AppealDecided:         on,
	}
}

// For returns the toggles of a single kind.
func (p Preferences) For(kind Kind) KindPreferences {
	switch kind {
	case KindMention:
		return p.Mention
	case KindFriendRequestReceived:
		return p.FriendRequestReceived
	case KindFriendRequestAccepted:
		return p.FriendRequestAccepted
	case KindModerationNotice:
		return p.ModerationNotice
	case KindAppealDecided:
		return p.AppealDecided
	}
	return KindPreferences{}
}

// SetFor replaces the toggles of a single kind.
func (p *Preferences) SetFor(kind Kind, kp KindPreferences) {
	switch kind {
	case KindMention:
		p.Mention = kp
	case KindFriendRequestReceived:
		p.FriendRequestReceived = kp
	case KindFriendRequestAccepted:
		p.FriendRequestAccepted = kp
	case KindModerationNotice:
		p.ModerationNotice = kp
	case KindAppealDecided:
		p.AppealDecided = kp
	}
}

// EmailAllowed reports whether the kind's email toggle is enabled. Moderation
// notices are safety notices and always reach the inbox and the mailbox.
func (p Preferences) EmailAllowed(kind Kind) bool {
	if kind == KindModerationNotice {
		return true
	}
	return p.For(kind).Email
}

// InAppAllowed reports whether the kind's in-app toggle is enabled. Moderation
// notices ignore the toggle (they are never silenced).
func (p Preferences) InAppAllowed(kind Kind) bool {
	if kind == KindModerationNotice {
		return true
	}
	return p.For(kind).InApp
}

// DeliveryStatus is the outcome of an email delivery attempt.
type DeliveryStatus string

// Email delivery statuses from docs/architecture.md.
const (
	DeliveryPending            DeliveryStatus = "pending"
	DeliverySent               DeliveryStatus = "sent"
	DeliveryThrottled          DeliveryStatus = "throttled"
	DeliverySkippedPreference  DeliveryStatus = "skipped_preference"
	DeliverySkippedUnverified  DeliveryStatus = "skipped_unverified"
	DeliverySkippedDeactivated DeliveryStatus = "skipped_deactivated"
	DeliveryFailed             DeliveryStatus = "failed"
)

// EmailDelivery records the single email decision and its outcome for one
// notification.
type EmailDelivery struct {
	ID                string
	NotificationID    string
	Kind              Kind
	RecipientUserID   string
	Status            DeliveryStatus
	ProviderMessageID *string
	Attempts          int
	LastError         *string
	NextAttemptAt     *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// AccountStatus is the lifecycle state of a user account in the local
// projection.
type AccountStatus string

// Account statuses from EVT-001.
const (
	AccountActive      AccountStatus = "active"
	AccountDeactivated AccountStatus = "deactivated"
)

// RecipientProjection is the local projection of a notification recipient's
// contact data, maintained from EVT-001.
type RecipientProjection struct {
	UserID         string
	Email          string
	Username       string
	DisplayName    string
	EmailVerified  bool
	Status         AccountStatus
	AccountVersion int
	UpdatedAt      time.Time
}

// Eligible reports whether the projection may receive notification emails.
func (r RecipientProjection) Eligible() bool {
	return r.EmailVerified && r.Status == AccountActive && r.Email != ""
}

// ProcessedEvent is a deduplication record for one consumed event.
type ProcessedEvent struct {
	EventID     string
	Topic       string
	ProcessedAt time.Time
}

// OutboxEntry is a pending or published EVT-011 publication.
type OutboxEntry struct {
	ID             string
	NotificationID string
	PublishedAt    *time.Time
	CreatedAt      time.Time
}

// OutboxMessage is a pending outbox row joined with the notification snapshot
// it must publish.
type OutboxMessage struct {
	OutboxID  string
	Envelope  Envelope
	Payload   NotificationCreated
	CreatedAt time.Time
}

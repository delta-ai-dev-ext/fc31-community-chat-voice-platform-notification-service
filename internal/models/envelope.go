package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Envelope is the mandatory nine-field event envelope carried by every Kafka
// message on every topic (docs/architecture.md "Envelope").
type Envelope struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	EventVersion  int       `json:"event_version"`
	OccurredAt    time.Time `json:"occurred_at"`
	Producer      string    `json:"producer"`
	TraceID       string    `json:"trace_id"`
	CorrelationID string    `json:"correlation_id"`
	CausationID   *string   `json:"causation_id"`
	SchemaRef     string    `json:"schema_ref"`
}

// Message is the two-key wire form of every Kafka message: an envelope and its
// payload.
type Message struct {
	Envelope Envelope        `json:"envelope"`
	Payload  json.RawMessage `json:"payload"`
}

// Event types and producers of the contracts this service consumes/produces.
const (
	EventTypeUserAccountChanged    = "user_account_changed"
	EventTypeMemberSanctionChanged = "member_sanction_changed"
	EventTypeAppealStatusChanged   = "appeal_status_changed"
	EventTypeFriendRequestChanged  = "friend_request_changed"
	EventTypeMessageChanged        = "message_changed"
	EventTypeNotificationCreated   = "notification_created"

	ProducerNotificationService = "notification_service"

	SchemaRefNotificationCreated = "EVENT_CONTRACTS.md#notification_created-v1"
)

// Topics consumed and produced by this service.
const (
	TopicIdentityEvents     = "identity_events"
	TopicModerationEvents   = "moderation_events"
	TopicSocialEvents       = "social_events"
	TopicMessageEvents      = "message_events"
	TopicNotificationEvents = "notification_events"
)

// ConsumedTopics is the ordered topic set read by the single consumer group.
var ConsumedTopics = []string{
	TopicIdentityEvents,
	TopicModerationEvents,
	TopicSocialEvents,
	TopicMessageEvents,
}

var (
	snakeCasePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	traceIDPattern   = regexp.MustCompile(`^[0-9a-f]{32}$`)
	schemaRefPattern = regexp.MustCompile(`^EVENT_CONTRACTS\.md#[a-z][a-z0-9_]*-v[0-9]+$`)
)

// ErrInvalidEnvelope is the sentinel wrapped by every envelope validation error.
var ErrInvalidEnvelope = errors.New("invalid envelope")

// ValidateEnvelope checks every mandatory envelope rule; the returned error
// wraps ErrInvalidEnvelope.
func ValidateEnvelope(env Envelope) error {
	if env.EventID == "" {
		return fmt.Errorf("%w: event_id is empty", ErrInvalidEnvelope)
	}
	if !snakeCasePattern.MatchString(env.EventType) {
		return fmt.Errorf("%w: event_type %q is not snake_case", ErrInvalidEnvelope, env.EventType)
	}
	if env.EventVersion < 1 {
		return fmt.Errorf("%w: event_version must be >= 1", ErrInvalidEnvelope)
	}
	if env.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred_at is empty", ErrInvalidEnvelope)
	}
	if !snakeCasePattern.MatchString(env.Producer) {
		return fmt.Errorf("%w: producer %q is not snake_case", ErrInvalidEnvelope, env.Producer)
	}
	if !traceIDPattern.MatchString(env.TraceID) {
		return fmt.Errorf("%w: trace_id must be 32 lowercase hex characters", ErrInvalidEnvelope)
	}
	if env.CorrelationID == "" {
		return fmt.Errorf("%w: correlation_id is empty", ErrInvalidEnvelope)
	}
	if env.CausationID != nil && *env.CausationID == "" {
		return fmt.Errorf("%w: causation_id must be a uuid or null", ErrInvalidEnvelope)
	}
	if !schemaRefPattern.MatchString(env.SchemaRef) {
		return fmt.Errorf("%w: schema_ref %q does not match EVENT_CONTRACTS.md#<event>-v<n>", ErrInvalidEnvelope, env.SchemaRef)
	}
	return nil
}

// DecodeMessage validates the envelope of a raw Kafka value and returns the
// stamped message together with its raw payload.
func DecodeMessage(raw []byte) (Message, error) {
	var msg Message
	if err := json.Unmarshal(raw, &msg); err != nil {
		return Message{}, fmt.Errorf("%w: malformed json: %v", ErrInvalidEnvelope, err)
	}
	if err := ValidateEnvelope(msg.Envelope); err != nil {
		return Message{}, err
	}
	if len(msg.Payload) == 0 || string(msg.Payload) == "null" {
		return Message{}, fmt.Errorf("%w: payload is missing", ErrInvalidEnvelope)
	}
	return msg, nil
}

// DecodePayload unmarshals the payload of a consumed event into out.
func DecodePayload(raw json.RawMessage, out any) error {
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("%w: malformed payload: %v", ErrInvalidEnvelope, err)
	}
	return nil
}

// Kafka header names mirrored from the envelope.
const (
	HeaderTraceID   = "trace_id"
	HeaderEventType = "event_type"
)

package models_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"notification_service/internal/models"
)

func validEnvelope() models.Envelope {
	return models.Envelope{
		EventID:       "11111111-1111-4111-8111-111111111111",
		EventType:     "message_changed",
		EventVersion:  1,
		OccurredAt:    time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC),
		Producer:      "messaging_service",
		TraceID:       "4bf92f3577b34da6a3ce929d0e0e4736",
		CorrelationID: "22222222-2222-4222-8222-222222222222",
		SchemaRef:     "EVENT_CONTRACTS.md#message_changed-v1",
	}
}

func TestValidateEnvelopeAcceptsCompleteEnvelope(t *testing.T) {
	t.Parallel()
	require.NoError(t, models.ValidateEnvelope(validEnvelope()))

	withCausation := validEnvelope()
	causation := "33333333-3333-4333-8333-333333333333"
	withCausation.CausationID = &causation
	require.NoError(t, models.ValidateEnvelope(withCausation))
}

func TestValidateEnvelopeRejectsInvalidEnvelopes(t *testing.T) {
	t.Parallel()
	emptyCausation := ""
	zeroTime := time.Time{}

	tests := []struct {
		name   string
		mutate func(env *models.Envelope)
	}{
		{name: "missing event id", mutate: func(env *models.Envelope) { env.EventID = "" }},
		{name: "event type not snake case", mutate: func(env *models.Envelope) { env.EventType = "MessageChanged" }},
		{name: "event type with dash", mutate: func(env *models.Envelope) { env.EventType = "message-changed" }},
		{name: "event version zero", mutate: func(env *models.Envelope) { env.EventVersion = 0 }},
		{name: "missing occurred_at", mutate: func(env *models.Envelope) { env.OccurredAt = zeroTime }},
		{name: "producer not snake case", mutate: func(env *models.Envelope) { env.Producer = "Messaging" }},
		{name: "trace id too short", mutate: func(env *models.Envelope) { env.TraceID = "4bf92f3577b34da6a3ce929d0e0e473" }},
		{name: "trace id upper case", mutate: func(env *models.Envelope) { env.TraceID = "4BF92F3577B34DA6A3CE929D0E0E4736" }},
		{name: "trace id empty", mutate: func(env *models.Envelope) { env.TraceID = "" }},
		{name: "missing correlation id", mutate: func(env *models.Envelope) { env.CorrelationID = "" }},
		{name: "empty causation id", mutate: func(env *models.Envelope) { env.CausationID = &emptyCausation }},
		{name: "bad schema ref", mutate: func(env *models.Envelope) { env.SchemaRef = "message_changed-v1" }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			env := validEnvelope()
			test.mutate(&env)
			err := models.ValidateEnvelope(env)
			require.Error(t, err)
			require.True(t, errors.Is(err, models.ErrInvalidEnvelope))
		})
	}
}

func TestDecodeMessage(t *testing.T) {
	t.Parallel()

	envelope := validEnvelope()
	payload := map[string]any{"message_id": "abc"}
	valid := map[string]any{"envelope": envelope, "payload": payload}
	encoded, err := json.Marshal(valid)
	require.NoError(t, err)

	message, err := models.DecodeMessage(encoded)
	require.NoError(t, err)
	require.Equal(t, envelope.EventID, message.Envelope.EventID)
	require.Contains(t, string(message.Payload), "message_id")

	_, err = models.DecodeMessage([]byte("{"))
	require.ErrorIs(t, err, models.ErrInvalidEnvelope)

	withoutPayload, err := json.Marshal(map[string]any{"envelope": envelope})
	require.NoError(t, err)
	_, err = models.DecodeMessage(withoutPayload)
	require.ErrorIs(t, err, models.ErrInvalidEnvelope)

	brokenEnvelope, err := json.Marshal(map[string]any{"envelope": models.Envelope{}, "payload": payload})
	require.NoError(t, err)
	_, err = models.DecodeMessage(brokenEnvelope)
	require.ErrorIs(t, err, models.ErrInvalidEnvelope)
}

func TestDecodePayload(t *testing.T) {
	t.Parallel()
	var payload models.FriendRequestChanged
	require.NoError(t, models.DecodePayload(json.RawMessage(`{"status":"pending"}`), &payload))
	require.Equal(t, models.FriendRequestPending, payload.Status)

	err := models.DecodePayload(json.RawMessage(`"nope"`), &payload)
	require.ErrorIs(t, err, models.ErrInvalidEnvelope)
}

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()
	cursor := models.Cursor{CreatedAt: time.Date(2026, 2, 1, 10, 11, 12, 123456000, time.UTC), ID: "n1"}
	encoded := cursor.Encode()
	require.NotContains(t, encoded, ":")
	require.False(t, strings.ContainsAny(encoded, "+/="))

	decoded, err := models.ParseCursor(encoded)
	require.NoError(t, err)
	require.Equal(t, cursor.ID, decoded.ID)
	require.True(t, cursor.CreatedAt.Equal(decoded.CreatedAt))
}

func TestParseCursorRejectsInvalidValues(t *testing.T) {
	t.Parallel()
	tests := []string{"", "!!!", "MTIz", "MTIzOg", "YWJjOm4x"}
	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			_, err := models.ParseCursor(value)
			require.ErrorIs(t, err, models.ErrInvalidCursor)
		})
	}
}

func TestIDs(t *testing.T) {
	t.Parallel()
	id := models.NewUUID()
	require.True(t, models.IsUUID(id))
	require.False(t, models.IsUUID("not-a-uuid"))
	require.NotEqual(t, id, models.NewUUID())

	traceID := models.NewTraceID()
	require.Len(t, traceID, 32)
	require.NoError(t, models.ValidateEnvelope(models.Envelope{
		EventID: id, EventType: "notification_created", EventVersion: 1, OccurredAt: time.Now().UTC(),
		Producer: "notification_service", TraceID: traceID, CorrelationID: id,
		SchemaRef: models.SchemaRefNotificationCreated,
	}))
}

func TestPreferencesHelpers(t *testing.T) {
	t.Parallel()
	defaults := models.DefaultPreferences()
	for _, kind := range models.AllKinds {
		require.True(t, kind.Valid())
		require.Equal(t, models.KindPreferences{InApp: true, Email: true}, defaults.For(kind))
	}
	require.False(t, models.Kind("system_message").Valid())

	var prefs models.Preferences
	prefs.SetFor(models.KindAppealDecided, models.KindPreferences{InApp: true, Email: false})
	require.False(t, prefs.For(models.KindAppealDecided).Email)
	require.True(t, prefs.For(models.KindAppealDecided).InApp)
	require.Equal(t, models.KindPreferences{}, prefs.For(models.Kind("unknown")))
}

func TestRecipientEligibility(t *testing.T) {
	t.Parallel()
	projection := models.RecipientProjection{Email: "a@example.com", EmailVerified: true, Status: models.AccountActive}
	require.True(t, projection.Eligible())

	projection.EmailVerified = false
	require.False(t, projection.Eligible())

	projection.EmailVerified = true
	projection.Status = models.AccountDeactivated
	require.False(t, projection.Eligible())
}

func TestAPIErrorBody(t *testing.T) {
	t.Parallel()
	encoded, err := json.Marshal(models.ErrNotificationNotFound)
	require.NoError(t, err)
	require.JSONEq(t, `{"error":"notification_not_found","detail":"notification not found"}`, string(encoded))
	require.Equal(t, "notification_not_found: notification not found", models.ErrNotificationNotFound.Error())
}

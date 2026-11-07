package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"notification_service/internal/models"
	"notification_service/internal/repository"
)

// PreferenceService serves and replaces the caller's notification preferences.
type PreferenceService struct {
	preferences repository.PreferenceRepository
	clock       Clock
}

// NewPreferenceService builds a PreferenceService.
func NewPreferenceService(preferences repository.PreferenceRepository, clock Clock) *PreferenceService {
	if clock == nil {
		clock = SystemClock
	}
	return &PreferenceService{preferences: preferences, clock: clock}
}

// Get returns the caller's preferences; users without stored preferences see
// the documented defaults (all kinds enabled in-app and by email).
func (s *PreferenceService) Get(ctx context.Context, userID string) (models.Preferences, error) {
	prefs, err := s.preferences.Get(ctx, userID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return models.DefaultPreferences(), nil
		}
		return models.Preferences{}, fmt.Errorf("service: get notification preferences: %w", err)
	}
	return prefs, nil
}

// Replace stores a full preference replacement and returns the saved value.
// The body must contain exactly the five kinds, each with exactly the in_app
// and email booleans; anything else is a 422 invalid_preferences.
func (s *PreferenceService) Replace(ctx context.Context, userID string, body []byte) (models.Preferences, error) {
	prefs, err := parsePreferences(body)
	if err != nil {
		return models.Preferences{}, err
	}
	saved, err := s.preferences.Upsert(ctx, userID, prefs, s.clock())
	if err != nil {
		return models.Preferences{}, fmt.Errorf("service: replace notification preferences: %w", err)
	}
	return saved, nil
}

// parsePreferences applies the strict full-replacement contract of
// PUT /notification-preferences.
func parsePreferences(body []byte) (models.Preferences, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		var syntaxErr *json.SyntaxError
		if errors.As(err, &syntaxErr) {
			return models.Preferences{}, models.NewAPIError(http.StatusBadRequest, models.CodeInvalidRequest,
				"request body must be valid JSON")
		}
		return models.Preferences{}, invalidPreferences("body must be a JSON object of the five notification kinds")
	}
	if len(top) != len(models.AllKinds) {
		return models.Preferences{}, invalidPreferences("body must contain exactly the five notification kinds")
	}

	prefs := models.Preferences{}
	for _, kind := range models.AllKinds {
		raw, ok := top[string(kind)]
		if !ok {
			return models.Preferences{}, invalidPreferences(fmt.Sprintf("missing notification kind %q", kind))
		}
		kindPrefs, err := parseKindPreferences(raw, kind)
		if err != nil {
			return models.Preferences{}, err
		}
		prefs.SetFor(kind, kindPrefs)
	}

	for key := range top {
		if !models.Kind(key).Valid() {
			return models.Preferences{}, invalidPreferences(fmt.Sprintf("unknown field %q", key))
		}
	}
	return prefs, nil
}

func parseKindPreferences(raw json.RawMessage, kind models.Kind) (models.KindPreferences, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return models.KindPreferences{}, invalidPreferences(fmt.Sprintf("%q must be an object with in_app and email", kind))
	}
	if len(fields) != 2 {
		return models.KindPreferences{}, invalidPreferences(fmt.Sprintf("%q must contain exactly in_app and email", kind))
	}
	inApp, err := parseToggle(fields, "in_app", kind)
	if err != nil {
		return models.KindPreferences{}, err
	}
	email, err := parseToggle(fields, "email", kind)
	if err != nil {
		return models.KindPreferences{}, err
	}
	return models.KindPreferences{InApp: inApp, Email: email}, nil
}

func parseToggle(fields map[string]json.RawMessage, name string, kind models.Kind) (bool, error) {
	raw, ok := fields[name]
	if !ok {
		return false, invalidPreferences(fmt.Sprintf("%q is missing %q", kind, name))
	}
	value, err := strconv.ParseBool(string(raw))
	if err != nil {
		return false, invalidPreferences(fmt.Sprintf("%q.%s must be a boolean", kind, name))
	}
	return value, nil
}

func invalidPreferences(detail string) *models.APIError {
	return models.NewAPIError(http.StatusUnprocessableEntity, models.CodeInvalidPreferences, detail)
}

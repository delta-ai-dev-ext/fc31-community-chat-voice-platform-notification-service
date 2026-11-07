package models

import "net/http"

// Stable machine-readable API error codes (docs/engineering_guide.md §5).
const (
	CodeInvalidCursor         = "invalid_cursor"
	CodeInvalidLimit          = "invalid_limit"
	CodeInvalidNotificationID = "invalid_notification_id"
	CodeInvalidPreferences    = "invalid_preferences"
	CodeInvalidRequest        = "invalid_request"
	CodeUnauthorized          = "unauthorized"
	CodeForbidden             = "forbidden"
	CodeNotificationNotFound  = "notification_not_found"
	CodeNotFound              = "not_found"
	CodeNotReady              = "not_ready"
	CodeInternal              = "internal_error"
)

// APIError is an error that maps onto the documented
// {"error": "<code>", "detail": "<human text>"} response body.
type APIError struct {
	Code   string `json:"error"`
	Detail string `json:"detail"`
	Status int    `json:"-"`
}

// Error implements the error interface.
func (e *APIError) Error() string { return e.Code + ": " + e.Detail }

// NewAPIError builds an APIError with the given status, code and detail.
func NewAPIError(status int, code, detail string) *APIError {
	return &APIError{Code: code, Detail: detail, Status: status}
}

// Common API errors used across the handler layer.
var (
	ErrUnauthorized         = NewAPIError(http.StatusUnauthorized, CodeUnauthorized, "missing, expired, or invalid access token")
	ErrInternal             = NewAPIError(http.StatusInternalServerError, CodeInternal, "unexpected server error")
	ErrNotificationNotFound = NewAPIError(http.StatusNotFound, CodeNotificationNotFound, "notification not found")
)

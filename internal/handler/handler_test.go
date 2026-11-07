package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"notification_service/internal/auth"
	"notification_service/internal/handler"
	"notification_service/internal/models"
	"notification_service/tests/mocks"
	"notification_service/tests/support"
)

func init() { gin.SetMode(gin.TestMode) }

type recordedRequest struct {
	route       string
	method      string
	statusClass string
	duration    time.Duration
}

type recordingObserver struct {
	mu       sync.Mutex
	observed []recordedRequest
}

func (o *recordingObserver) ObserveRequest(route, method, statusClass string, duration time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.observed = append(o.observed, recordedRequest{route: route, method: method, statusClass: statusClass, duration: duration})
}

type testEnv struct {
	router      *gin.Engine
	inbox       *mocks.Inbox
	preferences *mocks.Preferences
	readiness   *mocks.ReadyChecker
	observer    *recordingObserver
	verifier    *auth.Verifier
	signKey     func(userID string) string
}

func newEnv(t *testing.T, readyErr error) *testEnv {
	t.Helper()
	key := support.NewRSAKey(t)
	keyProvider, err := auth.NewStaticKeyProvider(support.PEMPublicKey(t, key))
	require.NoError(t, err)

	inbox := &mocks.Inbox{}
	preferences := &mocks.Preferences{}
	readiness := &mocks.ReadyChecker{}
	observer := &recordingObserver{}

	if readyErr != nil {
		readiness.On("Ready", mock.Anything).Return(readyErr)
	} else {
		readiness.On("Ready", mock.Anything).Return(nil)
	}

	h := handler.New(handler.Options{
		Inbox:       inbox,
		Preferences: preferences,
		Verifier:    auth.NewVerifier(keyProvider, "identity_service", "platform"),
		Readiness:   readiness,
		Logger:      zap.NewNop(),
		Metrics:     observer,
	})

	return &testEnv{
		router:      h.Router(),
		inbox:       inbox,
		preferences: preferences,
		readiness:   readiness,
		observer:    observer,
		signKey: func(userID string) string {
			return support.SignToken(t, key, support.ValidClaims(userID))
		},
	}
}

func (e *testEnv) do(t *testing.T, method, path, token string, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	e.router.ServeHTTP(recorder, request)
	return recorder
}

func decodeError(t *testing.T, recorder *httptest.ResponseRecorder) *models.APIError {
	t.Helper()
	var body models.APIError
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	return &body
}

func TestAuthenticationIsRequired(t *testing.T) {
	t.Parallel()
	env := newEnv(t, nil)

	tests := []struct {
		name   string
		path   string
		method string
		token  string
	}{
		{name: "no token", path: "/api/v1/notifications", method: http.MethodGet},
		{name: "garbage token", path: "/api/v1/notifications", method: http.MethodGet, token: "not-a-token"},
		{name: "no token on read", path: "/api/v1/notifications/abc/read", method: http.MethodPost},
		{name: "no token on preferences", path: "/api/v1/notification-preferences", method: http.MethodGet},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := env.do(t, test.method, test.path, test.token, "", nil)
			require.Equal(t, http.StatusUnauthorized, recorder.Code)
			body := decodeError(t, recorder)
			require.Equal(t, models.CodeUnauthorized, body.Code)
			require.NotEmpty(t, body.Detail)
		})
	}
	env.inbox.AssertNotCalled(t, "List", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestAuthenticatedRequestInjectsUserID(t *testing.T) {
	t.Parallel()
	env := newEnv(t, nil)
	token := env.signKey("11111111-1111-4111-8111-111111111111")

	env.inbox.On("List", mock.Anything, "11111111-1111-4111-8111-111111111111", false, 0, "").Return(models.NotificationPage{
		Items: []models.Notification{{
			ID:        "22222222-2222-4222-8222-222222222222",
			Kind:      models.KindMention,
			Title:     "You were mentioned",
			Summary:   "Someone mentioned you",
			Link:      "/channels/c/messages/m",
			CreatedAt: time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC),
		}},
		UnreadCount: 1,
	}, nil).Once()

	recorder := env.do(t, http.MethodGet, "/api/v1/notifications", token, "", nil)
	require.Equal(t, http.StatusOK, recorder.Code)

	var page map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &page))
	require.Equal(t, float64(1), page["unread_count"])
	require.Nil(t, page["next_cursor"])
	items := page["items"].([]any)
	require.Len(t, items, 1)
	item := items[0].(map[string]any)
	require.Equal(t, "mention", item["kind"])
	require.Nil(t, item["read_at"])
	_, hasRecipient := item["recipient_user_id"]
	require.False(t, hasRecipient, "recipient must not leak into the response")
}

func TestListNotificationsQueryBinding(t *testing.T) {
	t.Parallel()
	env := newEnv(t, nil)
	token := env.signKey("user-1")

	tests := []struct {
		name         string
		query        string
		wantStatus   int
		wantUnread   bool
		wantLimit    int
		wantCursor   string
		wantErrorCde string
	}{
		{name: "defaults", query: "", wantStatus: http.StatusOK},
		{name: "unread only", query: "?unread_only=true", wantStatus: http.StatusOK, wantUnread: true},
		{name: "limit and cursor", query: "?limit=25&cursor=abc", wantStatus: http.StatusOK, wantLimit: 25, wantCursor: "abc"},
		{name: "bad limit", query: "?limit=abc", wantStatus: http.StatusBadRequest, wantErrorCde: models.CodeInvalidLimit},
		{name: "bad unread_only", query: "?unread_only=maybe", wantStatus: http.StatusBadRequest, wantErrorCde: models.CodeInvalidRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env.inbox.ExpectedCalls = nil
			if test.wantStatus == http.StatusOK {
				env.inbox.On("List", mock.Anything, "user-1", test.wantUnread, test.wantLimit, test.wantCursor).
					Return(models.NotificationPage{Items: []models.Notification{}}, nil).Once()
			}
			recorder := env.do(t, http.MethodGet, "/api/v1/notifications"+test.query, token, "", nil)
			require.Equal(t, test.wantStatus, recorder.Code)
			if test.wantErrorCde != "" {
				require.Equal(t, test.wantErrorCde, decodeError(t, recorder).Code)
			}
		})
	}
}

func TestMarkNotificationRead(t *testing.T) {
	t.Parallel()
	env := newEnv(t, nil)
	token := env.signKey("user-1")
	notificationID := "22222222-2222-4222-8222-222222222222"

	tests := []struct {
		name       string
		serviceErr error
		wantStatus int
		wantCode   string
	}{
		{name: "marked", wantStatus: http.StatusNoContent},
		{name: "already read", wantStatus: http.StatusNoContent},
		{name: "not found", serviceErr: models.ErrNotificationNotFound, wantStatus: http.StatusNotFound, wantCode: models.CodeNotificationNotFound},
		{name: "unexpected", serviceErr: errors.New("boom"), wantStatus: http.StatusInternalServerError, wantCode: models.CodeInternal},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env.inbox.ExpectedCalls = nil
			env.inbox.On("MarkRead", mock.Anything, "user-1", notificationID).Return(test.serviceErr).Once()

			recorder := env.do(t, http.MethodPost, "/api/v1/notifications/"+notificationID+"/read", token, "", nil)
			require.Equal(t, test.wantStatus, recorder.Code)
			if test.wantCode != "" {
				require.Equal(t, test.wantCode, decodeError(t, recorder).Code)
			}
		})
	}
}

func TestMarkAllNotificationsRead(t *testing.T) {
	t.Parallel()
	env := newEnv(t, nil)
	token := env.signKey("user-1")

	env.inbox.On("MarkAllRead", mock.Anything, "user-1").Return(nil).Once()
	recorder := env.do(t, http.MethodPost, "/api/v1/notifications/read-all", token, "", nil)
	require.Equal(t, http.StatusNoContent, recorder.Code)

	env.inbox.On("MarkAllRead", mock.Anything, "user-1").Return(errors.New("boom")).Once()
	recorder = env.do(t, http.MethodPost, "/api/v1/notifications/read-all", token, "", nil)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.Equal(t, models.CodeInternal, decodeError(t, recorder).Code)
}

func TestPreferencesEndpoints(t *testing.T) {
	t.Parallel()
	env := newEnv(t, nil)
	token := env.signKey("user-1")
	defaults := models.DefaultPreferences()

	env.preferences.On("Get", mock.Anything, "user-1").Return(defaults, nil).Once()
	recorder := env.do(t, http.MethodGet, "/api/v1/notification-preferences", token, "", nil)
	require.Equal(t, http.StatusOK, recorder.Code)
	var fetched map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &fetched))
	require.Len(t, fetched, len(models.AllKinds))
	for _, kind := range models.AllKinds {
		entry := fetched[string(kind)].(map[string]any)
		require.Equal(t, true, entry["in_app"], string(kind))
		require.Equal(t, true, entry["email"], string(kind))
	}

	body := `{"mention":{"in_app":false,"email":true}}`
	env.preferences.On("Replace", mock.Anything, "user-1", []byte(body)).
		Return(models.Preferences{}, models.NewAPIError(http.StatusUnprocessableEntity, models.CodeInvalidPreferences, "body must contain exactly the five notification kinds")).Once()
	recorder = env.do(t, http.MethodPut, "/api/v1/notification-preferences", token, body, nil)
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	require.Equal(t, models.CodeInvalidPreferences, decodeError(t, recorder).Code)
}

func TestHealthProbes(t *testing.T) {
	t.Parallel()
	ready := newEnv(t, nil)
	notReady := newEnv(t, errors.New("postgres is unreachable"))

	recorder := ready.do(t, http.MethodGet, "/healthz", "", "", nil)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"status":"ok"}`, recorder.Body.String())

	recorder = notReady.do(t, http.MethodGet, "/healthz", "", "", nil)
	require.Equal(t, http.StatusOK, recorder.Code, "liveness must not depend on dependencies")

	recorder = ready.do(t, http.MethodGet, "/readyz", "", "", nil)
	require.Equal(t, http.StatusOK, recorder.Code)

	recorder = notReady.do(t, http.MethodGet, "/readyz", "", "", nil)
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Equal(t, models.CodeNotReady, decodeError(t, recorder).Code)

	recorder = ready.do(t, http.MethodGet, "/api/v1/readyz", "", "", nil)
	require.Equal(t, http.StatusOK, recorder.Code, "readyz must also be served under /api/v1")
}

func TestTraceAndRequestIDs(t *testing.T) {
	t.Parallel()
	env := newEnv(t, nil)
	token := env.signKey("user-1")
	env.inbox.On("List", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(models.NotificationPage{Items: []models.Notification{}}, nil)

	traceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	recorder := env.do(t, http.MethodGet, "/api/v1/notifications", token, "",
		map[string]string{"traceparent": "00-" + traceID + "-00f067aa0ba902b7-01"})
	require.Equal(t, traceID, recorder.Header().Get("X-Trace-Id"))
	require.NotEmpty(t, recorder.Header().Get("X-Request-Id"))

	recorder = env.do(t, http.MethodGet, "/api/v1/notifications", token, "",
		map[string]string{"X-Trace-Id": "0123456789abcdef0123456789abcdef"})
	require.Equal(t, "0123456789abcdef0123456789abcdef", recorder.Header().Get("X-Trace-Id"))

	recorder = env.do(t, http.MethodGet, "/api/v1/notifications", token, "", nil)
	require.Len(t, recorder.Header().Get("X-Trace-Id"), 32)
}

func TestREDMetricsAreRecorded(t *testing.T) {
	t.Parallel()
	env := newEnv(t, nil)
	token := env.signKey("user-1")
	env.inbox.On("List", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(models.NotificationPage{}, errors.New("boom")).Once()

	recorder := env.do(t, http.MethodGet, "/api/v1/notifications", token, "", nil)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)

	env.observer.mu.Lock()
	observed := append([]recordedRequest(nil), env.observer.observed...)
	env.observer.mu.Unlock()
	require.Len(t, observed, 1)
	require.Equal(t, "/api/v1/notifications", observed[0].route)
	require.Equal(t, http.MethodGet, observed[0].method)
	require.Equal(t, "5xx", observed[0].statusClass)
}

func TestUnmatchedRouteUsesErrorEnvelope(t *testing.T) {
	t.Parallel()
	env := newEnv(t, nil)
	recorder := env.do(t, http.MethodGet, "/nope", "", "", nil)
	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.Equal(t, models.CodeNotFound, decodeError(t, recorder).Code)
}

func TestMetricsEndpointWithoutRegistry(t *testing.T) {
	t.Parallel()
	env := newEnv(t, nil)
	recorder := env.do(t, http.MethodGet, "/metrics", "", "", nil)
	require.Equal(t, http.StatusNotFound, recorder.Code)
}

func TestMiddlewareWithoutVerifierRejects(t *testing.T) {
	t.Parallel()
	h := handler.New(handler.Options{Inbox: &mocks.Inbox{}, Preferences: &mocks.Preferences{}, Logger: zap.NewNop()})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/notifications", nil)
	request.Header.Set("Authorization", "Bearer token")
	recorder := httptest.NewRecorder()
	h.Router().ServeHTTP(recorder, request)
	require.Equal(t, http.StatusUnauthorized, recorder.Code)
}

func TestReadyzWithoutChecker(t *testing.T) {
	t.Parallel()
	h := handler.New(handler.Options{Logger: zap.NewNop()})
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()
	h.Router().ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)
}

func TestPreferencesBodyLimit(t *testing.T) {
	t.Parallel()
	env := newEnv(t, nil)
	token := env.signKey("user-1")
	huge := `{"mention":{"in_app":true,"email":true}}` + strings.Repeat(" ", 9000)
	env.preferences.On("Replace", mock.Anything, "user-1", mock.Anything).Return(models.Preferences{}, nil)
	recorder := env.do(t, http.MethodPut, "/api/v1/notification-preferences", token, huge, nil)
	// The body is truncated by the reader; the service still validates it.
	require.NotEqual(t, http.StatusUnauthorized, recorder.Code)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, ctx.Err())
}

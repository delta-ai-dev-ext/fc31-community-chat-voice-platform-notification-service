package provider_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/resend/resend-go/v4"
	"github.com/stretchr/testify/require"

	"notification_service/internal/provider"
)

// httpmock intercepts http.DefaultTransport globally, so these tests run
// sequentially (no t.Parallel) and reset the registry per test.

// recordingCalls captures provider.CallObserver invocations.
type recordingCalls struct {
	mu    sync.Mutex
	calls []call
}

type call struct {
	provider   string
	operation  string
	err        error
	errorClass string
}

func (c *recordingCalls) ObserveCall(name, operation string, _ time.Duration, err error, errorClass string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call{provider: name, operation: operation, err: err, errorClass: errorClass})
}

func (c *recordingCalls) last() call {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[len(c.calls)-1]
}

const resendEndpoint = "https://api.resend.com/emails"

func newSender(t *testing.T) (*provider.ResendSender, *recordingCalls) {
	t.Helper()
	httpmock.Activate()
	httpmock.Reset()
	t.Cleanup(httpmock.DeactivateAndReset)

	observer := &recordingCalls{}
	return provider.NewResendSender("re_test_key", "notifications@example.com", observer), observer
}

func message() provider.EmailMessage {
	return provider.EmailMessage{
		IdempotencyKey: "66666666-6666-4666-8666-666666666666",
		To:             "bob@example.com",
		Subject:        "Alice mentioned you",
		Text:           "You were mentioned in a channel.",
	}
}

func TestResendSenderSendsWithIdempotencyKey(t *testing.T) {
	sender, observer := newSender(t)

	var captured map[string]any
	httpmock.RegisterResponder(http.MethodPost, resendEndpoint, func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(body, &captured); err != nil {
			return nil, err
		}
		require.Equal(t, "66666666-6666-4666-8666-666666666666", request.Header.Get("Idempotency-Key"))
		require.Equal(t, "Bearer re_test_key", request.Header.Get("Authorization"))
		return httpmock.NewJsonResponse(http.StatusOK, map[string]string{"id": "resend-1"})
	})

	messageID, err := sender.Send(context.Background(), message())
	require.NoError(t, err)
	require.Equal(t, "resend-1", messageID)
	require.Equal(t, "notifications@example.com", captured["from"])
	require.Equal(t, []any{"bob@example.com"}, captured["to"])
	require.Equal(t, "Alice mentioned you", captured["subject"])

	require.Equal(t, provider.ProviderResend, observer.last().provider)
	require.Equal(t, provider.OperationSendEmail, observer.last().operation)
	require.Equal(t, "ok", observer.last().errorClass)
	require.NoError(t, observer.last().err)
}

func TestResendSenderClassifiesFailures(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      map[string]any
		wantKind  provider.FailureKind
		wantClass string
		wantRetry bool
	}{
		{
			name:      "rate limited",
			status:    http.StatusTooManyRequests,
			body:      map[string]any{"name": "rate_limit_exceeded", "message": "slow down"},
			wantKind:  provider.FailureRetryable,
			wantClass: "retryable",
			wantRetry: true,
		},
		{
			name:      "server error",
			status:    http.StatusInternalServerError,
			body:      map[string]any{"name": "internal_error", "message": "boom"},
			wantKind:  provider.FailureRetryable,
			wantClass: "retryable",
			wantRetry: true,
		},
		{
			name:      "bad request",
			status:    http.StatusUnprocessableEntity,
			body:      map[string]any{"name": "validation_error", "message": "invalid recipient"},
			wantKind:  provider.FailurePermanent,
			wantClass: "permanent",
			wantRetry: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sender, observer := newSender(t)
			httpmock.RegisterResponder(http.MethodPost, resendEndpoint,
				httpmock.NewJsonResponderOrPanic(test.status, test.body))

			messageID, err := sender.Send(context.Background(), message())
			require.Error(t, err)
			require.Empty(t, messageID)

			var sendErr *provider.SendError
			require.ErrorAs(t, err, &sendErr)
			require.Equal(t, test.wantKind, sendErr.Kind)
			require.Equal(t, test.status, sendErr.StatusCode)
			require.Equal(t, test.wantRetry, provider.IsRetryable(err))
			require.Equal(t, test.wantClass, observer.last().errorClass)
		})
	}
}

func TestResendSenderTreatsTransportErrorsAsRetryable(t *testing.T) {
	sender, observer := newSender(t)
	httpmock.RegisterResponder(http.MethodPost, resendEndpoint,
		httpmock.NewErrorResponder(errors.New("connection reset")))

	_, err := sender.Send(context.Background(), message())
	require.Error(t, err)
	require.True(t, provider.IsRetryable(err))
	require.Equal(t, "retryable", observer.last().errorClass)
}

func TestResendSenderUsesMessageSenderOverride(t *testing.T) {
	sender, _ := newSender(t)

	var captured map[string]any
	httpmock.RegisterResponder(http.MethodPost, resendEndpoint, func(request *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(request.Body)
		_ = json.Unmarshal(body, &captured)
		return httpmock.NewJsonResponse(http.StatusOK, map[string]string{"id": "resend-2"})
	})

	override := message()
	override.From = "safety@example.com"
	_, err := sender.Send(context.Background(), override)
	require.NoError(t, err)
	require.Equal(t, "safety@example.com", captured["from"])
}

func TestResendSenderRetryAfterIsExposed(t *testing.T) {
	sender, _ := newSender(t)
	httpmock.RegisterResponder(http.MethodPost, resendEndpoint, func(*http.Request) (*http.Response, error) {
		response := httpmock.NewStringResponse(http.StatusTooManyRequests, `{"name":"rate_limit_exceeded","message":"slow down"}`)
		response.Header.Set("Retry-After", "42")
		response.Header.Set("Content-Type", "application/json")
		return response, nil
	})

	_, err := sender.Send(context.Background(), message())
	require.Error(t, err)
	require.Equal(t, 42*time.Second, provider.RetryAfter(err))
}

func TestSendErrorFormatting(t *testing.T) {
	sentinel := errors.New("invalid")
	withStatus := &provider.SendError{Kind: provider.FailurePermanent, StatusCode: 422, Err: sentinel}
	require.Contains(t, withStatus.Error(), "status 422")
	require.ErrorIs(t, withStatus, sentinel, "the provider error stays in the unwrap chain")

	withoutStatus := &provider.SendError{Kind: provider.FailureRetryable, Err: errors.New("network")}
	require.Contains(t, withoutStatus.Error(), "retryable")
	require.True(t, provider.IsRetryable(errors.New("unclassified")))
	require.Zero(t, provider.RetryAfter(errors.New("unclassified")))
}

func TestResendSenderWithoutObserver(t *testing.T) {
	httpmock.Activate()
	httpmock.Reset()
	t.Cleanup(httpmock.DeactivateAndReset)
	httpmock.RegisterResponder(http.MethodPost, resendEndpoint,
		httpmock.NewJsonResponderOrPanic(http.StatusOK, map[string]string{"id": "resend-3"}))

	sender := provider.NewResendSenderWithClient(resend.NewClient("re_test_key"), "notifications@example.com", nil)
	messageID, err := sender.Send(context.Background(), message())
	require.NoError(t, err)
	require.Equal(t, "resend-3", messageID)
}

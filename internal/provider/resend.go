// Package provider adapts external notification providers. It owns the Resend
// seam so the domain layer can be tested without network calls.
package provider

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/resend/resend-go/v4"
)

// EmailMessage is one transactional notification email.
type EmailMessage struct {
	// IdempotencyKey deduplicates provider-side retries; it is the notification
	// id, so at-least-once consumption never sends twice.
	IdempotencyKey string
	// From is the verified sender identity; empty uses the adapter default.
	From string
	// To is the recipient address.
	To string
	// Subject and Text are the rendered notification content.
	Subject string
	Text    string
}

// EmailSender delivers notification emails.
type EmailSender interface {
	// Send delivers one email and returns the provider message id.
	Send(ctx context.Context, message EmailMessage) (string, error)
}

// FailureKind classifies a provider failure.
type FailureKind string

// Failure classes.
const (
	// FailureRetryable means the send may succeed later (rate limit, 5xx,
	// transport error).
	FailureRetryable FailureKind = "retryable"
	// FailurePermanent means the request will never succeed (other 4xx).
	FailurePermanent FailureKind = "permanent"
)

// SendError is the classified provider failure returned by EmailSender
// implementations.
type SendError struct {
	Kind       FailureKind
	StatusCode int
	RetryAfter time.Duration
	Err        error
}

// Error implements the error interface.
func (e *SendError) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("provider: %s failure (status %d): %v", e.Kind, e.StatusCode, e.Err)
	}
	return fmt.Sprintf("provider: %s failure: %v", e.Kind, e.Err)
}

// Unwrap exposes the underlying provider error.
func (e *SendError) Unwrap() error { return e.Err }

// IsRetryable reports whether err may be retried. Unclassified errors are
// treated as retryable so a transient outage never drops an email permanently.
func IsRetryable(err error) bool {
	var sendErr *SendError
	if errors.As(err, &sendErr) {
		return sendErr.Kind == FailureRetryable
	}
	return true
}

// RetryAfter exposes a provider-requested back-off, if any.
func RetryAfter(err error) time.Duration {
	var sendErr *SendError
	if errors.As(err, &sendErr) {
		return sendErr.RetryAfter
	}
	return 0
}

// CallObserver records external provider call metrics.
type CallObserver interface {
	// ObserveCall records one outbound provider call.
	ObserveCall(provider, operation string, duration time.Duration, err error, errorClass string)
}

// Provider and operation labels used by the external call metrics.
const (
	// ProviderResend is the provider label of the Resend metrics.
	ProviderResend = "resend"
	// OperationSendEmail is the operation label of the Resend metrics.
	OperationSendEmail = "send_email"
)

// ResendSender is the EmailSender implementation backed by the official Resend
// SDK (github.com/resend/resend-go/v4).
type ResendSender struct {
	emails   resend.EmailsSvc
	from     string
	observed CallObserver
	now      func() time.Time
}

// NewResendSender builds a ResendSender for the given API key and default
// sender address.
func NewResendSender(apiKey, from string, observed CallObserver) *ResendSender {
	return NewResendSenderWithClient(resend.NewClient(apiKey), from, observed)
}

// NewResendSenderWithClient builds a ResendSender over an existing SDK client.
// Tests use it to point the SDK at a mocked HTTP transport.
func NewResendSenderWithClient(client *resend.Client, from string, observed CallObserver) *ResendSender {
	if observed == nil {
		observed = nopCallObserver{}
	}
	return &ResendSender{emails: client.Emails, from: from, observed: observed, now: func() time.Time { return time.Now().UTC() }}
}

// Send implements EmailSender. 429 and 5xx responses are retryable; every other
// 4xx is permanent; the idempotency key is forwarded to Resend.
func (s *ResendSender) Send(ctx context.Context, message EmailMessage) (string, error) {
	from := message.From
	if from == "" {
		from = s.from
	}
	request := &resend.SendEmailRequest{
		From:    from,
		To:      []string{message.To},
		Subject: message.Subject,
		Text:    message.Text,
	}

	started := s.now()
	response, err := s.emails.SendWithOptions(ctx, request, &resend.SendEmailOptions{IdempotencyKey: message.IdempotencyKey})
	duration := s.now().Sub(started)

	if err != nil {
		classified := classifyResendError(err)
		s.observed.ObserveCall(ProviderResend, OperationSendEmail, duration, err, string(classified.Kind))
		return "", classified
	}
	s.observed.ObserveCall(ProviderResend, OperationSendEmail, duration, nil, "ok")
	if response == nil {
		return "", &SendError{Kind: FailureRetryable, Err: errors.New("empty provider response")}
	}
	return response.Id, nil
}

// classifyResendError maps SDK errors onto our retry classification.
func classifyResendError(err error) *SendError {
	var rateLimit *resend.RateLimitError
	if errors.As(err, &rateLimit) || errors.Is(err, resend.ErrRateLimit) {
		return &SendError{Kind: FailureRetryable, StatusCode: http.StatusTooManyRequests, RetryAfter: parseRetryAfter(rateLimit), Err: err}
	}

	var apiErr *resend.ResendError
	if errors.As(err, &apiErr) {
		kind := FailurePermanent
		if apiErr.StatusCode == http.StatusTooManyRequests || apiErr.StatusCode >= http.StatusInternalServerError || apiErr.StatusCode == 0 {
			kind = FailureRetryable
		}
		return &SendError{Kind: kind, StatusCode: apiErr.StatusCode, Err: err}
	}

	// Transport failures and unknown errors are retryable.
	return &SendError{Kind: FailureRetryable, Err: err}
}

func parseRetryAfter(err *resend.RateLimitError) time.Duration {
	if err == nil || err.RetryAfter == "" {
		return 0
	}
	seconds, parseErr := time.ParseDuration(err.RetryAfter + "s")
	if parseErr != nil {
		return 0
	}
	return seconds
}

type nopCallObserver struct{}

func (nopCallObserver) ObserveCall(string, string, time.Duration, error, string) {}

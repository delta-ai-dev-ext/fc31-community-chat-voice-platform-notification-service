package metrics_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"notification_service/internal/metrics"
	"notification_service/internal/models"
)

// gather renders every registered family through the /metrics handler.
func gather(t *testing.T, m *metrics.Metrics) map[string]*dto.MetricFamily {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)

	body, err := io.ReadAll(recorder.Body)
	require.NoError(t, err)

	families := make(map[string]*dto.MetricFamily)
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "# TYPE ") {
			fields := strings.Fields(line)
			families[fields[2]] = &dto.MetricFamily{Name: &fields[2]}
		}
	}
	return families
}

func TestDocumentedMetricsAreRegistered(t *testing.T) {
	t.Parallel()
	m := metrics.New()

	// Wire every observer with realistic label values.
	m.NotificationCreated(models.KindMention)
	m.NotificationRead(models.KindMention, 3)
	m.EmailResult(models.KindMention, models.DeliverySent)
	m.EmailThrottled(models.KindMention)
	m.ObserveRequest("/api/v1/notifications", http.MethodGet, "2xx", 12*time.Millisecond)
	m.ObserveCall("resend", "send_email", 30*time.Millisecond, nil, "ok")
	m.ObserveCall("resend", "send_email", 30*time.Millisecond, errors.New("boom"), "retryable")
	m.EventError(models.TopicSocialEvents, "malformed_event")
	for _, topic := range models.ConsumedTopics {
		m.SetConsumerLag(topic, "notification_service", 4)
	}

	families := gather(t, m)

	required := []string{
		"notification_created_total",
		"notification_emails_sent_total",
		"notification_emails_throttled_total",
		"notification_read_total",
		"http_requests_total",
		"http_request_duration_seconds",
		"kafka_consumer_lag",
		"external_call_duration_seconds",
		"external_call_errors_total",
	}
	for _, name := range required {
		require.Contains(t, families, name, "metric %s must be registered under its documented name", name)
	}

	// The service must not add a namespace prefix to the documented names.
	for name := range families {
		require.False(t, strings.HasPrefix(name, "notification_notification_"), "unexpected prefixed metric %s", name)
	}
}

func TestConsumerLagCoversEveryConsumedTopic(t *testing.T) {
	t.Parallel()
	m := metrics.New()
	for _, topic := range models.ConsumedTopics {
		m.SetConsumerLag(topic, "notification_service", 7)
	}

	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()

	for _, topic := range models.ConsumedTopics {
		require.Contains(t, body, `topic="`+topic+`"`, "lag must be exported for %s", topic)
	}
	require.Contains(t, body, `consumer_group="notification_service"`)
	require.Equal(t, 4, strings.Count(body, "kafka_consumer_lag{"))
}

func TestObserversIgnoreZeroCounts(t *testing.T) {
	t.Parallel()
	m := metrics.New()
	m.NotificationRead(models.KindMention, 0)
	m.NotificationRead(models.KindMention, -4)

	families := gather(t, m)
	_, reported := families["notification_read_total"]
	require.False(t, reported, "no read metric is exported before a real transition")
}

func TestRegistryIsIsolatedPerInstance(t *testing.T) {
	t.Parallel()
	// Building two instances must not panic on duplicate registration.
	require.NotPanics(t, func() {
		first := metrics.New()
		second := metrics.New()
		first.NotificationCreated(models.KindMention)
		second.NotificationCreated(models.KindMention)
	})
	var _ prometheus.Registerer = prometheus.NewRegistry()
}

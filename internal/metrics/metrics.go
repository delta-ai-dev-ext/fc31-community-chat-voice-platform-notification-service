// Package metrics registers the business, RED, consumer-lag and external-call
// metrics of notification_service (docs/architecture.md "Observability").
//
// Metric names are the documented ones exactly; no namespace prefix is applied
// so the exported families match the specification verbatim.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"notification_service/internal/models"
	"notification_service/internal/provider"
)

// Metrics holds every registered collector of the service.
type Metrics struct {
	registry *prometheus.Registry

	notificationCreated *prometheus.CounterVec
	emailsSent          *prometheus.CounterVec
	emailsThrottled     *prometheus.CounterVec
	notificationRead    *prometheus.CounterVec

	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec

	consumerLag *prometheus.GaugeVec

	externalDuration *prometheus.HistogramVec
	externalErrors   *prometheus.CounterVec

	eventErrors *prometheus.CounterVec
}

// New builds and registers every collector on a dedicated registry.
func New() *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{registry: registry}

	m.notificationCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "notification_created_total",
		Help: "Notifications persisted, by kind.",
	}, []string{"kind"})

	m.emailsSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "notification_emails_sent_total",
		Help: "Recorded email outcomes of notifications, by kind and result.",
	}, []string{"kind", "result"})

	m.emailsThrottled = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "notification_emails_throttled_total",
		Help: "Mention emails suppressed by the persisted throttle window, by kind.",
	}, []string{"kind"})

	m.notificationRead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "notification_read_total",
		Help: "Read-state transitions, by kind.",
	}, []string{"kind"})

	m.httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests served, by route, method and status class.",
	}, []string{"route", "method", "status_class"})

	m.httpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency, by route and method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route", "method"})

	m.consumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Consumer lag in messages, by topic and consumer group.",
	}, []string{"topic", "consumer_group"})

	m.externalDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "external_call_duration_seconds",
		Help:    "Outbound provider call latency, by provider and operation.",
		Buckets: prometheus.DefBuckets,
	}, []string{"provider", "operation"})

	m.externalErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "external_call_errors_total",
		Help: "Outbound provider call failures, by provider, operation and error class.",
	}, []string{"provider", "operation", "error_class"})

	m.eventErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "event_processing_errors_total",
		Help: "Consumed events that could not be processed, by topic and error class.",
	}, []string{"topic", "error_class"})

	registry.MustRegister(
		m.notificationCreated,
		m.emailsSent,
		m.emailsThrottled,
		m.notificationRead,
		m.httpRequests,
		m.httpDuration,
		m.consumerLag,
		m.externalDuration,
		m.externalErrors,
		m.eventErrors,
	)
	return m
}

// Handler renders the registered metrics for GET /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// NotificationCreated implements service.Observer.
func (m *Metrics) NotificationCreated(kind models.Kind) {
	m.notificationCreated.WithLabelValues(string(kind)).Inc()
}

// NotificationRead implements service.Observer.
func (m *Metrics) NotificationRead(kind models.Kind, n int) {
	if n <= 0 {
		return
	}
	m.notificationRead.WithLabelValues(string(kind)).Add(float64(n))
}

// EmailResult implements service.Observer.
func (m *Metrics) EmailResult(kind models.Kind, result models.DeliveryStatus) {
	m.emailsSent.WithLabelValues(string(kind), string(result)).Inc()
}

// EmailThrottled implements service.Observer.
func (m *Metrics) EmailThrottled(kind models.Kind) {
	m.emailsThrottled.WithLabelValues(string(kind)).Inc()
}

// ObserveRequest implements handler.HTTPObserver.
func (m *Metrics) ObserveRequest(route, method, statusClass string, duration time.Duration) {
	m.httpRequests.WithLabelValues(route, method, statusClass).Inc()
	m.httpDuration.WithLabelValues(route, method).Observe(duration.Seconds())
}

// SetConsumerLag publishes the current lag of a consumed topic.
func (m *Metrics) SetConsumerLag(topic, consumerGroup string, lag float64) {
	m.consumerLag.WithLabelValues(topic, consumerGroup).Set(lag)
}

// ObserveCall implements provider.CallObserver.
func (m *Metrics) ObserveCall(name, operation string, duration time.Duration, err error, errorClass string) {
	m.externalDuration.WithLabelValues(name, operation).Observe(duration.Seconds())
	if err != nil {
		m.externalErrors.WithLabelValues(name, operation, errorClass).Inc()
	}
}

// EventError counts a consumed event that could not be processed.
func (m *Metrics) EventError(topic, errorClass string) {
	m.eventErrors.WithLabelValues(topic, errorClass).Inc()
}

var _ provider.CallObserver = (*Metrics)(nil)

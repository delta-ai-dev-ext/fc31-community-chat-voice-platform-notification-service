// Package handler exposes the member-facing REST API: the notification inbox,
// notification preferences and the health probes. Handlers stay thin and
// delegate to the service layer.
package handler

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"notification_service/internal/auth"
	"notification_service/internal/models"
)

// Context keys set by the middleware chain.
const (
	// ContextUserID holds the authenticated user id.
	ContextUserID = "user_id"
	// ContextTraceID holds the trace id of the request.
	ContextTraceID = "trace_id"
	// ContextRequestID holds the request id of the request.
	ContextRequestID = "request_id"
	// ContextLogger holds the request-scoped logger.
	ContextLogger = "logger"
)

// Inbox is the inbox use-case surface used by the handlers.
type Inbox interface {
	List(ctx context.Context, recipientUserID string, unreadOnly bool, limit int, cursor string) (models.NotificationPage, error)
	MarkRead(ctx context.Context, recipientUserID, notificationID string) error
	MarkAllRead(ctx context.Context, recipientUserID string) error
}

// Preferences is the preference use-case surface used by the handlers.
type Preferences interface {
	Get(ctx context.Context, userID string) (models.Preferences, error)
	Replace(ctx context.Context, userID string, body []byte) (models.Preferences, error)
}

// TokenVerifier verifies identity_service access tokens locally.
type TokenVerifier interface {
	Verify(ctx context.Context, rawToken string) (auth.Claims, error)
}

// ReadyChecker reports dependency readiness for GET /readyz.
type ReadyChecker interface {
	Ready(ctx context.Context) error
}

// HTTPObserver records RED metrics for served requests.
type HTTPObserver interface {
	ObserveRequest(route, method, statusClass string, duration time.Duration)
}

// Options are the dependencies of the HTTP layer.
type Options struct {
	Inbox       Inbox
	Preferences Preferences
	Verifier    TokenVerifier
	Readiness   ReadyChecker
	Logger      *zap.Logger
	Metrics     HTTPObserver
	// PrometheusHandler renders GET /metrics; a nil handler disables the route
	// with 404.
	PrometheusHandler http.Handler
	// ServiceName is emitted in response headers and logs.
	ServiceName string
}

// Handler builds the gin router of notification_service.
type Handler struct {
	inbox             Inbox
	preferences       Preferences
	verifier          TokenVerifier
	readiness         ReadyChecker
	logger            *zap.Logger
	metrics           HTTPObserver
	prometheusHandler http.Handler
	serviceName       string
}

// New builds a Handler from its dependencies.
func New(opts Options) *Handler {
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics = nopHTTPObserver{}
	}
	serviceName := opts.ServiceName
	if serviceName == "" {
		serviceName = "notification_service"
	}
	return &Handler{
		inbox:             opts.Inbox,
		preferences:       opts.Preferences,
		verifier:          opts.Verifier,
		readiness:         opts.Readiness,
		logger:            logger,
		metrics:           metrics,
		prometheusHandler: opts.PrometheusHandler,
		serviceName:       serviceName,
	}
}

// Router returns the fully wired gin engine of the service.
func (h *Handler) Router() *gin.Engine {
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(h.instrument())
	engine.Use(h.contextMiddleware())

	health := engine.Group("")
	health.GET("/healthz", h.healthz)
	health.GET("/readyz", h.readyz)
	health.GET("/metrics", h.metricsEndpoint)

	group := engine.Group("/api/v1")
	group.GET("/healthz", h.healthz)
	group.GET("/readyz", h.readyz)

	authenticated := group.Group("", h.authenticate())
	authenticated.GET("/notifications", h.listNotifications)
	authenticated.POST("/notifications/:notification_id/read", h.markNotificationRead)
	authenticated.POST("/notifications/read-all", h.markAllNotificationsRead)
	authenticated.GET("/notification-preferences", h.getPreferences)
	authenticated.PUT("/notification-preferences", h.replacePreferences)

	engine.NoRoute(h.notFound)
	return engine
}

// respondError maps an error onto the documented {"error","detail"} body.
func (h *Handler) respondError(c *gin.Context, err error) {
	var apiErr *models.APIError
	if errors.As(err, &apiErr) {
		c.JSON(apiErr.Status, apiErr)
		return
	}
	loggerFrom(c, h.logger).Error("request failed", zap.String("operation", c.FullPath()), zap.Error(err))
	c.JSON(http.StatusInternalServerError, models.ErrInternal)
}

// userID returns the authenticated user id of the request.
func userID(c *gin.Context) string {
	return c.GetString(ContextUserID)
}

// loggerFrom returns the request-scoped logger carrying trace and request ids.
func loggerFrom(c *gin.Context, fallback *zap.Logger) *zap.Logger {
	if value, ok := c.Get(ContextLogger); ok {
		if logger, ok := value.(*zap.Logger); ok && logger != nil {
			return logger
		}
	}
	return fallback
}

type nopHTTPObserver struct{}

func (nopHTTPObserver) ObserveRequest(string, string, string, time.Duration) {}

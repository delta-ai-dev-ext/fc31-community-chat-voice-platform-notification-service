package handler

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"notification_service/internal/auth"
	"notification_service/internal/models"
)

var (
	traceParentPattern = regexp.MustCompile(`^[0-9]{2}-([0-9a-f]{32})-[0-9a-f]{16}-[0-9]{2}$`)
	traceIDPattern     = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// contextMiddleware propagates the incoming trace id (traceparent or
// X-Trace-Id) and mints one when the caller did not provide it, then attaches a
// request-scoped logger.
func (h *Handler) contextMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := traceIDFromHeaders(c.Request)
		requestID := strings.TrimSpace(c.GetHeader("X-Request-Id"))
		if requestID == "" {
			requestID = models.NewUUID()
		}

		logger := h.logger.With(
			zap.String("service_name", h.serviceName),
			zap.String("trace_id", traceID),
			zap.String("request_id", requestID),
			zap.String("correlation_id", requestID),
			zap.String("operation", c.Request.Method+" "+c.Request.URL.Path),
		)

		c.Set(ContextTraceID, traceID)
		c.Set(ContextRequestID, requestID)
		c.Set(ContextLogger, logger)
		c.Header("X-Trace-Id", traceID)
		c.Header("X-Request-Id", requestID)

		c.Next()
	}
}

// authenticate verifies the bearer access token locally and injects the caller.
func (h *Handler) authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.verifier == nil {
			h.respondError(c, models.ErrUnauthorized)
			c.Abort()
			return
		}
		raw, err := auth.BearerToken(c.GetHeader("Authorization"))
		if err != nil {
			h.respondError(c, models.ErrUnauthorized)
			c.Abort()
			return
		}
		claims, err := h.verifier.Verify(c.Request.Context(), raw)
		if err != nil {
			loggerFrom(c, h.logger).Warn("access token rejected", zap.Error(err))
			h.respondError(c, models.ErrUnauthorized)
			c.Abort()
			return
		}
		c.Set(ContextUserID, claims.UserID)
		c.Set("session_id", claims.SessionID)
		c.Next()
	}
}

// instrument records the RED metrics of every served request.
func (h *Handler) instrument() gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()

		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		h.metrics.ObserveRequest(route, c.Request.Method, statusClass(c.Writer.Status()), time.Since(started))

		logger := loggerFrom(c, h.logger)
		logger.Info("request completed",
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("route", route),
			zap.Duration("duration", time.Since(started)),
		)
	}
}

func statusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}

func traceIDFromHeaders(request *http.Request) string {
	if match := traceParentPattern.FindStringSubmatch(strings.TrimSpace(request.Header.Get("traceparent"))); match != nil {
		if match[1] != "00000000000000000000000000000000" {
			return match[1]
		}
	}
	header := strings.ToLower(strings.TrimSpace(request.Header.Get("X-Trace-Id")))
	if traceIDPattern.MatchString(header) {
		return header
	}
	return models.NewTraceID()
}

// parseBoolQuery parses an optional boolean query parameter.
func parseBoolQuery(c *gin.Context, name string) (bool, error) {
	raw := strings.TrimSpace(c.Query(name))
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, models.NewAPIError(http.StatusBadRequest, models.CodeInvalidRequest,
			name+" must be a boolean")
	}
	return value, nil
}

// parseIntQuery parses an optional integer query parameter.
func parseIntQuery(c *gin.Context, name string) (int, error) {
	raw := strings.TrimSpace(c.Query(name))
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, models.NewAPIError(http.StatusBadRequest, models.CodeInvalidLimit,
			name+" must be an integer")
	}
	return value, nil
}

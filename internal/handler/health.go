package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"notification_service/internal/models"
)

// healthz serves GET /healthz. Liveness only reports process responsiveness and
// never fails because a dependency is degraded.
func (h *Handler) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// readyz serves GET /readyz: PostgreSQL and Kafka must be reachable and the
// process must not be shutting down.
func (h *Handler) readyz(c *gin.Context) {
	if h.readiness == nil {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
		return
	}
	if err := h.readiness.Ready(c.Request.Context()); err != nil {
		loggerFrom(c, h.logger).Warn("readiness check failed", zap.Error(err))
		c.JSON(http.StatusServiceUnavailable, models.NewAPIError(http.StatusServiceUnavailable, models.CodeNotReady, err.Error()))
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// metricsEndpoint serves GET /metrics for the platform scraper.
func (h *Handler) metricsEndpoint(c *gin.Context) {
	if h.prometheusHandler == nil {
		c.Status(http.StatusNotFound)
		return
	}
	h.prometheusHandler.ServeHTTP(c.Writer, c.Request)
}

// notFound serves unmatched routes with the documented error body.
func (h *Handler) notFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, models.NewAPIError(http.StatusNotFound, models.CodeNotFound, "route not found"))
}

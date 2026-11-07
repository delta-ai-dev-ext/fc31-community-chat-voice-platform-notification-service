package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// listNotifications serves GET /notifications.
func (h *Handler) listNotifications(c *gin.Context) {
	unreadOnly, err := parseBoolQuery(c, "unread_only")
	if err != nil {
		h.respondError(c, err)
		return
	}
	limit, err := parseIntQuery(c, "limit")
	if err != nil {
		h.respondError(c, err)
		return
	}

	page, err := h.inbox.List(c.Request.Context(), userID(c), unreadOnly, limit, c.Query("cursor"))
	if err != nil {
		h.respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}

// markNotificationRead serves POST /notifications/{notification_id}/read.
func (h *Handler) markNotificationRead(c *gin.Context) {
	if err := h.inbox.MarkRead(c.Request.Context(), userID(c), c.Param("notification_id")); err != nil {
		h.respondError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// markAllNotificationsRead serves POST /notifications/read-all.
func (h *Handler) markAllNotificationsRead(c *gin.Context) {
	if err := h.inbox.MarkAllRead(c.Request.Context(), userID(c)); err != nil {
		h.respondError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

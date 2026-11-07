package handler

import (
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"notification_service/internal/models"
)

// maxPreferencesBody bounds the preference replacement body.
const maxPreferencesBody = 8 << 10

// getPreferences serves GET /notification-preferences.
func (h *Handler) getPreferences(c *gin.Context) {
	prefs, err := h.preferences.Get(c.Request.Context(), userID(c))
	if err != nil {
		h.respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, prefs)
}

// replacePreferences serves PUT /notification-preferences (full replacement).
func (h *Handler) replacePreferences(c *gin.Context) {
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, maxPreferencesBody))
	if err != nil {
		h.respondError(c, models.NewAPIError(http.StatusBadRequest, models.CodeInvalidRequest, "request body could not be read"))
		return
	}

	saved, err := h.preferences.Replace(c.Request.Context(), userID(c), body)
	if err != nil {
		h.respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, saved)
}

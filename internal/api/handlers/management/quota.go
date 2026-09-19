package management

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/stdlibhttp"
)

// Quota exceeded toggles
func (h *Handler) GetSwitchProject(c *web.Context) {
	c.JSON(200, web.H{"switch-project": h.cfg.QuotaExceeded.SwitchProject})
}
func (h *Handler) PutSwitchProject(c *web.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.QuotaExceeded.SwitchProject = v })
}

func (h *Handler) GetSwitchPreviewModel(c *web.Context) {
	c.JSON(200, web.H{"switch-preview-model": h.cfg.QuotaExceeded.SwitchPreviewModel})
}
func (h *Handler) PutSwitchPreviewModel(c *web.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.QuotaExceeded.SwitchPreviewModel = v })
}

// ResetQuota clears quota/cooldown routing state for one auth index.
func (h *Handler) ResetQuota(c *web.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, web.H{"error": "core auth manager unavailable"})
		return
	}

	var req struct {
		AuthIndex string `json:"auth_index"`
	}
	if errBindJSON := c.ShouldBindJSON(&req); errBindJSON != nil {
		c.JSON(http.StatusBadRequest, web.H{"error": "invalid request body"})
		return
	}

	authIndex := strings.TrimSpace(req.AuthIndex)
	if authIndex == "" {
		c.JSON(http.StatusBadRequest, web.H{"error": "auth_index is required"})
		return
	}

	auth := h.authByIndex(authIndex)
	if auth == nil {
		c.JSON(http.StatusNotFound, web.H{"error": "auth not found"})
		return
	}

	updated, models, errReset := h.authManager.ResetQuota(c.Request.Context(), auth.ID)
	if errReset != nil {
		c.JSON(http.StatusInternalServerError, web.H{"error": fmt.Sprintf("failed to reset quota: %v", errReset)})
		return
	}
	if updated == nil {
		c.JSON(http.StatusNotFound, web.H{"error": "auth not found"})
		return
	}
	updated.EnsureIndex()

	c.JSON(http.StatusOK, web.H{
		"status":     "ok",
		"auth_index": updated.Index,
		"models":     models,
	})
}

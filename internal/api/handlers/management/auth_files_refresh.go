package management

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/stdlibhttp"
)

// RefreshAuthFiles triggers active refresh for a single auth file or all auth files.
// Accepts query parameters (?all=true, ?name=file.json) or JSON body ({"all": true, "name": "file.json"}).
func (h *Handler) RefreshAuthFiles(c *web.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, web.H{"error": "core auth manager unavailable"})
		return
	}

	var req struct {
		Name      string `json:"name"`
		AuthIndex string `json:"auth_index"`
		All       bool   `json:"all"`
	}
	if c.Request.Body != nil && c.Request.ContentLength != 0 {
		if errBind := c.ShouldBindJSON(&req); errBind != nil && !errors.Is(errBind, io.EOF) {
			c.JSON(http.StatusBadRequest, web.H{"error": "invalid request body: " + errBind.Error()})
			return
		}
	}
	if c.Query("all") == "true" {
		req.All = true
	}
	if queryName := strings.TrimSpace(c.Query("name")); queryName != "" && req.Name == "" {
		req.Name = queryName
	}
	if queryAuthIndex := strings.TrimSpace(c.Query("auth_index")); queryAuthIndex != "" && req.AuthIndex == "" {
		req.AuthIndex = queryAuthIndex
	}

	ctx := c.Request.Context()

	if req.All {
		results := h.authManager.ForceRefreshAll(ctx)
		c.JSON(http.StatusOK, web.H{
			"ok":      true,
			"results": results,
		})
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		c.JSON(http.StatusBadRequest, web.H{"error": "name or all=true is required"})
		return
	}

	targetAuth, ok := h.lookupAuthFile(name, req.AuthIndex)
	if !ok || targetAuth == nil {
		c.JSON(http.StatusNotFound, web.H{"error": "auth file not found"})
		return
	}

	refreshed, err := h.authManager.ForceRefreshAuth(ctx, targetAuth.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, web.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, web.H{
		"ok":   true,
		"auth": refreshed,
	})
}

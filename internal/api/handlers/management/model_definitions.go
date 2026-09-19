package management

import (
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/stdlibhttp"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
)

// GetStaticModelDefinitions returns static model metadata for a given channel.
// Channel is provided via path param (:channel) or query param (?channel=...).
func (h *Handler) GetStaticModelDefinitions(c *web.Context) {
	channel := strings.TrimSpace(c.Param("channel"))
	if channel == "" {
		channel = strings.TrimSpace(c.Query("channel"))
	}
	if channel == "" {
		c.JSON(http.StatusBadRequest, web.H{"error": "channel is required"})
		return
	}

	models := registry.GetStaticModelDefinitionsByChannel(channel)
	if models == nil {
		c.JSON(http.StatusBadRequest, web.H{"error": "unknown channel", "channel": channel})
		return
	}

	c.JSON(http.StatusOK, web.H{
		"channel": strings.ToLower(strings.TrimSpace(channel)),
		"models":  models,
	})
}

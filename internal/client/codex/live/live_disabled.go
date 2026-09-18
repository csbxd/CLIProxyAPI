//go:build !codex_live

// Package live provides disabled Codex Live route compatibility for builds that
// do not include the optional WebRTC implementation.
package live

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

const (
	ClientSecretSessionContextKey   = "codexLiveClientSecretSession"
	ClientSecretPrincipalContextKey = "codexLiveClientSecretPrincipal"
	disabledClientSecret            = "csk_codex_live_disabled"
)

// ClientSecretAuthorization preserves the route middleware API when Codex Live
// is excluded from the build.
type ClientSecretAuthorization struct {
	Principal       string
	IssuerPrincipal string
	IssuerProvider  string
	Session         json.RawMessage
}

// Handler preserves the API server's route and lifecycle surface without
// pulling in the optional WebRTC implementation.
type Handler struct{}

func NewHandler(*auth.Manager, *config.Config) *Handler { return &Handler{} }
func (*Handler) UpdateConfig(*config.Config) error      { return nil }
func (*Handler) Close()                                 {}

func (*Handler) Handle(c *gin.Context) {
	writeDisabled(c, http.StatusServiceUnavailable, "Codex Live is disabled in this build", "codex_live_disabled")
}

func (*Handler) HandleSideband(c *gin.Context) {
	if c != nil {
		c.Header("Upgrade", "websocket")
	}
	writeDisabled(c, http.StatusUpgradeRequired, "Codex Live is disabled in this build", "codex_live_disabled")
}

func (*Handler) HandleRealtimeWebsocket(c *gin.Context) {
	if c != nil {
		c.Header("Upgrade", "websocket")
	}
	writeDisabled(c, http.StatusUpgradeRequired, "Codex Live is disabled in this build", "codex_live_disabled")
}

func (*Handler) CreateClientSecret(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"value":      disabledClientSecret,
		"expires_at": int64(0),
		"session":    json.RawMessage(`{"type":"realtime","model":"gpt-realtime"}`),
	})
}

func (*Handler) CreateLegacySession(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"id":         "sess_codex_live_disabled",
		"object":     "realtime.session",
		"expires_at": int64(0),
	})
}

func (*Handler) HandleTranscriptionSession(c *gin.Context) {
	writeDisabled(c, http.StatusNotImplemented, "Codex Live transcription sessions are disabled in this build", "codex_live_disabled")
}

func (*Handler) HandleTranslation(c *gin.Context) {
	writeDisabled(c, http.StatusNotImplemented, "Codex Live translation sessions are disabled in this build", "codex_live_disabled")
}

func (*Handler) HandleSIPControl(c *gin.Context) {
	writeDisabled(c, http.StatusNotImplemented, "Codex Live SIP control is disabled in this build", "codex_live_disabled")
}

func (*Handler) HandleHangup(c *gin.Context) {
	writeDisabled(c, http.StatusNotFound, "Realtime call not found", "realtime_call_not_found")
}

// AuthenticateClientSecret keeps the middleware contract. The disabled build
// recognizes only its deterministic compatibility secret so route tests and
// clients receive the same authentication shape without a live session store.
func (*Handler) AuthenticateClientSecret(request *http.Request) (ClientSecretAuthorization, bool, error) {
	if request == nil {
		return ClientSecretAuthorization{}, false, nil
	}
	token := strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
	if token == "" {
		return ClientSecretAuthorization{}, false, nil
	}
	if token == disabledClientSecret {
		return ClientSecretAuthorization{
			Principal:       "codex-live-disabled",
			IssuerPrincipal: "codex-live-disabled",
			IssuerProvider:  "realtime-client-secret",
			Session:         json.RawMessage(`{"type":"realtime","model":"gpt-realtime"}`),
		}, true, nil
	}
	if strings.HasPrefix(token, "csk_") {
		return ClientSecretAuthorization{}, true, errors.New("Realtime client secret is invalid or expired")
	}
	return ClientSecretAuthorization{}, false, nil
}

func writeDisabled(c *gin.Context, status int, message, code string) {
	if c == nil {
		return
	}
	c.JSON(status, gin.H{"error": gin.H{
		"message": message,
		"type":    "not_supported_error",
		"param":   nil,
		"code":    code,
	}})
}

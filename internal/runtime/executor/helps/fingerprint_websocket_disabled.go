//go:build !codex_rs

package helps

import (
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func newCodexFingerprintWebSocketDialer(cfg *config.Config, auth *cliproxyauth.Auth) (WebSocketDialer, error) {
	return &gorillaFingerprintWebSocketDialer{dialer: NewProxyAwareWebSocketDialer(cfg, auth)}, nil
}

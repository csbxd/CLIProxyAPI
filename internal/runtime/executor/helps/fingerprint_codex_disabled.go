//go:build !codex_rs

package helps

import (
	"context"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func newCodexFingerprintHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) (*http.Client, error) {
	return newUTLSFingerprintHTTPClient(ctx, cfg, auth, FingerprintCodex), nil
}

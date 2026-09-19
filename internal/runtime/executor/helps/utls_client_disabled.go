//go:build !utls

package helps

import (
	"context"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func newUTLSFingerprintHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, _ Fingerprint) *http.Client {
	return NewProxyAwareHTTPClient(ctx, cfg, auth, 0)
}

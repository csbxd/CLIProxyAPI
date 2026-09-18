//go:build !utls

package helps

import (
	"context"
	"net/http"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

// NewUtlsHTTPClient returns a standard proxy-aware HTTP client when the uTLS
// fingerprint transport is not enabled.
func NewUtlsHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	if ctx == nil {
		ctx = context.Background()
	}
	return NewProxyAwareHTTPClient(ctx, cfg, auth, timeout)
}

package helps

import (
	"context"
	"fmt"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

// Fingerprint identifies the desired client profile, not an enabled backend.
// Build tags determine which implementation is available for that profile.
type Fingerprint uint8

const (
	FingerprintNone Fingerprint = iota
	FingerprintCodex
	FingerprintClaude
)

// NewFingerprintHTTPClient selects a transport from the profile and build tags.
// Codex OAuth prefers codex_rs when enabled; otherwise utls selects the provider's
// TLS profile. Without either applicable tag, the client uses standard HTTP.
// FingerprintNone uses the ordinary proxy-aware client factory. Explicit context
// transport overrides retain their existing priority. No request timeout is
// installed; cancellation follows the incoming request context.
func NewFingerprintHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, fingerprint Fingerprint) (*http.Client, error) {
	switch fingerprint {
	case FingerprintNone:
		return NewProxyAwareHTTPClient(ctx, cfg, auth, 0), nil
	case FingerprintCodex:
		return newCodexFingerprintHTTPClient(ctx, cfg, auth)
	case FingerprintClaude:
		return newUTLSFingerprintHTTPClient(ctx, cfg, auth, fingerprint), nil
	default:
		return nil, fmt.Errorf("unsupported HTTP fingerprint profile: %d", fingerprint)
	}
}

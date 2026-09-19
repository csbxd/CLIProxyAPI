//go:build codex_rs

package helps

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/csbxd/gocodex/httpclient"
	internalcache "github.com/router-for-me/CLIProxyAPI/v7/internal/cache"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
)

type codexRSTransportKey struct {
	proxyURL string
	codexCA  string
	sslCA    string
}

type codexRSTransportCache struct {
	mu      sync.Mutex
	entries *internalcache.BoundedLRU[codexRSTransportKey, *httpclient.Transport]
}

func newCodexRSTransportCache(capacity int) *codexRSTransportCache {
	return &codexRSTransportCache{
		// Close cancels active SSE requests, so eviction only releases our reference.
		// gocodex keeps active responses alive and cleans up the pool after GC.
		entries: internalcache.NewBoundedLRU[codexRSTransportKey, *httpclient.Transport](capacity, nil),
	}
}

var codexRSTransports = newCodexRSTransportCache(DefaultTransportCacheCapacity)

func (c *codexRSTransportCache) get(key codexRSTransportKey, options httpclient.Options) (*httpclient.Transport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if transport, ok := c.entries.Get(key); ok {
		return transport, nil
	}
	transport, err := httpclient.NewTransport(options)
	if err != nil {
		// Configuration errors must remain retryable after a CA/proxy correction.
		return nil, fmt.Errorf("codex-rs transport: %w", err)
	}
	return c.entries.GetOrAdd(key, func() *httpclient.Transport { return transport }), nil
}

// newCodexFingerprintHTTPClient uses the Codex Rust HTTP SDK for OAuth inference requests.
// API-key clients retain their existing transport. Neither the client nor the
// transport sets a timeout; cancellation follows the incoming request context.
func newCodexFingerprintHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) (*http.Client, error) {
	if auth == nil || auth.AuthKind() != cliproxyauth.AuthKindOAuth || strings.TrimSpace(auth.Attributes[cliproxyauth.AttributeAPIKey]) != "" {
		return newUTLSFingerprintHTTPClient(ctx, cfg, auth, FingerprintCodex), nil
	}

	proxyURL := strings.TrimSpace(auth.ProxyURL)
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}
	// Preserve the host's injected transport only when no proxy is configured.
	// A per-auth provider may inject a standard Go proxy transport; explicit proxy
	// settings must instead configure the Rust transport to retain its TLS stack.
	if proxyURL == "" && ctx != nil {
		if transport, ok := ctx.Value("cliproxy.roundtripper").(http.RoundTripper); ok && transport != nil {
			return &http.Client{Transport: transport}, nil
		}
	}

	setting, err := proxyutil.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("codex-rs proxy: %w", err)
	}
	options := httpclient.Options{}
	switch setting.Mode {
	case proxyutil.ModeDirect:
		options.NoProxy = true
		proxyURL = "direct"
	case proxyutil.ModeProxy:
		options.ProxyURL = setting.URL.String()
		proxyURL = options.ProxyURL
	}
	key := codexRSTransportKey{
		proxyURL: proxyURL,
		codexCA:  os.Getenv("CODEX_CA_CERTIFICATE"),
		sslCA:    os.Getenv("SSL_CERT_FILE"),
	}
	transport, err := codexRSTransports.get(key, options)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport}, nil
}

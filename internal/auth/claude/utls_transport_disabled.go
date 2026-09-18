//go:build !utls

package claude

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	log "github.com/sirupsen/logrus"
)

type claudeRefreshHandshakeTimeoutContextKey struct{}

// NewAnthropicHttpClient uses the standard TLS stack when the uTLS fingerprint
// transport is not enabled.
func NewAnthropicHttpClient(cfg *config.SDKConfig) *http.Client {
	client := &http.Client{}
	if cfg == nil || strings.TrimSpace(cfg.ProxyURL) == "" {
		return client
	}
	rawProxy := strings.TrimSpace(cfg.ProxyURL)
	if strings.EqualFold(rawProxy, "direct") || strings.EqualFold(rawProxy, "none") {
		client.Transport = directHTTPTransport()
		return client
	}
	proxyURL, errParse := url.Parse(rawProxy)
	if errParse != nil || proxyURL.Host == "" {
		log.Errorf("failed to configure Claude HTTP proxy for %q", rawProxy)
		return client
	}
	if !strings.EqualFold(proxyURL.Scheme, "http") && !strings.EqualFold(proxyURL.Scheme, "https") {
		log.Warnf("Claude standard HTTP transport does not support proxy scheme %q without uTLS", proxyURL.Scheme)
		return client
	}
	transport := directHTTPTransport()
	transport.Proxy = http.ProxyURL(proxyURL)
	client.Transport = transport
	return client
}

func directHTTPTransport() *http.Transport {
	if transport, ok := http.DefaultTransport.(*http.Transport); ok && transport != nil {
		clone := transport.Clone()
		clone.Proxy = nil
		return clone
	}
	return &http.Transport{Proxy: nil}
}

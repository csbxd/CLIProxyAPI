//go:build codex_rs

package helps

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/csbxd/gocodex/httpclient"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func codexRSOAuth(proxy string) *cliproxyauth.Auth {
	return &cliproxyauth.Auth{Provider: "codex", ProxyURL: proxy, Metadata: map[string]any{"access_token": "fixture-token"}}
}

func clearCodexRSEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy", "CODEX_CA_CERTIFICATE", "SSL_CERT_FILE"} {
		t.Setenv(key, "")
	}
}

type codexRSTestTransport struct{}

func (*codexRSTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("injected")), Request: r}, nil
}

func TestCodexRSTransportSelection(t *testing.T) {
	clearCodexRSEnvironment(t)
	client, err := NewFingerprintHTTPClient(t.Context(), nil, codexRSOAuth("direct"), FingerprintCodex)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := client.Transport.(*httpclient.Transport); !ok || client.Timeout != 0 {
		t.Fatalf("OAuth client = %T, timeout = %v", client.Transport, client.Timeout)
	}
	for _, fingerprint := range []Fingerprint{FingerprintNone, FingerprintClaude} {
		other, errClient := NewFingerprintHTTPClient(t.Context(), nil, codexRSOAuth("direct"), fingerprint)
		if errClient != nil {
			t.Fatal(errClient)
		}
		if _, ok := other.Transport.(*httpclient.Transport); ok {
			t.Fatalf("non-Codex profile %d selected the Rust transport", fingerprint)
		}
	}
	for _, auth := range []*cliproxyauth.Auth{
		nil,
		{Attributes: map[string]string{"api_key": "fixture"}},
		{Attributes: map[string]string{"auth_kind": "apikey"}, Metadata: map[string]any{"access_token": "fixture"}},
		{Attributes: map[string]string{"auth_kind": "oauth", "api_key": "fixture"}},
	} {
		client, err := NewFingerprintHTTPClient(t.Context(), nil, auth, FingerprintCodex)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := client.Transport.(*httpclient.Transport); ok {
			t.Fatal("non-OAuth client selected Rust transport")
		}
	}
	injected := &codexRSTestTransport{}
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", injected)
	client, err = NewFingerprintHTTPClient(ctx, nil, codexRSOAuth(""), FingerprintCodex)
	if err != nil || client.Transport != injected {
		t.Fatalf("explicit context transport was lost: %v", err)
	}
}

func TestCodexRSProxyPriority(t *testing.T) {
	clearCodexRSEnvironment(t)
	proxy := func(marker string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !r.URL.IsAbs() || r.URL.Host != "codex.invalid" {
				t.Errorf("unexpected proxy request: %s", r.URL)
			}
			_, _ = io.WriteString(w, marker)
		}))
	}
	accountProxy, globalProxy := proxy("account"), proxy("global")
	defer accountProxy.Close()
	defer globalProxy.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "direct") }))
	defer origin.Close()
	t.Setenv("HTTP_PROXY", globalProxy.URL)
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", &codexRSTestTransport{})
	cfg := &config.Config{}
	cfg.ProxyURL = globalProxy.URL
	for _, tc := range []struct {
		name, proxy, target, want string
		cfg                       *config.Config
	}{
		{"account", accountProxy.URL, "http://codex.invalid/responses", "account", cfg},
		{"global", "", "http://codex.invalid/responses", "global", cfg},
		{"direct", " DiReCt ", origin.URL, "direct", cfg},
		{"none", "none", origin.URL, "direct", cfg},
		{"context", "", "http://codex.invalid/responses", "injected", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewFingerprintHTTPClient(ctx, tc.cfg, codexRSOAuth(tc.proxy), FingerprintCodex)
			if err != nil {
				t.Fatal(err)
			}
			request, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, tc.target, nil)
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, errRead := io.ReadAll(response.Body)
			if errClose := response.Body.Close(); errClose != nil {
				t.Error(errClose)
			}
			if errRead != nil || string(body) != tc.want {
				t.Fatalf("body = %q, error = %v", body, errRead)
			}
		})
	}
	for _, proxyURL := range []string{"ftp://user:secret@host", "http://user:secret@host/path", "http://user:secret@host:invalid"} {
		_, err := NewFingerprintHTTPClient(ctx, cfg, codexRSOAuth(proxyURL), FingerprintCodex)
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("invalid proxy must fail without disclosing credentials: %v", err)
		}
	}
}

func TestCodexRSTransportCacheReuseAndConfigurationRetry(t *testing.T) {
	clearCodexRSEnvironment(t)
	cache := newCodexRSTransportCache(2)
	key := codexRSTransportKey{proxyURL: "direct"}
	var transports [16]*httpclient.Transport
	var wg sync.WaitGroup
	for i := range transports {
		wg.Go(func() {
			var err error
			transports[i], err = cache.get(key, httpclient.Options{NoProxy: true})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	for _, transport := range transports {
		if transport == nil || transport != transports[0] {
			t.Fatal("concurrent callers did not reuse the same native pool")
		}
	}
	defer func() {
		if err := transports[0].Close(); err != nil {
			t.Error(err)
		}
	}()
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	t.Setenv("CODEX_CA_CERTIFICATE", caPath)
	key.codexCA = caPath
	if _, err := cache.get(key, httpclient.Options{NoProxy: true}); err == nil {
		t.Fatal("missing CA accepted")
	}
	if cache.entries.Len() != 1 {
		t.Fatal("failed construction occupied a cache entry")
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	transport, err := cache.get(key, httpclient.Options{NoProxy: true})
	if err != nil || transport == transports[0] {
		t.Fatalf("CA correction did not create a separate pool: %v", err)
	}
	if errClose := transport.Close(); errClose != nil {
		t.Error(errClose)
	}
}

func TestCodexRSEvictionPreservesActiveStream(t *testing.T) {
	clearCodexRSEnvironment(t)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "first")
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(w, "last")
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer unblock()
	cache := newCodexRSTransportCache(1)
	first, err := cache.get(codexRSTransportKey{proxyURL: "first"}, httpclient.Options{NoProxy: true})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	response, err := (&http.Client{Transport: first}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if errClose := response.Body.Close(); errClose != nil {
			t.Error(errClose)
		}
	}()
	chunk := make([]byte, 5)
	if _, err := io.ReadFull(response.Body, chunk); err != nil || string(chunk) != "first" {
		t.Fatalf("first chunk = %q, %v", chunk, err)
	}
	if _, err := cache.get(codexRSTransportKey{proxyURL: "second"}, httpclient.Options{NoProxy: true}); err != nil {
		t.Fatal(err)
	}
	if cache.entries.Len() != 1 {
		t.Fatal("cache exceeded capacity")
	}
	unblock()
	tail, err := io.ReadAll(response.Body)
	if err != nil || string(tail) != "last" {
		t.Fatalf("eviction interrupted active response: %q, %v", tail, err)
	}
}

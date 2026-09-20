//go:build codex_rs

package helps

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/csbxd/gocodex/httpclient"
)

func TestCodexRSAutomaticHTTPPreservesConnectionFailure(t *testing.T) {
	clearCodexRSEnvironment(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	client, err := NewFingerprintHTTPClient(t.Context(), nil, codexRSOAuth(""), FingerprintCodex)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+address, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	var native *httpclient.Error
	if !errors.As(err, &native) || !strings.Contains(strings.ToLower(native.Message), "connection refused") {
		t.Fatalf("automatic OAuth transport lost the dial error: %T %v", err, err)
	}
}

func TestCodexRSWebSocketHandshakeTimeoutPreservesNetError(t *testing.T) {
	clearCodexRSEnvironment(t)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	for _, proxy := range []string{"", "direct"} {
		dialer, err := NewFingerprintWebSocketDialer(nil, codexRSOAuth(proxy), FingerprintCodex)
		if err != nil {
			t.Fatal(err)
		}
		native := dialer.(*codexRSWebSocketDialer)
		native.options.HandshakeTimeout = 50 * time.Millisecond
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		conn, response, err := dialer.DialContext(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		contextErr := ctx.Err()
		cancel()
		var network net.Error
		if contextErr != nil || conn != nil || response != nil || !errors.As(err, &network) || !network.Timeout() {
			t.Fatalf("proxy=%q: native timeout lost net.Error: %T %v (context: %v)", proxy, err, err, contextErr)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("native timeout was converted into caller cancellation: %v", err)
		}
		ctx, cancel = context.WithCancel(t.Context())
		cancel()
		_, _, err = dialer.DialContext(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("caller cancellation was lost: %v", err)
		}
	}
}

//go:build codex_rs

package auth

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	codexhttp "github.com/csbxd/gocodex/httpclient"
	codexws "github.com/csbxd/gocodex/websocket"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestCodexRSTransportFailuresRetryWithoutCooldown(t *testing.T) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy", "CODEX_CA_CERTIFICATE", "SSL_CERT_FILE"} {
		t.Setenv(key, "")
	}
	previous := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(previous) })

	for _, tc := range []struct {
		name string
		fail func(*testing.T) error
	}{
		{"automatic HTTP connection refused", func(t *testing.T) error { return codexRSRefusedHTTP(t, false) }},
		{"direct HTTP connection refused", func(t *testing.T) error { return codexRSRefusedHTTP(t, true) }},
		{"WebSocket handshake timeout", codexRSHandshakeTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := tc.fail(t)
			if !isTransientTransportError(failure) || !isRequestRetryRoundError(failure) {
				t.Fatalf("native failure is not eligible for transport retry: %T %v", failure, failure)
			}
			resultErr := resultErrorFromError(failure)
			if resultErr.Code != transientTransportErrorCode || !shouldSkipCredentialCooldown(resultErr) {
				t.Fatalf("native failure lost its classification for MarkResult: %+v", resultErr)
			}

			manager := NewManager(nil, nil, nil)
			manager.SetRetryConfig(1, 0, 0)
			executor := &transportThenSuccessExecutor{identifier: "codex", fail: failure}
			manager.RegisterExecutor(executor)
			model := "codex-native-retry-" + uuid.NewString()
			authID := "codex-native-retry-" + uuid.NewString()
			registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
			if _, err := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"access_token": "fixture-token"}}); err != nil {
				t.Fatal(err)
			}
			manager.MarkResult(t.Context(), Result{AuthID: authID, Provider: "codex", Model: model, Error: resultErr})
			assertNoCooldown(t, manager, authID, model)
			if _, retry := manager.shouldRetryAfterError(failure, 1, []string{"codex"}, model, 0); retry {
				t.Fatal("native transport error bypassed the configured retry limit")
			}
			response, err := manager.Execute(t.Context(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
			if err != nil || string(response.Payload) != "ok" || executor.callCount() != 2 {
				t.Fatalf("retry failed: response=%q calls=%d err=%v", response.Payload, executor.callCount(), err)
			}
			assertNoCooldown(t, manager, authID, model)
		})
	}
}

func codexRSRefusedHTTP(t *testing.T, direct bool) error {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	transport, err := codexhttp.NewTransport(codexhttp.Options{NoProxy: direct})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Close() })
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+address, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Transport: transport}).Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	return err
}

func TestCodexRSNonTransportErrorsDoNotBecomeRetryable(t *testing.T) {
	for _, err := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		&codexhttp.Error{Kind: "invalid_input", Message: "invalid proxy configuration"},
		&codexhttp.Error{Kind: "request", Message: "certificate verify failed"},
		&Error{HTTPStatus: http.StatusUnauthorized, Message: "invalid credential"},
		&Error{HTTPStatus: http.StatusUnauthorized, Message: "unexpected EOF: connection closed before message completed"},
	} {
		if isTransientTransportError(err) || isRequestRetryRoundError(err) {
			t.Errorf("non-transport error became eligible for transport retry: %T %v", err, err)
		}
	}
}

func codexRSHandshakeTimeout(t *testing.T) error {
	t.Helper()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	client, err := codexws.NewClient(codexws.Options{NoProxy: true, HandshakeTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, _, err = client.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	var native *codexws.Error
	if !errors.As(err, &native) || native.Kind != "timeout" || ctx.Err() != nil {
		t.Fatalf("expected a native handshake timeout: %v (context: %v)", err, ctx.Err())
	}
	return err
}

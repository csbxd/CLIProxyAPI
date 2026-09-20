//go:build codex_rs

package auth

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	codexhttp "github.com/csbxd/gocodex/httpclient"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestCodexRSDNSFailuresRetryWithoutCooldown(t *testing.T) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy", "CODEX_CA_CERTIFICATE", "SSL_CERT_FILE"} {
		t.Setenv(key, "")
	}
	previous := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(previous) })
	previousTransient := transientErrorCooldownSeconds.Load()
	SetTransientErrorCooldownSeconds(5)
	t.Cleanup(func() { transientErrorCooldownSeconds.Store(previousTransient) })
	// A 64-byte label is rejected by both resolvers without querying an external
	// DNS server. It exercises the native name-resolution failure deterministically.
	endpoint := "http://" + strings.Repeat("n", 64) + ".invalid/responses"
	for _, backend := range []string{"Go", "Rust automatic", "Rust direct"} {
		t.Run(backend, func(t *testing.T) {
			var transport http.RoundTripper
			if backend == "Go" {
				standard := &http.Transport{}
				t.Cleanup(standard.CloseIdleConnections)
				transport = standard
			} else {
				native, err := codexhttp.NewTransport(codexhttp.Options{NoProxy: backend == "Rust direct"})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = native.Close() })
				transport = native
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(`{"stream":true}`))
			if err != nil {
				t.Fatal(err)
			}
			_, failure := (&http.Client{Transport: transport}).Do(request)
			if failure == nil || ctx.Err() != nil {
				t.Fatalf("expected resolver failure, got %v (context: %v)", failure, ctx.Err())
			}
			resultErr := resultErrorFromError(failure)
			t.Logf("error=%v; retry=%v; skipCooldown=%v; code=%q", failure, isRequestRetryRoundError(failure), shouldSkipCredentialCooldown(resultErr), resultErr.Code)
			var op *net.OpError
			var dns *net.DNSError
			if !errors.As(failure, &op) || !errors.As(failure, &dns) {
				t.Errorf("DNS transport types lost: %T %v", failure, failure)
			}
			if !isRequestRetryRoundError(failure) || !shouldSkipCredentialCooldown(resultErr) || resultErr.Code != transientTransportErrorCode {
				t.Error("DNS failure does not retain retry and no-cooldown classification")
			}
			manager := NewManager(nil, nil, nil)
			manager.SetRetryConfig(1, 0, 0)
			executor := &transportThenSuccessExecutor{identifier: "codex", fail: failure}
			manager.RegisterExecutor(executor)
			model := "codex-dns-" + uuid.NewString()
			authID := "codex-dns-" + uuid.NewString()
			registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
			if _, err := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"access_token": "fixture-token"}}); err != nil {
				t.Fatal(err)
			}
			manager.MarkResult(t.Context(), Result{AuthID: authID, Provider: "codex", Model: model, Error: resultErr})
			assertNoCooldown(t, manager, authID, model)
			if _, retry := manager.shouldRetryAfterError(failure, 1, []string{"codex"}, model, 0); retry {
				t.Fatal("DNS failure bypassed configured retry limit")
			}
			response, err := manager.Execute(t.Context(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
			if err != nil || string(response.Payload) != "ok" || executor.callCount() != 2 {
				t.Fatalf("DNS retry failed: payload=%q calls=%d err=%v", response.Payload, executor.callCount(), err)
			}
			assertNoCooldown(t, manager, authID, model)
			var streamCalls atomic.Int32
			manager.RegisterExecutor(&customStreamMockExecutor{
				identifier: "codex",
				streamFn: func(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
					if streamCalls.Add(1) == 1 {
						return nil, failure
					}
					return successStreamResult(), nil
				},
			})
			stream, err := manager.ExecuteStream(t.Context(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
			if err != nil {
				t.Fatal(err)
			}
			for chunk := range stream.Chunks {
				if chunk.Err != nil {
					t.Fatal(chunk.Err)
				}
			}
			if streamCalls.Load() != 2 {
				t.Fatalf("SSE DNS retry calls=%d, want 2", streamCalls.Load())
			}
			assertNoCooldown(t, manager, authID, model)
		})
	}
}

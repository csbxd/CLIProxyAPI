//go:build codex_rs

package auth

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	codexhttp "github.com/csbxd/gocodex/httpclient"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestCodexRSHandshakeFailuresRetryWithoutCooldown(t *testing.T) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy", "CODEX_CA_CERTIFICATE", "SSL_CERT_FILE"} {
		t.Setenv(key, "")
	}
	previous := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(previous) })
	previousTransient := transientErrorCooldownSeconds.Load()
	SetTransientErrorCooldownSeconds(5)
	t.Cleanup(func() { transientErrorCooldownSeconds.Store(previousTransient) })
	for _, mode := range []string{"TLS internal error", "SOCKS general failure"} {
		for _, backend := range []string{"Go", "Rust automatic", "Rust direct"} {
			if mode == "SOCKS general failure" && backend == "Rust direct" {
				continue
			}
			t.Run(mode+"/"+backend, func(t *testing.T) {
				endpoint, proxyURL := codexRSFailingHandshake(t, mode == "TLS internal error")
				var transport http.RoundTripper
				if backend == "Go" {
					standard := &http.Transport{}
					if proxyURL != "" {
						parsed, err := url.Parse(proxyURL)
						if err != nil {
							t.Fatal(err)
						}
						standard.Proxy = http.ProxyURL(parsed)
					}
					t.Cleanup(standard.CloseIdleConnections)
					transport = standard
				} else {
					native, err := codexhttp.NewTransport(codexhttp.Options{NoProxy: backend == "Rust direct", ProxyURL: proxyURL})
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
					t.Fatalf("expected peer handshake failure: %v (context: %v)", failure, ctx.Err())
				}
				message := strings.ToLower(failure.Error())
				if mode == "TLS internal error" && !strings.Contains(message, "internal error") && !strings.Contains(message, "internalerror") {
					t.Fatalf("fixture did not produce a TLS internal_error alert: %v", failure)
				}
				if mode == "SOCKS general failure" && !strings.Contains(message, "server failure") {
					t.Fatalf("fixture did not produce SOCKS general failure: %v", failure)
				}
				resultErr := resultErrorFromError(failure)
				t.Logf("error=%v; retry=%v; skipCooldown=%v; code=%q", failure, isRequestRetryRoundError(failure), shouldSkipCredentialCooldown(resultErr), resultErr.Code)
				var op *net.OpError
				if !errors.As(failure, &op) || !isRequestRetryRoundError(failure) || !shouldSkipCredentialCooldown(resultErr) {
					t.Error("handshake failure lost transport retry/no-cooldown classification")
				}
				manager := NewManager(nil, nil, nil)
				manager.SetRetryConfig(1, 0, 0)
				model := "codex-handshake-" + uuid.NewString()
				authID := "codex-handshake-" + uuid.NewString()
				registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
				if _, err := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"access_token": "fixture-token"}}); err != nil {
					t.Fatal(err)
				}
				manager.MarkResult(t.Context(), Result{AuthID: authID, Provider: "codex", Model: model, Error: resultErr})
				assertNoCooldown(t, manager, authID, model)
				if _, retry := manager.shouldRetryAfterError(failure, 1, []string{"codex"}, model, 0); retry {
					t.Fatal("handshake failure bypassed retry limit")
				}
				var calls atomic.Int32
				manager.RegisterExecutor(&customStreamMockExecutor{identifier: "codex", streamFn: func(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
					if calls.Add(1) == 1 {
						return nil, failure
					}
					return successStreamResult(), nil
				}})
				stream, err := manager.ExecuteStream(t.Context(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range stream.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
				if calls.Load() != 2 {
					t.Fatalf("SSE retry calls=%d, want 2", calls.Load())
				}
				assertNoCooldown(t, manager, authID, model)
			})
		}
	}
}

func codexRSFailingHandshake(t *testing.T, tlsAlert bool) (string, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	t.Cleanup(func() { _ = listener.Close(); <-done })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if tlsAlert {
			var header [5]byte
			_, err = io.ReadFull(conn, header[:])
			if err == nil {
				_, err = io.CopyN(io.Discard, conn, int64(binary.BigEndian.Uint16(header[3:])))
			}
			if err == nil {
				_, err = conn.Write([]byte{21, 3, 3, 0, 2, 2, 80})
			}
		} else {
			var greeting [2]byte
			_, err = io.ReadFull(conn, greeting[:])
			if err == nil {
				_, err = io.CopyN(io.Discard, conn, int64(greeting[1]))
			}
			if err == nil {
				_, err = conn.Write([]byte{5, 0})
			}
			var command [4]byte
			if err == nil {
				_, err = io.ReadFull(conn, command[:])
			}
			length := 4
			if command[3] == 4 {
				length = 16
			} else if command[3] == 3 {
				var size [1]byte
				_, err = io.ReadFull(conn, size[:])
				length = int(size[0])
			}
			if err == nil {
				_, err = io.CopyN(io.Discard, conn, int64(length+2))
			}
			if err == nil {
				_, err = conn.Write([]byte{5, 1, 0, 1, 0, 0, 0, 0, 0, 0})
			}
		}
		done <- err
	}()
	if tlsAlert {
		return "https://" + listener.Addr().String(), ""
	}
	return "http://127.0.0.1:80/responses", "socks5://" + listener.Addr().String()
}

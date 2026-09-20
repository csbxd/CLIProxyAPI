//go:build codex_rs

package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	codexhttp "github.com/csbxd/gocodex/httpclient"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestCodexRSHTTPDisconnectRetriesWithoutCooldown(t *testing.T) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy", "CODEX_CA_CERTIFICATE", "SSL_CERT_FILE"} {
		t.Setenv(key, "")
	}
	previous := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(previous) })
	previousTransient := transientErrorCooldownSeconds.Load()
	SetTransientErrorCooldownSeconds(5)
	t.Cleanup(func() { transientErrorCooldownSeconds.Store(previousTransient) })

	const partial = "data: {\"type\":\"response.created\"}\n\n"
	for _, truncatedBody := range []bool{false, true} {
		phase := "before headers"
		if truncatedBody {
			phase = "truncated SSE"
		}
		t.Run(phase, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				conn, writer, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = conn.Close() }()
				if truncatedBody {
					_, _ = fmt.Fprintf(writer, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(partial)+32, partial)
					_ = writer.Flush()
				}
			}))
			t.Cleanup(server.Close)
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
					request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
					if err != nil {
						t.Fatal(err)
					}
					response, failure := (&http.Client{Transport: transport}).Do(request)
					if truncatedBody {
						if failure != nil {
							t.Fatalf("response headers failed: %v", failure)
						}
						data, errRead := io.ReadAll(response.Body)
						_ = response.Body.Close()
						if string(data) != partial {
							t.Fatalf("partial SSE bytes lost: %q", data)
						}
						failure = errRead
					} else if response != nil {
						_ = response.Body.Close()
					}
					if failure == nil {
						t.Fatal("truncated response was accepted")
					}
					resultErr := resultErrorFromError(failure)
					t.Logf("error=%v; retry=%v; skipCooldown=%v; code=%q", failure, isRequestRetryRoundError(failure), shouldSkipCredentialCooldown(resultErr), resultErr.Code)
					if !isRequestRetryRoundError(failure) {
						t.Error("disconnect does not permit request retry")
					}
					if !shouldSkipCredentialCooldown(resultErr) {
						t.Error("disconnect can cool a healthy credential")
					}
					manager := NewManager(nil, nil, nil)
					manager.SetRetryConfig(1, 0, 0)
					executor := &transportThenSuccessExecutor{identifier: "codex", fail: failure}
					manager.RegisterExecutor(executor)
					model := "codex-eof-" + uuid.NewString()
					authID := "codex-eof-" + uuid.NewString()
					registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
					if _, err := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex"}); err != nil {
						t.Fatal(err)
					}
					manager.MarkResult(t.Context(), Result{AuthID: authID, Provider: "codex", Model: model, Error: resultErr})
					assertNoCooldown(t, manager, authID, model)
					if _, retry := manager.shouldRetryAfterError(failure, 1, []string{"codex"}, model, 0); retry {
						t.Fatal("disconnect bypassed the configured retry limit")
					}
					result, err := manager.Execute(t.Context(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
					if err != nil || string(result.Payload) != "ok" || executor.callCount() != 2 {
						t.Fatalf("disconnect retry failed: payload=%q calls=%d err=%v", result.Payload, executor.callCount(), err)
					}
					assertNoCooldown(t, manager, authID, model)

					// A stream which has emitted output must report the failure
					// without replaying the request or cooling its credential.
					var streamCalls atomic.Int32
					manager.RegisterExecutor(&customStreamMockExecutor{
						identifier: "codex",
						streamFn: func(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
							streamCalls.Add(1)
							chunks := make(chan cliproxyexecutor.StreamChunk, 2)
							chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("partial output")}
							chunks <- cliproxyexecutor.StreamChunk{Err: failure}
							close(chunks)
							return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
						},
					})
					stream, err := manager.ExecuteStream(t.Context(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
					if err != nil {
						t.Fatal(err)
					}
					var payloads, failures int
					for chunk := range stream.Chunks {
						if string(chunk.Payload) == "partial output" {
							payloads++
						}
						if errors.Is(chunk.Err, io.EOF) || errors.Is(chunk.Err, io.ErrUnexpectedEOF) {
							failures++
						}
					}
					if payloads != 1 || failures != 1 || streamCalls.Load() != 1 {
						t.Fatalf("partial stream was replayed or lost its failure: payloads=%d failures=%d calls=%d", payloads, failures, streamCalls.Load())
					}
					assertNoCooldown(t, manager, authID, model)
				})
			}
		})
	}
}

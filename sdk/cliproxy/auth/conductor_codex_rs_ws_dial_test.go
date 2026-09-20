//go:build codex_rs

package auth

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	codexws "github.com/csbxd/gocodex/websocket"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
)

func TestCodexRSWebSocketDialFailureClassification(t *testing.T) {
	previous := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(previous) })
	previousTransient := transientErrorCooldownSeconds.Load()
	SetTransientErrorCooldownSeconds(5)
	t.Cleanup(func() { transientErrorCooldownSeconds.Store(previousTransient) })
	for _, stall := range []bool{false, true} {
		mode := "EOF"
		if stall {
			mode = "timeout"
		}
		t.Run(mode, func(t *testing.T) {
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if stall {
					<-release
					return
				}
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				_ = conn.Close()
			}))
			t.Cleanup(server.Close)
			t.Cleanup(func() { close(release) })
			for _, backend := range []string{"Go", "Rust"} {
				t.Run(backend, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					url := "ws" + strings.TrimPrefix(server.URL, "http")
					var failure error
					if backend == "Go" {
						dialer := websocket.Dialer{HandshakeTimeout: 50 * time.Millisecond}
						_, response, err := dialer.DialContext(ctx, url, nil)
						if response != nil {
							_ = response.Body.Close()
						}
						failure = err
					} else {
						client, err := codexws.NewClient(codexws.Options{NoProxy: true, HandshakeTimeout: 50 * time.Millisecond})
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = client.Close() })
						_, _, failure = client.Dial(ctx, url, nil)
					}
					if failure == nil || ctx.Err() != nil {
						t.Fatalf("expected native dial failure, got %v (context: %v)", failure, ctx.Err())
					}
					result := resultErrorFromError(failure)
					t.Logf("error=%v; retry=%v; skipCooldown=%v", failure, isRequestRetryRoundError(failure), shouldSkipCredentialCooldown(result))
					if !isRequestRetryRoundError(failure) || !shouldSkipCredentialCooldown(result) {
						t.Errorf("WebSocket dial failure lost retry/cooldown classification: %v", failure)
					}
					var timeout net.Error
					if stall {
						if !errors.As(failure, &timeout) || !timeout.Timeout() {
							t.Fatalf("handshake timeout does not implement net.Error: %T %v", failure, failure)
						}
					} else if !errors.Is(failure, io.EOF) && !errors.Is(failure, io.ErrUnexpectedEOF) {
						t.Fatalf("handshake EOF lost standard classification: %v", failure)
					}
					manager := NewManager(nil, nil, nil)
					manager.SetRetryConfig(1, 0, 0)
					model := "ws-dial-" + uuid.NewString()
					authID := "ws-dial-" + uuid.NewString()
					registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
					if _, err := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex"}); err != nil {
						t.Fatal(err)
					}
					manager.MarkResult(t.Context(), Result{AuthID: authID, Provider: "codex", Model: model, Error: result})
					assertNoCooldown(t, manager, authID, model)
					if _, retry := manager.shouldRetryAfterError(failure, 0, []string{"codex"}, model, 0); !retry {
						t.Fatal("configured handshake retry was skipped")
					}
					if _, retry := manager.shouldRetryAfterError(failure, 1, []string{"codex"}, model, 0); retry {
						t.Fatal("handshake error bypassed the retry limit")
					}
				})
			}
		})
	}
}

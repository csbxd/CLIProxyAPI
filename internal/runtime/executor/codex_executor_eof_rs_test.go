//go:build codex_rs

package executor

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestCodexRSExecutorDisconnectSemantics(t *testing.T) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy", "CODEX_CA_CERTIFICATE", "SSL_CERT_FILE"} {
		t.Setenv(key, "")
	}
	for _, mode := range []string{"headers", "nonstream body", "stream bootstrap"} {
		for _, proxy := range []string{"", "direct"} {
			t.Run(mode+"/proxy="+proxy, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					// Drain the POST so closing the socket produces EOF rather than
					// a reset caused by unread request bytes.
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Error(err)
						return
					}
					conn, writer, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close() }()
					if mode != "headers" {
						body := "data: " + codexCreatedEvent + "\n\n"
						_, _ = fmt.Fprintf(writer, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body)+32, body)
						_ = writer.Flush()
					}
				}))
				t.Cleanup(server.Close)
				auth := &cliproxyauth.Auth{
					Provider: "codex", ProxyURL: proxy,
					Attributes: map[string]string{"base_url": server.URL},
					Metadata:   map[string]any{"access_token": "fixture-token"},
				}
				executor := NewCodexExecutor(codexBufferingConfig(true))
				request, options := codexTestRequest()
				var err error
				if mode == "stream bootstrap" {
					stream, failure := executor.ExecuteStream(t.Context(), auth, request, options)
					if stream != nil {
						for range stream.Chunks {
						}
						t.Fatal("truncated bootstrap exposed a stream")
					}
					err = failure
				} else {
					options.Stream = false
					_, err = executor.Execute(t.Context(), auth, request, options)
				}
				if mode == "nonstream body" {
					// Execute intentionally maps missing terminal SSE events to
					// a request-scoped error for both Go and Rust transports.
					var incomplete codexIncompleteStreamError
					if !errors.As(err, &incomplete) || !incomplete.IsRequestScoped() || incomplete.StatusCode() != http.StatusRequestTimeout {
						t.Fatalf("nonstream incomplete-response semantics changed: %T %v", err, err)
					}
				} else if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("executor lost native EOF classification: %T %v", err, err)
				}
			})
		}
	}
}

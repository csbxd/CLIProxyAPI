//go:build codex_rs

package executor

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

const codexRSDelta = `data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_1","delta":"hello"}` + "\n\n"
const codexRSCompleted = `data: {"type":"response.completed","response":{"id":"resp_fixture","object":"response","status":"completed","output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n"

func codexRSTestCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := &x509.Certificate{
		SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, root, root, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), BasicConstraintsValid: true,
		NotBefore: root.NotBefore, NotAfter: root.NotAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, root, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: key}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})
}

func TestCodexRSOAuthSSE(t *testing.T) {
	for _, mode := range []string{"stream", "nonstream", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			release, left := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(left)
				if r.ProtoMajor != 2 || r.Method != http.MethodPost || r.URL.Path != "/responses" {
					t.Errorf("upstream request = %s %s %s", r.Proto, r.Method, r.URL.Path)
				}
				if r.Header.Get("Authorization") != "Bearer fixture-token" || r.Header.Get("Chatgpt-Account-Id") != "fixture-account" || r.Header.Get("Accept") != "text/event-stream" {
					t.Error("OAuth or SSE headers were lost")
				}
				body, err := io.ReadAll(r.Body)
				if err != nil || !bytes.Contains(body, []byte(`"stream":true`)) {
					t.Errorf("upstream request was not an SSE request: %v", err)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-Fixture", "native-h2")
				_, _ = io.WriteString(w, codexRSDelta)
				w.(http.Flusher).Flush()
				if mode != "nonstream" {
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
				}
				_, _ = io.WriteString(w, codexRSCompleted)
			}))
			certificate, rootPEM := codexRSTestCertificate(t)
			server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}}
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()
			defer unblock()
			caFile := filepath.Join(t.TempDir(), "ca.pem")
			if err := os.WriteFile(caFile, rootPEM, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("CODEX_CA_CERTIFICATE", caFile)
			auth := &cliproxyauth.Auth{
				Provider: "codex", ProxyURL: "direct",
				Attributes: map[string]string{"base_url": server.URL},
				Metadata:   map[string]any{"access_token": "fixture-token", "account_id": "fixture-account"},
			}
			executor := NewCodexExecutor(&config.Config{})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			request := cliproxyexecutor.Request{Model: "gpt-5.5", Payload: []byte(`{"model":"gpt-5.5","input":"hello","stream":true}`)}
			options := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), Stream: mode != "nonstream"}
			if mode == "nonstream" {
				response, err := executor.Execute(ctx, auth, request, options)
				if err != nil || !bytes.Contains(response.Payload, []byte("hello")) {
					t.Fatalf("nonstream response = %s, error = %v", response.Payload, err)
				}
				return
			}
			stream, err := executor.ExecuteStream(ctx, auth, request, options)
			if err != nil {
				t.Fatal(err)
			}
			if stream.Headers.Get("X-Fixture") != "native-h2" {
				t.Fatal("upstream response headers were lost")
			}
			// The server cannot finish until the first event has reached the caller.
			select {
			case chunk, ok := <-stream.Chunks:
				if !ok || chunk.Err != nil || !bytes.Contains(chunk.Payload, []byte("hello")) {
					t.Fatalf("first SSE chunk = %s, error = %v", chunk.Payload, chunk.Err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("SSE response was buffered instead of streamed")
			}
			if mode == "cancel" {
				cancel()
			} else {
				unblock()
			}
			var received strings.Builder
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			for {
				select {
				case chunk, ok := <-stream.Chunks:
					if !ok {
						if mode == "stream" && !strings.Contains(received.String(), "response.completed") {
							t.Fatal("terminal SSE event missing")
						}
						select {
						case <-left:
						case <-timer.C:
							t.Fatal("upstream request remained active")
						}
						return
					}
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
					received.Write(chunk.Payload)
				case <-timer.C:
					t.Fatal("SSE stream did not terminate")
				}
			}
		})
	}
}

func TestCodexRSProxyConfigurationErrors(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "codex", ProxyURL: "socks4://user:secret@invalid", Metadata: map[string]any{"access_token": "fixture"}}
	request := cliproxyexecutor.Request{Model: "gpt-5.5", Payload: []byte(`{"model":"gpt-5.5","input":"hello"}`)}
	options := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")}
	check := func(t *testing.T, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "codex-rs proxy") || strings.Contains(err.Error(), "secret") {
			t.Fatalf("expected redacted proxy configuration error, got %v", err)
		}
	}
	t.Run("stream", func(t *testing.T) {
		_, err := executor.ExecuteStream(t.Context(), auth, request, options)
		check(t, err)
	})
	t.Run("nonstream", func(t *testing.T) {
		_, err := executor.Execute(t.Context(), auth, request, options)
		check(t, err)
	})
	t.Run("compact", func(t *testing.T) {
		compactOptions := options
		compactOptions.Alt = "responses/compact"
		_, err := executor.Execute(t.Context(), auth, request, compactOptions)
		check(t, err)
	})
	t.Run("http-request", func(t *testing.T) {
		httpRequest, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://chatgpt.com/backend-api/codex/responses", nil)
		_, err := executor.HttpRequest(t.Context(), auth, httpRequest)
		check(t, err)
	})
}

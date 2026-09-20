//go:build codex_rs

package executor

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

const wsRSCompleted = `{"type":"response.completed","response":{"id":"rs-response","object":"response","status":"completed","output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"native"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`

func wsRSRequest() (cliproxyexecutor.Request, cliproxyexecutor.Options) {
	return cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`)}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), ResponseFormat: sdktranslator.FromString("openai-response")}
}

func wsRSAuth(baseURL string) *cliproxyauth.Auth {
	return &cliproxyauth.Auth{ID: "rs-oauth", Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": baseURL}, Metadata: map[string]any{"access_token": "fixture-token", "account_id": "fixture-account"}}
}

func wsRSDrain(t *testing.T, result *cliproxyexecutor.StreamResult) ([]byte, error) {
	t.Helper()
	var data []byte
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case chunk, ok := <-result.Chunks:
			if !ok {
				return data, nil
			}
			if chunk.Err != nil {
				return data, chunk.Err
			}
			data = append(data, chunk.Payload...)
		case <-timer.C:
			t.Fatal("WebSocket stream did not finish")
			return nil, nil
		}
	}
}

func TestCodexRSWebSocketSessionReuseAndProxyChange(t *testing.T) {
	var upgrades, proxyHits atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture-token" || r.Header.Get("ChatGPT-Account-ID") != "fixture-account" {
			t.Error("OAuth handshake headers missing")
		}
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, errUpgrade := upgrader.Upgrade(w, r, http.Header{"X-Fixture": {"rs-wss"}})
		if errUpgrade != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		upgrades.Add(1)
		for {
			_, data, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			if !bytes.Contains(data, []byte(`"response.create"`)) {
				t.Error("wrong Codex request type")
			}
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(wsRSCompleted)); errWrite != nil {
				return
			}
		}
	}))
	certificate, root := codexRSTestCertificate(t)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}}
	server.StartTLS()
	defer server.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if errWrite := os.WriteFile(ca, root, 0600); errWrite != nil {
		t.Fatal(errWrite)
	}
	t.Setenv("CODEX_CA_CERTIFICATE", ca)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "CONNECT" {
			t.Error("proxy did not receive CONNECT")
			w.WriteHeader(400)
			return
		}
		proxyHits.Add(1)
		upstream, errDial := net.Dial("tcp", server.Listener.Addr().String())
		if errDial != nil {
			t.Error(errDial)
			return
		}
		defer upstream.Close()
		conn, stream, errHijack := w.(http.Hijacker).Hijack()
		if errHijack != nil {
			t.Error(errHijack)
			return
		}
		defer conn.Close()
		_, _ = io.WriteString(stream, "HTTP/1.1 200 Connection Established\r\n\r\n")
		if errFlush := stream.Flush(); errFlush != nil {
			return
		}
		done := make(chan struct{})
		go func() { _, _ = io.Copy(upstream, stream); _ = upstream.Close(); close(done) }()
		_, _ = io.Copy(conn, upstream)
		_ = conn.Close()
		<-done
	}))
	defer proxy.Close()
	cfg := &config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll, ProxyURL: "http://127.0.0.1:1"}}
	exec := NewCodexWebsocketsExecutor(cfg)
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	defer exec.CloseExecutionSession("rs-session")
	auth := wsRSAuth(server.URL)
	request, options := wsRSRequest()
	options.Metadata = map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "rs-session"}
	var first helps.WebSocketConn
	for turn := 0; turn < 3; turn++ {
		if turn == 2 {
			auth = auth.Clone()
			auth.ProxyURL = proxy.URL
		}
		ctx, cancel := context.WithCancel(t.Context())
		result, errStream := exec.ExecuteStream(ctx, auth, request, options)
		if errStream != nil {
			cancel()
			t.Fatal(errStream)
		}
		data, errDrain := wsRSDrain(t, result)
		cancel()
		if errDrain != nil || !bytes.Contains(data, []byte("native")) {
			t.Fatalf("result=%s %v", data, errDrain)
		}
		sess := exec.getOrCreateSession("rs-session")
		sess.connMu.Lock()
		current := sess.conn
		sess.connMu.Unlock()
		if !helps.WebSocketAutoPong(current) {
			t.Fatal("Codex OAuth did not use Rust")
		}
		if turn == 0 {
			first = current
			if result.Headers.Get("X-Fixture") != "rs-wss" {
				t.Fatal("handshake response header missing")
			}
		}
		if turn == 1 && current != first {
			t.Fatal("finished request context cancelled reusable connection")
		}
		if turn == 2 && current == first {
			t.Fatal("proxy change reused the old connection")
		}
	}
	if upgrades.Load() != 2 || proxyHits.Load() != 1 {
		t.Fatalf("upgrades=%d proxy=%d", upgrades.Load(), proxyHits.Load())
	}
}

func TestCodexRSWebSocketRejectedHandshakeAndFallback(t *testing.T) {
	for _, status := range []int{401, 426, 429} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var wsCalls, httpCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if websocket.IsWebSocketUpgrade(r) {
					wsCalls.Add(1)
					w.Header().Set("Retry-After", "7")
					w.WriteHeader(status)
					_, _ = io.WriteString(w, `{"error":{"message":"fixture rejection","code":"fixture"}}`)
					return
				}
				httpCalls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, codexRSDelta+codexRSCompleted)
			}))
			defer server.Close()
			exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
			request, options := wsRSRequest()
			result, errStream := exec.ExecuteStream(t.Context(), wsRSAuth(server.URL), request, options)
			if status == 426 {
				if errStream != nil {
					t.Fatal(errStream)
				}
				_, errDrain := wsRSDrain(t, result)
				if errDrain != nil {
					t.Fatal(errDrain)
				}
				if httpCalls.Load() != 1 {
					t.Fatal("426 did not fall back to HTTP")
				}
			} else {
				coded, ok := errStream.(interface{ StatusCode() int })
				if !ok || coded.StatusCode() != status {
					t.Fatalf("HTTP status lost: %T %v", errStream, errStream)
				}
				if httpCalls.Load() != 0 {
					t.Fatal("rejection incorrectly fell back to HTTP")
				}
			}
			if wsCalls.Load() != 1 {
				t.Fatal("failed handshake was retried")
			}
		})
	}
}

func TestCodexRSWebSocketClose1009IsRequestScoped(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var upgrades atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		defer conn.Close()
		upgrades.Add(1)
		_, reader, errReader := conn.NextReader()
		if errReader != nil {
			return
		}
		_, _ = io.CopyN(io.Discard, reader, 1024)
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(1009, "too big"), time.Now().Add(time.Second))
		_, _ = io.Copy(io.Discard, reader)
	}))
	defer server.Close()
	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	request, options := wsRSRequest()
	request.Payload = []byte(`{"model":"gpt-5-codex","input":"` + strings.Repeat("x", 8<<20) + `"}`)
	result, errStream := exec.ExecuteStream(t.Context(), wsRSAuth(server.URL), request, options)
	if errStream == nil {
		_, errStream = wsRSDrain(t, result)
	}
	coded, ok := errStream.(interface {
		StatusCode() int
		IsRequestScoped() bool
	})
	if !ok || coded.StatusCode() != 413 || !coded.IsRequestScoped() {
		t.Fatalf("close 1009 misclassified: %T %v", errStream, errStream)
	}
	if upgrades.Load() != 1 {
		t.Fatal("message-too-big error retried the request")
	}
}

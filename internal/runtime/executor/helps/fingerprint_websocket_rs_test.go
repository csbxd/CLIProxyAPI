//go:build codex_rs

package helps

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	codexws "github.com/csbxd/gocodex/websocket"
	"github.com/gorilla/websocket"
)

func wsFixture(t *testing.T, handler func(*websocket.Conn)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, errUpgrade := upgrader.Upgrade(w, r, http.Header{"X-Fixture": {"native"}})
		if errUpgrade != nil {
			return
		}
		defer func() {
			if errClose := conn.Close(); errClose != nil {
				t.Error(errClose)
			}
		}()
		handler(conn)
	}))
	t.Cleanup(server.Close)
	return server
}

func nativeWSTestDial(t *testing.T, ctx context.Context, server *httptest.Server) WebSocketConn {
	t.Helper()
	dialer, errFactory := NewFingerprintWebSocketDialer(nil, codexRSOAuth("direct"), FingerprintCodex)
	if errFactory != nil {
		t.Fatal(errFactory)
	}
	conn, response, errDial := dialer.DialContext(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), http.Header{"Authorization": {"Bearer fixture"}})
	if errDial != nil {
		t.Fatal(errDial)
	}
	if !WebSocketAutoPong(conn) || response.Header.Get("X-Fixture") != "native" {
		t.Fatal("wrong backend or lost handshake metadata")
	}
	t.Cleanup(func() {
		if errClose := conn.Close(); errClose != nil {
			t.Error(errClose)
		}
	})
	return conn
}

func wsTestAwait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("WebSocket operation did not finish")
		var zero T
		return zero
	}
}

func TestCodexRSWebSocketHandshakeAndSelection(t *testing.T) {
	clearCodexRSEnvironment(t)
	for _, fingerprint := range []Fingerprint{FingerprintNone, FingerprintClaude, FingerprintCodex} {
		dialer, errFactory := NewFingerprintWebSocketDialer(nil, codexRSOAuth("direct"), fingerprint)
		if errFactory != nil {
			t.Fatal(errFactory)
		}
		_, native := dialer.(*codexRSWebSocketDialer)
		if native != (fingerprint == FingerprintCodex) {
			t.Fatalf("profile %d selected %T", fingerprint, dialer)
		}
	}
	auth := codexRSOAuth("direct")
	auth.Attributes = map[string]string{"api_key": "fixture"}
	dialer, errFactory := NewFingerprintWebSocketDialer(nil, auth, FingerprintCodex)
	if errFactory != nil {
		t.Fatal(errFactory)
	}
	if _, native := dialer.(*codexRSWebSocketDialer); native {
		t.Fatal("API-key auth selected Rust")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(429)
		_, _ = io.WriteString(w, `{"error":"quota"}`)
	}))
	defer server.Close()
	dialer, errFactory = NewFingerprintWebSocketDialer(nil, codexRSOAuth("direct"), FingerprintCodex)
	if errFactory != nil {
		t.Fatal(errFactory)
	}
	conn, response, errDial := dialer.DialContext(t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	var rejected *codexws.HandshakeError
	if conn != nil || !errors.Is(errDial, websocket.ErrBadHandshake) || !errors.As(errDial, &rejected) {
		t.Fatalf("rejection=%T %v", errDial, errDial)
	}
	if response == nil || response.StatusCode != 429 || response.Header.Get("Retry-After") != "7" {
		t.Fatal("failed upgrade metadata lost")
	}
	body, errRead := io.ReadAll(response.Body)
	if errClose := response.Body.Close(); errClose != nil {
		t.Error(errClose)
	}
	if errRead != nil || string(body) != `{"error":"quota"}` {
		t.Fatalf("error body=%q %v", body, errRead)
	}
}

func TestCodexRSWebSocketLifetimeAndIsolation(t *testing.T) {
	clearCodexRSEnvironment(t)
	server := wsFixture(t, func(conn *websocket.Conn) {
		for {
			kind, data, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			if errWrite := conn.WriteMessage(kind, data); errWrite != nil {
				return
			}
		}
	})
	dialCtx, cancel := context.WithCancel(t.Context())
	a := nativeWSTestDial(t, dialCtx, server)
	b := nativeWSTestDial(t, t.Context(), server)
	cancel() // The handshake context must not become the session's read context.
	for _, conn := range []WebSocketConn{a, b} {
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte("echo")); errWrite != nil {
			t.Fatal(errWrite)
		}
		kind, body, errRead := conn.ReadMessage()
		if errRead != nil || kind != websocket.TextMessage || string(body) != "echo" {
			t.Fatalf("echo=%d %q %v", kind, body, errRead)
		}
	}
	if errClose := a.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	if _, _, errDial := a.(*codexRSWebSocketConn).client.Dial(t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), nil); !errors.Is(errDial, codexws.ErrClientClosed) {
		t.Fatal("connection Close did not release its SDK client")
	}
	if errWrite := b.WriteMessage(websocket.TextMessage, []byte("independent")); errWrite != nil {
		t.Fatal(errWrite)
	}
	_, body, errRead := b.ReadMessage()
	if errRead != nil || string(body) != "independent" {
		t.Fatalf("other connection was closed: %q %v", body, errRead)
	}
}

func TestCodexRSWebSocketAutomaticControls(t *testing.T) {
	clearCodexRSEnvironment(t)
	var pongs atomic.Int32
	server := wsFixture(t, func(conn *websocket.Conn) {
		conn.SetPongHandler(func(string) error { pongs.Add(1); return nil })
		if errPing := conn.WriteControl(websocket.PingMessage, []byte("p"), time.Now().Add(time.Second)); errPing != nil {
			return
		}
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			return
		}
		if errClose := conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(1009, "too big"), time.Now().Add(time.Second)); errClose != nil {
			return
		}
		_, _, _ = conn.ReadMessage()
	})
	conn := nativeWSTestDial(t, t.Context(), server)
	ping := make(chan struct{})
	var once sync.Once
	conn.SetPingHandler(func(string) error {
		once.Do(func() { close(ping) })
		if !WebSocketAutoPong(conn) {
			return errors.New("expected automatic Pong")
		}
		return nil
	})
	var observed atomic.Int32
	conn.SetCloseHandler(func(code int, _ string) error { observed.Store(int32(code)); return nil })
	done := make(chan error, 1)
	go func() { _, _, errRead := conn.ReadMessage(); done <- errRead }()
	wsTestAwait(t, ping)
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte("go")); errWrite != nil {
		var closed *websocket.CloseError
		if !errors.As(errWrite, &closed) || closed.Code != 1009 {
			t.Fatal(errWrite)
		}
	}
	errRead := wsTestAwait(t, done)
	var closed *websocket.CloseError
	if !errors.As(errRead, &closed) || closed.Code != 1009 || observed.Load() != 1009 {
		t.Fatalf("Close status lost: %v", errRead)
	}
	if pongs.Load() != 1 {
		t.Fatalf("Pong count=%d, want one", pongs.Load())
	}
}

func TestCodexRSWebSocketDeadlineUpdates(t *testing.T) {
	clearCodexRSEnvironment(t)
	peerClosed := make(chan struct{})
	server := wsFixture(t, func(conn *websocket.Conn) { _, _, _ = conn.ReadMessage(); close(peerClosed) })
	conn := nativeWSTestDial(t, t.Context(), server)
	if errSet := conn.SetReadDeadline(time.Now().Add(time.Hour)); errSet != nil {
		t.Fatal(errSet)
	}
	done := make(chan error, 1)
	go func() { _, _, errRead := conn.ReadMessage(); done <- errRead }()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for !conn.(*codexRSWebSocketConn).readDeadline.active() {
		select {
		case <-timer.C:
			t.Fatal("read did not start")
		default:
			runtime.Gosched()
		}
	}
	if errSet := conn.SetReadDeadline(time.Time{}); errSet != nil {
		t.Fatal(errSet)
	}
	if errSet := conn.SetReadDeadline(time.Now().Add(-time.Second)); errSet != nil {
		t.Fatal(errSet)
	}
	errRead := wsTestAwait(t, done)
	var timeout net.Error
	if !errors.Is(errRead, os.ErrDeadlineExceeded) || !errors.As(errRead, &timeout) || !timeout.Timeout() {
		t.Fatalf("read deadline=%v", errRead)
	}
	wsTestAwait(t, peerClosed)
}

func TestCodexRSWebSocketCloseDuringWrite(t *testing.T) {
	clearCodexRSEnvironment(t)
	server := wsFixture(t, func(conn *websocket.Conn) {
		_, reader, errReader := conn.NextReader()
		if errReader != nil {
			return
		}
		if _, errRead := io.CopyN(io.Discard, reader, 1024); errRead != nil {
			return
		}
		if errClose := conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(1009, "too big"), time.Now().Add(time.Second)); errClose != nil {
			return
		}
		_, _ = io.Copy(io.Discard, reader)
	})
	conn := nativeWSTestDial(t, t.Context(), server)
	readDone := make(chan error, 1)
	go func() { _, _, errRead := conn.ReadMessage(); readDone <- errRead }()
	writeDone := make(chan error, 1)
	go func() { writeDone <- conn.WriteMessage(websocket.TextMessage, bytes.Repeat([]byte("a"), 8<<20)) }()
	errWrite := wsTestAwait(t, writeDone)
	if errWrite != nil {
		errWrite = ResolveWebSocketWriteError(conn, errWrite)
	}
	errRead := wsTestAwait(t, readDone)
	if errRead == nil {
		t.Fatal("expected peer Close from reader")
	}
	for _, err := range []error{errWrite, errRead} {
		// The kernel may accept the whole upload before the reader sees Close.
		if err == nil {
			continue
		}
		var closed *websocket.CloseError
		if !errors.As(err, &closed) || closed.Code != 1009 {
			t.Fatalf("close during write=%v", err)
		}
	}
}

func TestCodexRSWebSocketCloseBeforeReaderStarts(t *testing.T) {
	clearCodexRSEnvironment(t)
	server := wsFixture(t, func(conn *websocket.Conn) {
		_, reader, err := conn.NextReader()
		if err != nil {
			return
		}
		if _, err := io.CopyN(io.Discard, reader, 1024); err != nil {
			return
		}
		if err := conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(1009, "too big"), time.Now().Add(time.Second)); err != nil {
			t.Error(err)
		}
	})
	conn := nativeWSTestDial(t, t.Context(), server)
	writeDone := make(chan error, 1)
	go func() { writeDone <- conn.WriteMessage(websocket.TextMessage, bytes.Repeat([]byte("a"), 8<<20)) }()
	errWrite := wsTestAwait(t, writeDone)
	var native *codexws.Error
	if !errors.As(errWrite, &native) || native.Kind != "closing" {
		t.Fatalf("expected closing write error before starting reader: %v", errWrite)
	}
	resolved := make(chan error, 1)
	go func() { resolved <- ResolveWebSocketWriteError(conn, errWrite) }()
	readDone := make(chan error, 1)
	go func() { _, _, err := conn.ReadMessage(); readDone <- err }()
	for _, err := range []error{wsTestAwait(t, resolved), wsTestAwait(t, readDone)} {
		var closed *websocket.CloseError
		if !errors.As(err, &closed) || closed.Code != 1009 {
			t.Fatalf("delayed reader lost peer close: %v", err)
		}
	}
}

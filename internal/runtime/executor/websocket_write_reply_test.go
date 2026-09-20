package executor

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

type lateWebsocketWriteError struct {
	helps.WebSocketConn
	onWrite func() error
}

func TestWebsocketFailedLegacyDialReturnsNilInterface(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(401) }))
	defer server.Close()
	exec := NewXAIWebsocketsExecutor(&config.Config{})
	conn, closer, response, errDial := exec.dialXAIWebsocket(t.Context(), &cliproxyauth.Auth{ProxyURL: "direct"}, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if errDial == nil || conn != nil || closer != nil || response == nil || response.StatusCode != 401 {
		t.Fatalf("failed dial returned a typed nil connection: %T %v", conn, errDial)
	}
	if errClose := response.Body.Close(); errClose != nil {
		t.Error(errClose)
	}
}

func (c *lateWebsocketWriteError) WriteMessage(int, []byte) error { return c.onWrite() }

func TestWebsocketConsumesReplyBeforeRetryingLateWriteFailure(t *testing.T) {
	session := &codexWebsocketSession{}
	conn := &lateWebsocketWriteError{}
	readCh := session.activate(conn)
	conn.onWrite = func() error {
		session.noteActiveReply(conn)
		readCh <- codexWebsocketRead{conn: conn, msgType: websocket.TextMessage, payload: []byte(`{"type":"response.completed"}`)}
		if session.clearActive(conn, readCh) {
			close(readCh)
		}
		return net.ErrClosed
	}
	if errWrite := writeWebsocketPayloadMessage("codex", session, conn, []byte("request")); errWrite != nil {
		t.Fatalf("late write failure discarded response: %v", errWrite)
	}
	_, payload, errRead := readCodexWebsocketMessage(context.Background(), session, conn, readCh)
	if errRead != nil || string(payload) != `{"type":"response.completed"}` {
		t.Fatalf("buffered response=%q %v", payload, errRead)
	}
	// Starting another turn must not inherit the preceding turn's reply marker.
	next := session.activate(conn)
	defer func() {
		if session.clearActive(conn, next) {
			close(next)
		}
	}()
	conn.onWrite = func() error { return net.ErrClosed }
	if errWrite := writeWebsocketPayloadMessage("codex", session, conn, []byte("next")); !errors.Is(errWrite, net.ErrClosed) {
		t.Fatalf("new attempt reused old acknowledgement: %v", errWrite)
	}
}

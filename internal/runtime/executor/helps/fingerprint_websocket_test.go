package helps

import (
	"errors"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestFingerprintWebSocketProfileSelection(t *testing.T) {
	if dialer, errFactory := NewFingerprintWebSocketDialer(nil, nil, Fingerprint(255)); errFactory == nil || dialer != nil {
		t.Fatal("invalid profile accepted")
	}
	for _, fingerprint := range []Fingerprint{FingerprintNone, FingerprintClaude} {
		auth := &cliproxyauth.Auth{ProxyURL: "direct", Metadata: map[string]any{"access_token": "fixture"}}
		dialer, errFactory := NewFingerprintWebSocketDialer(nil, auth, fingerprint)
		if errFactory != nil {
			t.Fatal(errFactory)
		}
		standard, ok := dialer.(*gorillaFingerprintWebSocketDialer)
		if !ok || standard.dialer.Proxy != nil {
			t.Fatalf("profile %d failed to preserve direct Gorilla transport", fingerprint)
		}
	}
}

func TestGorillaWebSocketConnResolvesPeerCloseAfterWriteFailure(t *testing.T) {
	conn := &gorillaWebSocketConn{readTerminal: make(chan struct{})}
	closeErr := &websocket.CloseError{Code: websocket.CloseMessageTooBig, Text: "too big"}
	go func() {
		time.Sleep(time.Millisecond)
		conn.setTerminal(closeErr)
	}()
	got := conn.resolveWriteError(errors.New("connection reset by peer"))
	var received *websocket.CloseError
	if !errors.As(got, &received) || received.Code != websocket.CloseMessageTooBig {
		t.Fatalf("resolved write error = %v, want peer Close 1009", got)
	}
}

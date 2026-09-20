//go:build codex_rs

package helps

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	codexws "github.com/csbxd/gocodex/websocket"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
)

type codexRSWebSocketDialer struct{ options codexws.Options }

func newCodexFingerprintWebSocketDialer(cfg *config.Config, auth *cliproxyauth.Auth) (WebSocketDialer, error) {
	if auth == nil || auth.AuthKind() != cliproxyauth.AuthKindOAuth || strings.TrimSpace(auth.Attributes[cliproxyauth.AttributeAPIKey]) != "" {
		return &gorillaFingerprintWebSocketDialer{dialer: NewProxyAwareWebSocketDialer(cfg, auth)}, nil
	}
	setting, errParse := proxyutil.Parse(websocketProxyURL(cfg, auth))
	if errParse != nil {
		return nil, fmt.Errorf("codex-rs websocket proxy: %w", errParse)
	}
	options := codexws.Options{
		HandshakeTimeout: WebSocketHandshakeTimeout, TCPNoDelay: true,
		// Match the existing Gorilla connection's unbounded read limit.
		MaxMessageSize: math.MaxInt, MaxFrameSize: math.MaxInt,
	}
	switch setting.Mode {
	case proxyutil.ModeDirect:
		options.NoProxy = true
	case proxyutil.ModeProxy:
		options.ProxyURL = setting.URL.String()
	}
	return &codexRSWebSocketDialer{options: options}, nil
}

func (d *codexRSWebSocketDialer) DialContext(ctx context.Context, url string, headers http.Header) (WebSocketConn, *http.Response, error) {
	client, errClient := codexws.NewClient(d.options)
	if errClient != nil {
		return nil, nil, errClient
	}
	conn, handshake, errDial := client.Dial(ctx, url, headers)
	var response *http.Response
	if handshake != nil {
		response = &http.Response{
			StatusCode: handshake.StatusCode, Status: strconv.Itoa(handshake.StatusCode) + " " + http.StatusText(handshake.StatusCode),
			Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Header: handshake.Headers.Clone(),
			Body: io.NopCloser(bytes.NewReader(handshake.Body)), ContentLength: int64(len(handshake.Body)),
		}
		if handshake.BodyTruncated {
			response.ContentLength = -1
		}
	}
	if errDial != nil {
		if errClose := client.Close(); errClose != nil {
			errDial = errors.Join(errDial, errClose)
		}
		if handshake != nil {
			errDial = errors.Join(websocket.ErrBadHandshake, errDial)
		}
		return nil, response, errDial
	}
	// A pooled session outlives the request which performed its handshake.
	lifetime, cancel := context.WithCancel(context.Background())
	return &codexRSWebSocketConn{client: client, conn: conn, lifetime: lifetime, cancel: cancel, readTerminal: make(chan struct{})}, response, nil
}

// Each physical connection owns a client; disposing one session never closes
// another session's connection. The underlying Tokio runtime remains shared.
type codexRSWebSocketConn struct {
	client           *codexws.Client
	conn             *codexws.Conn
	lifetime         context.Context
	cancel           context.CancelFunc
	once             sync.Once
	closeErr         error
	readMu           sync.Mutex
	writeMu          sync.Mutex
	readDeadline     nativeWSDeadline
	writeDeadline    nativeWSDeadline
	handlersMu       sync.Mutex
	pingHandler      func(string) error
	closeHandler     func(int, string) error
	peerClose        *websocket.CloseError
	readTerminal     chan struct{}
	readTerminalOnce sync.Once
}

var _ WebSocketConn = (*codexRSWebSocketConn)(nil)

func (*codexRSWebSocketConn) AutomaticPong() bool { return true }

func (c *codexRSWebSocketConn) ReadMessage() (_ int, _ []byte, err error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	ctx, finish := c.readDeadline.begin(c.lifetime)
	defer finish()
	defer func() {
		if err != nil {
			c.readTerminalOnce.Do(func() { close(c.readTerminal) })
			_ = c.Close()
		}
	}()
	for {
		kind, payload, errRead := c.conn.ReadMessage(ctx)
		if errRead != nil {
			return 0, nil, c.operationError(ctx, errRead, false)
		}
		switch kind {
		case codexws.PingMessage:
			if errPing := c.PingHandler()(string(payload)); errPing != nil {
				return 0, nil, errPing
			}
		case codexws.PongMessage:
			// Gorilla handles controls internally and only returns data messages.
		case codexws.CloseMessage:
			code, reason, errParse := codexws.ParseClosePayload(payload)
			if errParse != nil {
				return 0, nil, errParse
			}
			closed := &websocket.CloseError{Code: int(code), Text: reason}
			c.handlersMu.Lock()
			c.peerClose = closed
			c.handlersMu.Unlock()
			if errClose := c.CloseHandler()(int(code), reason); errClose != nil {
				return 0, nil, errClose
			}
			return 0, nil, closed
		default:
			return int(kind), payload, nil
		}
	}
}

func (c *codexRSWebSocketConn) WriteMessage(kind int, payload []byte) error {
	if kind != websocket.TextMessage && kind != websocket.BinaryMessage && kind != websocket.PingMessage && kind != websocket.PongMessage && kind != websocket.CloseMessage {
		return fmt.Errorf("invalid WebSocket message type: %d", kind)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	ctx, finish := c.writeDeadline.begin(c.lifetime)
	defer finish()
	return c.operationError(ctx, c.conn.WriteMessage(ctx, codexws.MessageType(kind), payload), true)
}

func (c *codexRSWebSocketConn) WriteControl(kind int, payload []byte, deadline time.Time) error {
	if kind != websocket.PingMessage && kind != websocket.PongMessage && kind != websocket.CloseMessage {
		return fmt.Errorf("invalid WebSocket control type: %d", kind)
	}
	ctx := c.lifetime
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	return c.operationError(ctx, c.conn.WriteControl(ctx, codexws.MessageType(kind), payload), false)
}

func (c *codexRSWebSocketConn) operationError(ctx context.Context, err error, waitForClose bool) error {
	if err == nil {
		return nil
	}
	if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		_ = c.Close()
		return os.ErrDeadlineExceeded
	}
	var native *codexws.Error
	if waitForClose && errors.As(err, &native) && native.Kind == "closing" && c.readDeadline.active() {
		// The SDK may reject a concurrent data write before its reader returns
		// the peer's Close. Let that reader publish the status (notably 1009)
		// before the executor classifies the failed write for retry.
		select {
		case <-c.readTerminal:
		case <-ctx.Done():
		}
	}
	c.handlersMu.Lock()
	peerClose := c.peerClose
	c.handlersMu.Unlock()
	if peerClose != nil {
		return peerClose
	}
	if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		_ = c.Close()
		return os.ErrDeadlineExceeded
	}
	if c.lifetime.Err() != nil {
		return net.ErrClosed
	}
	if errors.Is(err, io.EOF) {
		return &websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"}
	}
	return err
}

func (c *codexRSWebSocketConn) SetReadDeadline(deadline time.Time) error {
	c.readDeadline.set(deadline)
	return nil
}
func (c *codexRSWebSocketConn) SetWriteDeadline(deadline time.Time) error {
	c.writeDeadline.set(deadline)
	return nil
}
func (c *codexRSWebSocketConn) SetPingHandler(handler func(string) error) {
	c.handlersMu.Lock()
	c.pingHandler = handler
	c.handlersMu.Unlock()
}
func (c *codexRSWebSocketConn) PingHandler() func(string) error {
	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()
	if c.pingHandler != nil {
		return c.pingHandler
	}
	return func(string) error { return nil } // The SDK has already sent the Pong.
}
func (c *codexRSWebSocketConn) SetCloseHandler(handler func(int, string) error) {
	c.handlersMu.Lock()
	c.closeHandler = handler
	c.handlersMu.Unlock()
}
func (c *codexRSWebSocketConn) CloseHandler() func(int, string) error {
	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()
	if c.closeHandler != nil {
		return c.closeHandler
	}
	return func(int, string) error { return nil } // The SDK has already acknowledged Close.
}
func (c *codexRSWebSocketConn) Close() error {
	c.once.Do(func() { c.cancel(); c.closeErr = errors.Join(c.conn.Close(), c.client.Close()) })
	return c.closeErr
}

// Deadline updates affect an in-flight operation, as they do for net.Conn. A
// generation prevents an expired timer callback from cancelling a renewed read.
type nativeWSDeadline struct {
	mu         sync.Mutex
	at         time.Time
	generation uint64
	cancel     context.CancelCauseFunc
	timer      *time.Timer
}

func (d *nativeWSDeadline) active() bool { d.mu.Lock(); defer d.mu.Unlock(); return d.cancel != nil }

func (d *nativeWSDeadline) set(at time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.at = at
	d.resetLocked()
}

func (d *nativeWSDeadline) begin(parent context.Context) (context.Context, func()) {
	d.mu.Lock()
	ctx, cancel := context.WithCancelCause(parent)
	d.cancel = cancel
	d.resetLocked()
	d.mu.Unlock()
	return ctx, func() {
		d.mu.Lock()
		d.cancel = nil
		d.resetLocked()
		d.mu.Unlock()
		cancel(nil)
	}
}

func (d *nativeWSDeadline) resetLocked() {
	d.generation++
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	if d.cancel == nil || d.at.IsZero() {
		return
	}
	if !d.at.After(time.Now()) {
		d.cancel(context.DeadlineExceeded)
		return
	}
	generation := d.generation
	d.timer = time.AfterFunc(time.Until(d.at), func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.generation == generation && d.cancel != nil {
			d.cancel(context.DeadlineExceeded)
		}
	})
}

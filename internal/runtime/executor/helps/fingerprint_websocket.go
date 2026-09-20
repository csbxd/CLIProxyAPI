package helps

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/proxy"
)

// WebSocketHandshakeTimeout is shared by the existing and native dialers.
const WebSocketHandshakeTimeout = 30 * time.Second

// WebSocketConn is the connection surface used by upstream executors. Backends
// must be comparable pointer values: sessions use connection identity to reject
// events from replaced connections. One data reader and writer run concurrently;
// control writes and Close may run alongside them.
type WebSocketConn interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(int, []byte) error
	WriteControl(int, []byte, time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	SetPingHandler(func(string) error)
	PingHandler() func(string) error
	SetCloseHandler(func(int, string) error)
	CloseHandler() func(int, string) error
	Close() error
}

// WebSocketDialer returns HTTP metadata even when an upgrade is rejected.
// DialContext's context controls the handshake, not an established connection.
type WebSocketDialer interface {
	DialContext(context.Context, string, http.Header) (WebSocketConn, *http.Response, error)
}

// WebSocketAutoPong reports whether Ping callbacks observe an already-replied
// control frame. Such callbacks must not send a duplicate Pong.
func WebSocketAutoPong(conn WebSocketConn) bool {
	automatic, ok := conn.(interface{ AutomaticPong() bool })
	return ok && automatic.AutomaticPong()
}

// ResolveWebSocketWriteError lets a session's background reader finish receiving
// a peer Close before classifying a failed write. Callers must have launched that
// reader; it need not have entered ReadMessage yet. Closing the connection also
// releases the wait. Backends without this capability return the original error.
func ResolveWebSocketWriteError(conn WebSocketConn, err error) error {
	if resolver, ok := conn.(interface{ resolveWriteError(error) error }); ok && err != nil {
		return resolver.resolveWriteError(err)
	}
	return err
}

// NewFingerprintWebSocketDialer selects the backend from a profile and build
// tags. codex_rs applies to Codex OAuth; other combinations retain Gorilla.
func NewFingerprintWebSocketDialer(cfg *config.Config, auth *cliproxyauth.Auth, fingerprint Fingerprint) (WebSocketDialer, error) {
	switch fingerprint {
	case FingerprintCodex:
		return newCodexFingerprintWebSocketDialer(cfg, auth)
	case FingerprintNone, FingerprintClaude:
		return &gorillaFingerprintWebSocketDialer{dialer: NewProxyAwareWebSocketDialer(cfg, auth)}, nil
	default:
		return nil, fmt.Errorf("unsupported WebSocket fingerprint profile: %d", fingerprint)
	}
}

type gorillaFingerprintWebSocketDialer struct{ dialer *websocket.Dialer }

func (d *gorillaFingerprintWebSocketDialer) DialContext(ctx context.Context, url string, headers http.Header) (WebSocketConn, *http.Response, error) {
	conn, response, err := d.dialer.DialContext(ctx, url, headers)
	if conn == nil {
		return nil, response, err
	}
	// Keep the existing inbound compression negotiation without compressing
	// outbound messages (some upstreams reject Gorilla's flate tail).
	conn.EnableWriteCompression(false)
	return &gorillaWebSocketConn{Conn: conn, readTerminal: make(chan struct{}), readerStarted: make(chan struct{})}, response, err
}

// gorillaWebSocketConn records the read-side terminal error so a concurrent
// failed upload can use a peer Close code instead of replaying the request.
type gorillaWebSocketConn struct {
	*websocket.Conn
	readTerminal      chan struct{}
	readTerminalOnce  sync.Once
	readerStarted     chan struct{}
	readerStartedOnce sync.Once
	terminalMu        sync.Mutex
	terminalErr       error
}

func (c *gorillaWebSocketConn) setTerminal(err error) {
	c.terminalMu.Lock()
	if c.terminalErr == nil {
		c.terminalErr = err
	}
	c.terminalMu.Unlock()
	c.readTerminalOnce.Do(func() { close(c.readTerminal) })
}

func (c *gorillaWebSocketConn) terminal() error {
	c.terminalMu.Lock()
	defer c.terminalMu.Unlock()
	return c.terminalErr
}

func (c *gorillaWebSocketConn) ReadMessage() (int, []byte, error) {
	if c.readerStarted != nil {
		c.readerStartedOnce.Do(func() { close(c.readerStarted) })
	}
	kind, payload, err := c.Conn.ReadMessage()
	if err != nil {
		c.setTerminal(err)
	}
	return kind, payload, err
}

func (c *gorillaWebSocketConn) SetCloseHandler(handler func(int, string) error) {
	c.Conn.SetCloseHandler(func(code int, text string) error {
		c.setTerminal(&websocket.CloseError{Code: code, Text: text})
		if handler != nil {
			return handler(code, text)
		}
		return nil
	})
}

func (c *gorillaWebSocketConn) resolveWriteError(err error) error {
	if err == nil {
		return nil
	}
	startup := time.NewTimer(time.Second)
	defer startup.Stop()
	if c.readerStarted != nil {
		select {
		case <-c.readTerminal:
		case <-c.readerStarted:
		case <-startup.C:
			return err
		}
	}
	if c.readerStarted == nil {
		select {
		case <-c.readTerminal:
		case <-startup.C:
			return err
		}
	}
	select {
	case <-c.readTerminal:
	case <-startup.C:
		return err
	}
	if terminal := c.terminal(); terminal != nil {
		return terminal
	}
	return err
}

func (c *gorillaWebSocketConn) Close() error {
	c.setTerminal(net.ErrClosed)
	return c.Conn.Close()
}

// NewProxyAwareWebSocketDialer preserves the existing Gorilla proxy policy.
func NewProxyAwareWebSocketDialer(cfg *config.Config, auth *cliproxyauth.Auth) *websocket.Dialer {
	dialer := &websocket.Dialer{
		Proxy: http.ProxyFromEnvironment, HandshakeTimeout: WebSocketHandshakeTimeout, EnableCompression: true,
		NetDialContext: (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}
	proxyURL := websocketProxyURL(cfg, auth)
	if proxyURL == "" {
		return dialer
	}
	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		log.Errorf("codex websockets executor: %v", errParse)
		return dialer
	}
	switch setting.Mode {
	case proxyutil.ModeDirect:
		dialer.Proxy = nil
		return dialer
	case proxyutil.ModeProxy:
	default:
		return dialer
	}
	switch setting.URL.Scheme {
	case "socks5", "socks5h":
		var proxyAuth *proxy.Auth
		if setting.URL.User != nil {
			password, _ := setting.URL.User.Password()
			proxyAuth = &proxy.Auth{User: setting.URL.User.Username(), Password: password}
		}
		direct, errSOCKS := proxy.SOCKS5("tcp", setting.URL.Host, proxyAuth, proxy.Direct)
		if errSOCKS != nil {
			log.Errorf("codex websockets executor: create SOCKS5 dialer failed: %v", errSOCKS)
			return dialer
		}
		dialer.Proxy = nil
		dialer.NetDialContext = func(_ context.Context, network, addr string) (net.Conn, error) { return direct.Dial(network, addr) }
	case "http", "https":
		dialer.Proxy = http.ProxyURL(setting.URL)
	}
	return dialer
}

func websocketProxyURL(cfg *config.Config, auth *cliproxyauth.Auth) string {
	if auth != nil && strings.TrimSpace(auth.ProxyURL) != "" {
		return strings.TrimSpace(auth.ProxyURL)
	}
	if cfg != nil {
		return strings.TrimSpace(cfg.ProxyURL)
	}
	return ""
}

// WebSocketRouteKey identifies settings which require a new physical connection.
// Hash credentials-bearing proxy URLs so the session key cannot disclose them.
func WebSocketRouteKey(cfg *config.Config, auth *cliproxyauth.Auth, fingerprint Fingerprint) string {
	proxyURL := websocketProxyURL(cfg, auth)
	if setting, errParse := proxyutil.Parse(proxyURL); errParse == nil {
		if setting.Mode == proxyutil.ModeDirect {
			proxyURL = "direct"
		}
		if setting.Mode == proxyutil.ModeProxy {
			proxyURL = setting.URL.String()
		}
	}
	parts := []string{strconv.Itoa(int(fingerprint)), proxyURL, os.Getenv("CODEX_CA_CERTIFICATE"), os.Getenv("SSL_CERT_FILE")}
	if auth != nil {
		parts = append(parts, auth.AuthKind(), strconv.FormatBool(strings.TrimSpace(auth.Attributes[cliproxyauth.AttributeAPIKey]) != ""))
	}
	if proxyURL == "" {
		for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy"} {
			parts = append(parts, os.Getenv(key))
		}
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(parts, "\x00"))))
}

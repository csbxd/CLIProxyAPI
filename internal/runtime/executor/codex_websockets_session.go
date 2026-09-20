package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

type codexWebsocketSessionStore struct {
	mu       sync.Mutex
	sessions map[string]*codexWebsocketSession
}

var globalCodexWebsocketSessionStore = &codexWebsocketSessionStore{
	sessions: make(map[string]*codexWebsocketSession),
}

type websocketConnectionCloser struct {
	conn helps.WebSocketConn
	once sync.Once
	err  error
}

func newWebsocketConnectionCloser(conn helps.WebSocketConn) *websocketConnectionCloser {
	if conn == nil {
		return nil
	}
	return &websocketConnectionCloser{conn: conn}
}

func (c *websocketConnectionCloser) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.once.Do(func() {
		c.err = c.conn.Close()
	})
	return c.err
}

type codexWebsocketSession struct {
	sessionID string

	reqMu sync.Mutex

	connMu                    sync.Mutex
	conn                      helps.WebSocketConn
	connCloser                *websocketConnectionCloser
	wsURL                     string
	routeKey                  string
	authID                    string
	multiAgentV2OptimizedConn helps.WebSocketConn
	lifecycleBindMu           sync.Mutex
	lifecycle                 cliproxyexecutor.ExecutionLifecycle
	lifecycleModel            string

	writeMu sync.Mutex

	activeMu           sync.Mutex
	activeConn         helps.WebSocketConn
	activeCh           chan codexWebsocketRead
	activeDone         <-chan struct{}
	activeCancel       context.CancelFunc
	activeReadConn     helps.WebSocketConn
	activeReadObserved bool

	readerConn helps.WebSocketConn

	upstreamDisconnectOnce    sync.Once
	upstreamDisconnectCh      chan error
	upstreamDisconnectErrMu   sync.RWMutex
	upstreamDisconnectErrConn helps.WebSocketConn
	upstreamDisconnectErr     error

	lastEventMu   sync.Mutex
	lastEventConn helps.WebSocketConn
	lastEventType string
}

type codexWebsocketRead struct {
	conn    helps.WebSocketConn
	msgType int
	payload []byte
	err     error
}

func (s *codexWebsocketSession) setActive(conn helps.WebSocketConn, ch chan codexWebsocketRead) {
	if s == nil {
		return
	}
	s.activeMu.Lock()
	if s.activeCancel != nil {
		s.activeCancel()
		s.activeCancel = nil
		s.activeDone = nil
	}
	s.activeConn = conn
	s.activeReadConn = conn
	s.activeReadObserved = false
	s.activeCh = ch
	if conn != nil && ch != nil {
		activeCtx, activeCancel := context.WithCancel(context.Background())
		s.activeDone = activeCtx.Done()
		s.activeCancel = activeCancel
	}
	s.activeMu.Unlock()
}

func (s *codexWebsocketSession) activate(conn helps.WebSocketConn) chan codexWebsocketRead {
	if s == nil || conn == nil {
		return nil
	}
	ch := make(chan codexWebsocketRead, 4096)
	s.setActive(conn, ch)
	return ch
}

func (s *codexWebsocketSession) activeForConn(conn helps.WebSocketConn) (chan codexWebsocketRead, <-chan struct{}) {
	if s == nil || conn == nil {
		return nil, nil
	}
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if s.activeConn != conn {
		return nil, nil
	}
	return s.activeCh, s.activeDone
}

func (s *codexWebsocketSession) noteActiveReply(conn helps.WebSocketConn) {
	s.activeMu.Lock()
	if s.activeConn == conn && s.activeCh != nil {
		s.activeReadObserved = true
	}
	s.activeMu.Unlock()
}

func (s *codexWebsocketSession) receivedActiveReply(conn helps.WebSocketConn) bool {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	// Retain this fact when the reader closes the channel; its buffered response
	// must still be consumed if the concurrent writer reports a late failure.
	return s.activeReadConn == conn && s.activeReadObserved
}

func clearRetryActiveState(sess *codexWebsocketSession, conn helps.WebSocketConn, ch chan codexWebsocketRead) bool {
	if sess == nil {
		return false
	}
	return sess.clearActive(conn, ch)
}

func (s *codexWebsocketSession) clearActive(conn helps.WebSocketConn, ch chan codexWebsocketRead) bool {
	if s == nil {
		return false
	}
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if s.activeConn != conn || s.activeCh != ch {
		return false
	}
	s.activeConn = nil
	s.activeCh = nil
	if s.activeCancel != nil {
		s.activeCancel()
	}
	s.activeCancel = nil
	s.activeDone = nil
	return true
}

const codexWebsocketWriteChunkSize = 32 * 1024

var (
	testWebsocketWritePayloadHook func(conn helps.WebSocketConn)
	testWebsocketWriteChunkHook   func(chunkIndex int, totalChunks int)
)

func (s *codexWebsocketSession) writeMessage(conn helps.WebSocketConn, msgType int, payload []byte) error {
	if s == nil {
		return fmt.Errorf("codex websockets executor: session is nil")
	}
	if conn == nil {
		return fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if testWebsocketWritePayloadHook != nil {
		testWebsocketWritePayloadHook(conn)
	}
	if len(payload) <= codexWebsocketWriteChunkSize {
		return conn.WriteMessage(msgType, payload)
	}
	fragmented, ok := conn.(interface {
		NextWriter(int) (io.WriteCloser, error)
	})
	if !ok {
		return conn.WriteMessage(msgType, payload)
	}
	w, errNext := fragmented.NextWriter(msgType)
	if errNext != nil {
		return errNext
	}
	totalChunks := (len(payload) + codexWebsocketWriteChunkSize - 1) / codexWebsocketWriteChunkSize
	for i := 0; i < len(payload); i += codexWebsocketWriteChunkSize {
		end := i + codexWebsocketWriteChunkSize
		if end > len(payload) {
			end = len(payload)
		}
		chunkIdx := i / codexWebsocketWriteChunkSize
		if testWebsocketWriteChunkHook != nil {
			testWebsocketWriteChunkHook(chunkIdx, totalChunks)
		}
		if _, errWrite := w.Write(payload[i:end]); errWrite != nil {
			_ = w.Close()
			return errWrite
		}
	}
	return w.Close()
}

func (s *codexWebsocketSession) setMultiAgentV2Optimized(conn helps.WebSocketConn, optimized bool) {
	if s == nil || conn == nil {
		return
	}
	s.connMu.Lock()
	if s.conn == conn {
		if optimized {
			s.multiAgentV2OptimizedConn = conn
		} else {
			s.multiAgentV2OptimizedConn = nil
		}
	}
	s.connMu.Unlock()
}

func (s *codexWebsocketSession) isMultiAgentV2Optimized(conn helps.WebSocketConn) bool {
	if s == nil || conn == nil {
		return false
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.conn == conn && s.multiAgentV2OptimizedConn == conn
}

// sendTerminalWebsocketRead reports whether it invalidated a full channel's connection before waiting.
func sendTerminalWebsocketRead(ch chan<- codexWebsocketRead, done <-chan struct{}, event codexWebsocketRead, invalidate func()) bool {
	select {
	case ch <- event:
		return false
	case <-done:
		return false
	default:
	}

	invalidated := invalidate != nil
	if invalidated {
		invalidate()
	}
	select {
	case ch <- event:
	case <-done:
	}
	return invalidated
}

func (s *codexWebsocketSession) configureConn(conn helps.WebSocketConn) {
	if s == nil || conn == nil {
		return
	}
	s.resetUpstreamDisconnectError(conn)
	conn.SetPingHandler(func(appData string) error {
		if helps.WebSocketAutoPong(conn) {
			return nil
		}
		sessionID := ""
		if s != nil {
			sessionID = s.sessionID
		}
		sessionKind := sessionObjectKind(s)
		log.Debugf("codex websockets: upstream ping received session=%s session_object=%s ping_bytes=%d", sessionID, sessionKind, len(appData))
		log.Debugf("codex websockets: upstream pong write started session=%s session_object=%s", sessionID, sessionKind)
		start := time.Now()
		// Gorilla websocket allows concurrent WriteControl with WriteMessage.
		// Avoid writeMu here so keepalive pongs are not starved by long payload writes.
		errPong := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
		if errPong != nil {
			log.Warnf("codex websockets: upstream pong write failed session=%s session_object=%s duration=%v err=%v", sessionID, sessionKind, time.Since(start), errPong)
		} else {
			log.Debugf("codex websockets: upstream pong replied session=%s session_object=%s duration=%v", sessionID, sessionKind, time.Since(start))
		}
		return errPong
	})
	defaultCloseHandler := conn.CloseHandler()
	conn.SetCloseHandler(func(code int, text string) error {
		s.setUpstreamDisconnectError(conn, &websocket.CloseError{Code: code, Text: text})
		return defaultCloseHandler(code, text)
	})
}

func (s *codexWebsocketSession) bindExecutionLifecycle(opts cliproxyexecutor.Options, conn helps.WebSocketConn, closer *websocketConnectionCloser, model string) error {
	if closer == nil {
		return fmt.Errorf("codex websockets executor: websocket connection closer is nil")
	}
	if s == nil {
		return cliproxyexecutor.BindExecutionResource(opts, closer)
	}
	lifecycle := opts.ExecutionLifecycle
	if lifecycle == nil || conn == nil {
		return nil
	}

	s.lifecycleBindMu.Lock()
	defer s.lifecycleBindMu.Unlock()

	s.connMu.Lock()
	if s.conn == conn && s.connCloser == nil {
		s.connCloser = closer
	}
	alreadyBound := s.conn == conn && s.connCloser == closer && s.lifecycle == lifecycle
	s.connMu.Unlock()
	if alreadyBound {
		return nil
	}

	if errBind := lifecycle.Bind(func() error {
		return s.closeBoundConnection(conn, closer, lifecycle)
	}); errBind != nil {
		return errBind
	}
	if retained, ok := lifecycle.(interface{ Retain() }); ok {
		retained.Retain()
	}

	s.connMu.Lock()
	if s.conn != conn || s.connCloser != closer {
		s.connMu.Unlock()
		return fmt.Errorf("codex websockets executor: websocket connection closed during lifecycle bind")
	}
	previous := s.lifecycle
	s.lifecycle = lifecycle
	s.lifecycleModel = strings.TrimSpace(model)
	s.connMu.Unlock()
	if previous != nil && previous != lifecycle {
		previous.End("target_replaced")
	}
	return nil
}

func (s *codexWebsocketSession) closeBoundConnection(conn helps.WebSocketConn, closer *websocketConnectionCloser, lifecycle cliproxyexecutor.ExecutionLifecycle) error {
	if s == nil || conn == nil {
		return nil
	}
	s.detachConnection(conn, lifecycle)
	errClose := closer.Close()
	go lifecycle.End("connection_closed")
	return errClose
}

func (s *codexWebsocketSession) detachConnection(conn helps.WebSocketConn, lifecycle cliproxyexecutor.ExecutionLifecycle) *websocketConnectionCloser {
	if s == nil || conn == nil {
		return nil
	}
	s.connMu.Lock()
	var closer *websocketConnectionCloser
	matched := s.conn == conn
	if matched {
		closer = s.connCloser
		s.conn = nil
		s.routeKey = ""
		s.connCloser = nil
		s.multiAgentV2OptimizedConn = nil
		if s.readerConn == conn {
			s.readerConn = nil
		}
	}
	if (lifecycle == nil && matched) || (lifecycle != nil && s.lifecycle == lifecycle) {
		s.lifecycle = nil
		s.lifecycleModel = ""
	}
	s.connMu.Unlock()
	return closer
}

func closeWebsocketAfterBindFailure(sess *codexWebsocketSession, conn helps.WebSocketConn, closer *websocketConnectionCloser) {
	if conn == nil || closer == nil {
		return
	}
	if sess != nil {
		sess.detachConnection(conn, nil)
	}
	if errClose := closer.Close(); errClose != nil {
		log.Errorf("websockets executor: close lifecycle bind failure connection error: %v", errClose)
	}
}

func websocketSessionTargetChanged(sess *codexWebsocketSession, authID string, wsURL string) bool {
	if sess == nil {
		return false
	}

	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	if strings.TrimSpace(sess.authID) == "" && strings.TrimSpace(sess.wsURL) == "" {
		return false
	}
	return strings.TrimSpace(sess.authID) != strings.TrimSpace(authID) || strings.TrimSpace(sess.wsURL) != strings.TrimSpace(wsURL)
}

func existingWebsocketSessionConn(sess *codexWebsocketSession, authID string, wsURL string, routeKey ...string) (helps.WebSocketConn, *websocketConnectionCloser) {
	if sess == nil {
		return nil, nil
	}
	sess.connMu.Lock()
	conn := sess.conn
	closer := sess.connCloser
	matches := conn != nil && closer != nil &&
		strings.TrimSpace(sess.authID) == strings.TrimSpace(authID) &&
		strings.TrimSpace(sess.wsURL) == strings.TrimSpace(wsURL) &&
		(len(routeKey) == 0 || sess.routeKey == "" || sess.routeKey == routeKey[0])
	sess.connMu.Unlock()
	if !matches || sess.upstreamDisconnectError(conn) != nil {
		return nil, nil
	}
	return conn, closer
}

func detachMismatchedWebsocketSessionConn(sess *codexWebsocketSession, authID string, wsURL string, routeKey ...string) (helps.WebSocketConn, *websocketConnectionCloser, string, string, cliproxyexecutor.ExecutionLifecycle) {
	if sess == nil {
		return nil, nil, "", "", nil
	}

	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	conn := sess.conn
	if conn == nil || (strings.TrimSpace(sess.authID) == strings.TrimSpace(authID) && strings.TrimSpace(sess.wsURL) == strings.TrimSpace(wsURL) && (len(routeKey) == 0 || sess.routeKey == "" || sess.routeKey == routeKey[0])) {
		return nil, nil, "", "", nil
	}

	previousAuthID := sess.authID
	previousWSURL := sess.wsURL
	lifecycle := sess.lifecycle
	closer := sess.connCloser
	sess.lifecycle = nil
	sess.lifecycleModel = ""
	sess.conn = nil
	sess.routeKey = ""
	sess.connCloser = nil
	sess.multiAgentV2OptimizedConn = nil
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	return conn, closer, previousAuthID, previousWSURL, lifecycle
}

func configureRawCodexWebsocketConn(conn helps.WebSocketConn, authID string, wsURL string) {
	if conn == nil {
		return
	}
	conn.SetPingHandler(func(appData string) error {
		if helps.WebSocketAutoPong(conn) {
			return nil
		}
		log.Debugf("codex websockets: upstream ping received session= session_object=none ping_bytes=%d", len(appData))
		log.Debugf("codex websockets: upstream pong write started session= session_object=none")
		start := time.Now()
		errPong := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
		if errPong != nil {
			log.Warnf("codex websockets: upstream pong write failed session= session_object=none duration=%v err=%v", time.Since(start), errPong)
		} else {
			log.Debugf("codex websockets: upstream pong replied session= session_object=none duration=%v", time.Since(start))
		}
		return errPong
	})
}

func (s *codexWebsocketSession) setLastEventType(conn helps.WebSocketConn, eventType string) {
	if s == nil || conn == nil || eventType == "" {
		return
	}
	s.lastEventMu.Lock()
	if s.lastEventConn == conn {
		s.lastEventType = eventType
	}
	s.lastEventMu.Unlock()
}

func (s *codexWebsocketSession) getLastEventType(conn helps.WebSocketConn) string {
	if s == nil || conn == nil {
		return ""
	}
	s.lastEventMu.Lock()
	defer s.lastEventMu.Unlock()
	if s.lastEventConn != conn {
		return ""
	}
	return s.lastEventType
}

func newEphemeralCodexWebsocketSession() *codexWebsocketSession {
	return &codexWebsocketSession{
		sessionID:            "",
		upstreamDisconnectCh: make(chan error, 1),
	}
}

func (s *codexWebsocketSession) resetUpstreamDisconnectError(conn helps.WebSocketConn) {
	if s == nil || conn == nil {
		return
	}
	s.upstreamDisconnectErrMu.Lock()
	s.upstreamDisconnectErrConn = conn
	s.upstreamDisconnectErr = nil
	s.upstreamDisconnectErrMu.Unlock()

	s.lastEventMu.Lock()
	s.lastEventConn = conn
	s.lastEventType = ""
	s.lastEventMu.Unlock()
}

func (s *codexWebsocketSession) setUpstreamDisconnectError(conn helps.WebSocketConn, err error) {
	if s == nil || conn == nil || err == nil {
		return
	}
	s.upstreamDisconnectErrMu.Lock()
	if s.upstreamDisconnectErrConn == conn && s.upstreamDisconnectErr == nil {
		s.upstreamDisconnectErr = err
	}
	s.upstreamDisconnectErrMu.Unlock()
}

func (s *codexWebsocketSession) upstreamDisconnectError(conn helps.WebSocketConn) error {
	if s == nil || conn == nil {
		return nil
	}
	s.upstreamDisconnectErrMu.RLock()
	defer s.upstreamDisconnectErrMu.RUnlock()
	if s.upstreamDisconnectErrConn != conn {
		return nil
	}
	return s.upstreamDisconnectErr
}

func (s *codexWebsocketSession) notifyUpstreamDisconnect(err error) {
	if s == nil {
		return
	}
	s.upstreamDisconnectOnce.Do(func() {
		if s.upstreamDisconnectCh == nil {
			return
		}
		select {
		case s.upstreamDisconnectCh <- err:
		default:
		}
		close(s.upstreamDisconnectCh)
	})
}

func executionSessionIDFromOptions(opts cliproxyexecutor.Options) string {
	if len(opts.Metadata) == 0 {
		return ""
	}
	raw, ok := opts.Metadata[cliproxyexecutor.ExecutionSessionMetadataKey]
	if !ok || raw == nil {
		return ""
	}
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	default:
		return ""
	}
}

func (e *CodexWebsocketsExecutor) getOrCreateSession(sessionID string) *codexWebsocketSession {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	if e == nil {
		return nil
	}
	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.sessions == nil {
		store.sessions = make(map[string]*codexWebsocketSession)
	}
	if sess, ok := store.sessions[sessionID]; ok && sess != nil {
		return sess
	}
	sess := &codexWebsocketSession{
		sessionID:            sessionID,
		upstreamDisconnectCh: make(chan error, 1),
	}
	store.sessions[sessionID] = sess
	return sess
}

func (e *CodexWebsocketsExecutor) UpstreamDisconnectChan(sessionID string) <-chan error {
	sess := e.getOrCreateSession(sessionID)
	if sess == nil {
		return nil
	}
	return sess.upstreamDisconnectCh
}

func (e *CodexWebsocketsExecutor) ensureUpstreamConn(ctx context.Context, auth *cliproxyauth.Auth, sess *codexWebsocketSession, authID string, wsURL string, headers http.Header) (helps.WebSocketConn, *websocketConnectionCloser, *http.Response, error) {
	if sess == nil {
		conn, closer, resp, err := e.dialCodexWebsocket(ctx, auth, wsURL, headers)
		if conn != nil {
			configureRawCodexWebsocketConn(conn, authID, wsURL)
		}
		return conn, closer, resp, err
	}

	routeKey := helps.WebSocketRouteKey(e.cfg, auth, helps.FingerprintCodex)
	if staleConn, staleCloser, staleAuthID, staleWSURL, staleLifecycle := detachMismatchedWebsocketSessionConn(sess, authID, wsURL, routeKey); staleConn != nil {
		staleLastEvent := sess.getLastEventType(staleConn)
		logCodexWebsocketDisconnectedWithLastEvent(sess, sess.sessionID, staleAuthID, staleWSURL, "target_changed", staleLastEvent, nil)
		if staleCloser != nil {
			if errClose := staleCloser.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close stale websocket error: %v", errClose)
			}
		}
		if staleLifecycle != nil {
			staleLifecycle.End("target_changed")
		}
	}

	sess.connMu.Lock()
	conn := sess.conn
	closer := sess.connCloser
	readerConn := sess.readerConn
	sess.connMu.Unlock()
	if conn != nil {
		if readerConn != conn {
			sess.connMu.Lock()
			sess.readerConn = conn
			sess.connMu.Unlock()
			sess.configureConn(conn)
			go e.readUpstreamLoop(sess, conn)
		}
		logCodexWebsocketConnectedWithReused(sess, sess.sessionID, authID, wsURL, true)
		return conn, closer, nil, nil
	}

	conn, closer, resp, errDial := e.dialCodexWebsocket(ctx, auth, wsURL, headers)
	if errDial != nil {
		return nil, closer, resp, errDial
	}

	sess.connMu.Lock()
	if sess.conn != nil {
		previous := sess.conn
		previousCloser := sess.connCloser
		sess.connMu.Unlock()
		if errClose := closer.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
		logCodexWebsocketConnectedWithReused(sess, sess.sessionID, authID, wsURL, true)
		return previous, previousCloser, nil, nil
	}
	sess.conn = conn
	sess.connCloser = closer
	sess.multiAgentV2OptimizedConn = nil
	sess.wsURL = wsURL
	sess.routeKey = routeKey
	sess.authID = authID
	sess.readerConn = conn
	sess.connMu.Unlock()

	sess.configureConn(conn)
	go e.readUpstreamLoop(sess, conn)
	logCodexWebsocketConnectedWithReused(sess, sess.sessionID, authID, wsURL, false)
	return conn, closer, resp, nil
}

func (e *CodexWebsocketsExecutor) readUpstreamLoop(sess *codexWebsocketSession, conn helps.WebSocketConn) {
	if e == nil || sess == nil || conn == nil {
		return
	}
	for {
		_ = conn.SetReadDeadline(time.Now().Add(codexResponsesWebsocketIdleTimeout))
		msgType, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			invalidate := func() {
				e.invalidateUpstreamConn(sess, conn, "upstream_disconnected", errRead)
			}
			invalidated := false
			ch, done := sess.activeForConn(conn)
			if ch != nil {
				invalidated = sendTerminalWebsocketRead(ch, done, codexWebsocketRead{conn: conn, err: errRead}, invalidate)
				if sess.clearActive(conn, ch) {
					close(ch)
				}
			}
			if !invalidated {
				invalidate()
			}
			return
		}

		if msgType != websocket.TextMessage {
			if msgType == websocket.BinaryMessage {
				errBinary := fmt.Errorf("codex websockets executor: unexpected binary message")
				invalidate := func() {
					e.invalidateUpstreamConn(sess, conn, "unexpected_binary", errBinary)
				}
				invalidated := false
				ch, done := sess.activeForConn(conn)
				if ch != nil {
					invalidated = sendTerminalWebsocketRead(ch, done, codexWebsocketRead{conn: conn, err: errBinary}, invalidate)
					if sess.clearActive(conn, ch) {
						close(ch)
					}
				}
				if !invalidated {
					invalidate()
				}
				return
			}
			continue
		}

		payload = bytes.TrimSpace(payload)
		if len(payload) > 0 {
			eventType := gjson.GetBytes(payload, "type").String()
			if eventType != "" {
				sess.setLastEventType(conn, eventType)
				if strings.HasPrefix(eventType, "response.") || eventType == "error" {
					sess.noteActiveReply(conn)
				}
			}
		}

		ch, done := sess.activeForConn(conn)
		if ch == nil {
			continue
		}
		select {
		case ch <- codexWebsocketRead{conn: conn, msgType: msgType, payload: payload}:
		case <-done:
		}
	}
}

func (e *CodexWebsocketsExecutor) invalidateUpstreamConn(sess *codexWebsocketSession, conn helps.WebSocketConn, reason string, err error) {
	e.invalidateUpstreamConnWithNotify(sess, conn, reason, err, true)
}

func (e *CodexWebsocketsExecutor) invalidateUpstreamConnWithoutDisconnectNotify(sess *codexWebsocketSession, conn helps.WebSocketConn, reason string, err error) {
	e.invalidateUpstreamConnWithNotify(sess, conn, reason, err, false)
}

func (e *CodexWebsocketsExecutor) invalidateUpstreamConnWithNotify(sess *codexWebsocketSession, conn helps.WebSocketConn, reason string, err error, notify bool) {
	if sess == nil || conn == nil {
		return
	}

	sess.connMu.Lock()
	current := sess.conn
	authID := sess.authID
	wsURL := sess.wsURL
	sessionID := sess.sessionID
	if current == nil || current != conn {
		sess.connMu.Unlock()
		return
	}
	lifecycle := sess.lifecycle
	closer := sess.connCloser
	sess.lifecycle = nil
	sess.lifecycleModel = ""
	sess.conn = nil
	sess.routeKey = ""
	sess.connCloser = nil
	sess.multiAgentV2OptimizedConn = nil
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	sess.connMu.Unlock()

	lastEvent := sess.getLastEventType(conn)
	logCodexWebsocketDisconnectedWithLastEvent(sess, sessionID, authID, wsURL, reason, lastEvent, err)
	if notify {
		sess.notifyUpstreamDisconnect(err)
	}
	if closer != nil {
		if errClose := closer.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
	}
	if lifecycle != nil {
		lifecycle.End(reason)
	}
}

func (e *CodexWebsocketsExecutor) CloseExecutionSession(sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if e == nil {
		return
	}
	if sessionID == "" {
		return
	}
	if sessionID == cliproxyauth.CloseAllExecutionSessionsID {
		e.closeAllExecutionSessions("executor_shutdown")
		return
	}

	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	sess := store.sessions[sessionID]
	delete(store.sessions, sessionID)
	store.mu.Unlock()

	e.closeExecutionSession(sess, "session_closed")
}

func (e *CodexWebsocketsExecutor) closeAllExecutionSessions(reason string) {
	if e == nil {
		return
	}

	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	sessions := make([]*codexWebsocketSession, 0, len(store.sessions))
	for sessionID, sess := range store.sessions {
		delete(store.sessions, sessionID)
		if sess != nil {
			sessions = append(sessions, sess)
		}
	}
	store.mu.Unlock()

	for i := range sessions {
		e.closeExecutionSession(sessions[i], reason)
	}
}

func (e *CodexWebsocketsExecutor) closeExecutionSession(sess *codexWebsocketSession, reason string) {
	closeCodexWebsocketSession(sess, reason)
}

func closeCodexWebsocketSession(sess *codexWebsocketSession, reason string) {
	if sess == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "session_closed"
	}

	sess.connMu.Lock()
	conn := sess.conn
	authID := sess.authID
	wsURL := sess.wsURL
	lifecycle := sess.lifecycle
	closer := sess.connCloser
	sess.lifecycle = nil
	sess.lifecycleModel = ""
	sess.conn = nil
	sess.routeKey = ""
	sess.connCloser = nil
	sess.multiAgentV2OptimizedConn = nil
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	sessionID := sess.sessionID
	sess.connMu.Unlock()

	lastEvent := sess.getLastEventType(conn)
	if conn != nil {
		logCodexWebsocketDisconnectedWithLastEvent(sess, sessionID, authID, wsURL, reason, lastEvent, nil)
		if closer != nil {
			if errClose := closer.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
		}
	}
	if lifecycle != nil {
		lifecycle.End(reason)
	}
}

func sessionObjectKind(sess *codexWebsocketSession) string {
	if sess == nil {
		return "none"
	}
	if strings.TrimSpace(sess.sessionID) != "" {
		return "persistent"
	}
	return "ephemeral"
}

func logCodexWebsocketConnected(sessionID string, authID string, wsURL string) {
	logCodexWebsocketConnectedWithReused(nil, sessionID, authID, wsURL, false)
}

func logCodexWebsocketConnectedWithReused(sess *codexWebsocketSession, sessionID string, authID string, wsURL string, reused bool) {
	sessionStr := strings.TrimSpace(sessionID)
	sessionKind := sessionObjectKind(sess)
	if reused {
		log.Infof("codex websockets: upstream connected session=%s auth=%s url=%s session_object=%s reused=true", sessionStr, strings.TrimSpace(authID), strings.TrimSpace(wsURL), sessionKind)
		return
	}
	log.Infof("codex websockets: upstream connected session=%s auth=%s url=%s session_object=%s reused=false", sessionStr, strings.TrimSpace(authID), strings.TrimSpace(wsURL), sessionKind)
}

func logCodexWebsocketDisconnected(sessionID string, authID string, wsURL string, reason string, err error) {
	logCodexWebsocketDisconnectedWithLastEvent(nil, sessionID, authID, wsURL, reason, "", err)
}

func isTerminalEvent(eventType string) bool {
	switch eventType {
	case "response.completed", "response.done", "response.incomplete", "response.failed", "error":
		return true
	default:
		return false
	}
}

func logCodexWebsocketDisconnectedWithLastEvent(sess *codexWebsocketSession, sessionID string, authID string, wsURL string, reason string, lastEvent string, err error) {
	sessionStr := strings.TrimSpace(sessionID)
	sessionKind := sessionObjectKind(sess)
	terminalStatus := isTerminalEvent(lastEvent)
	if err != nil {
		if lastEvent != "" {
			log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s session_object=%s reason=%s last_event=%s is_terminal=%t err=%v", sessionStr, strings.TrimSpace(authID), strings.TrimSpace(wsURL), sessionKind, strings.TrimSpace(reason), lastEvent, terminalStatus, err)
			return
		}
		log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s session_object=%s reason=%s is_terminal=false err=%v", sessionStr, strings.TrimSpace(authID), strings.TrimSpace(wsURL), sessionKind, strings.TrimSpace(reason), err)
		return
	}
	if lastEvent != "" {
		log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s session_object=%s reason=%s last_event=%s is_terminal=%t", sessionStr, strings.TrimSpace(authID), strings.TrimSpace(wsURL), sessionKind, strings.TrimSpace(reason), lastEvent, terminalStatus)
		return
	}
	log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s session_object=%s reason=%s is_terminal=false", sessionStr, strings.TrimSpace(authID), strings.TrimSpace(wsURL), sessionKind, strings.TrimSpace(reason))
}

// CloseCodexWebsocketSessionsForAuthID closes all active Codex upstream websocket sessions
// associated with the supplied auth ID.
func CloseCodexWebsocketSessionsForAuthID(authID string, reason string) {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "auth_removed"
	}

	store := globalCodexWebsocketSessionStore
	if store == nil {
		return
	}

	type sessionItem struct {
		sessionID string
		sess      *codexWebsocketSession
	}

	store.mu.Lock()
	items := make([]sessionItem, 0, len(store.sessions))
	for sessionID, sess := range store.sessions {
		items = append(items, sessionItem{sessionID: sessionID, sess: sess})
	}
	store.mu.Unlock()

	matches := make([]sessionItem, 0)
	for i := range items {
		sess := items[i].sess
		if sess == nil {
			continue
		}
		sess.connMu.Lock()
		sessAuthID := strings.TrimSpace(sess.authID)
		sess.connMu.Unlock()
		if sessAuthID == authID {
			matches = append(matches, items[i])
		}
	}
	if len(matches) == 0 {
		return
	}

	toClose := make([]*codexWebsocketSession, 0, len(matches))
	store.mu.Lock()
	for i := range matches {
		current, ok := store.sessions[matches[i].sessionID]
		if !ok || current == nil || current != matches[i].sess {
			continue
		}
		delete(store.sessions, matches[i].sessionID)
		toClose = append(toClose, current)
	}
	store.mu.Unlock()

	for i := range toClose {
		closeCodexWebsocketSession(toClose[i], reason)
	}
}

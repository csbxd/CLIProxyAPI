package executor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/sjson"
)

const (
	codexResponsesWebsocketBetaHeaderValue = "responses_websockets=2026-02-06"
	codexResponsesWebsocketIdleTimeout     = 5 * time.Minute
	codexResponsesWebsocketHandshakeTO     = helps.WebSocketHandshakeTimeout
)

func (e *CodexWebsocketsExecutor) dialCodexWebsocket(ctx context.Context, auth *cliproxyauth.Auth, wsURL string, headers http.Header) (helps.WebSocketConn, *websocketConnectionCloser, *http.Response, error) {
	dialer, err := helps.NewFingerprintWebSocketDialer(e.cfg, auth, helps.FingerprintCodex)
	if err != nil {
		return nil, nil, nil, err
	}
	conn, resp, err := dialer.DialContext(ctx, wsURL, headers)
	if err != nil {
		cliproxyexecutor.MarkUpstreamAttempt(ctx)
	}
	closer := newWebsocketConnectionCloser(conn)
	return conn, closer, resp, err
}

func writeWebsocketPayloadMessage(provider string, sess *codexWebsocketSession, conn helps.WebSocketConn, payload []byte) error {
	provider = strings.TrimSpace(provider)
	if provider == "" {
		provider = "codex"
	}
	sessionID := ""
	if sess != nil {
		sessionID = sess.sessionID
	}
	sessionKind := sessionObjectKind(sess)
	payloadBytes := len(payload)
	start := time.Now()
	log.Debugf("%s websockets: write payload started session=%s session_object=%s bytes=%d", provider, sessionID, sessionKind, payloadBytes)
	var errSend error
	if sess != nil {
		errSend = sess.writeMessage(conn, websocket.TextMessage, payload)
	} else if conn == nil {
		errSend = fmt.Errorf("%s websockets executor: websocket conn is nil", provider)
	} else {
		errSend = conn.WriteMessage(websocket.TextMessage, payload)
	}
	if errSend != nil && sess != nil && sess.receivedActiveReply(conn) {
		// A response proves the upstream accepted this attempt. Preserve it in
		// the read channel instead of replaying after a concurrent socket close.
		log.Debugf("%s websockets: upstream response arrived before write completed session=%s", provider, sessionID)
		errSend = nil
	}
	if errSend != nil {
		log.Warnf("%s websockets: write payload failed session=%s session_object=%s bytes=%d duration=%v err=%v", provider, sessionID, sessionKind, payloadBytes, time.Since(start), errSend)
	} else {
		log.Debugf("%s websockets: write payload completed session=%s session_object=%s bytes=%d duration=%v", provider, sessionID, sessionKind, payloadBytes, time.Since(start))
	}
	return errSend
}

func writeCodexWebsocketMessage(sess *codexWebsocketSession, conn helps.WebSocketConn, payload []byte) error {
	return writeWebsocketPayloadMessage("codex", sess, conn, payload)
}

func mapCodexWebsocketWriteError(sess *codexWebsocketSession, conn helps.WebSocketConn, err error) error {
	if err == nil || sess == nil || conn == nil {
		return err
	}
	err = helps.ResolveWebSocketWriteError(conn, err)
	upstreamErr := sess.upstreamDisconnectError(conn)
	var closeErr *websocket.CloseError
	if !errors.As(upstreamErr, &closeErr) || closeErr.Code != websocket.CloseMessageTooBig {
		return err
	}
	return mapCodexWebsocketReadError(upstreamErr)
}

func shouldRetryCodexWebsocketSend(err error) bool {
	if err == nil {
		return false
	}
	var requestErr cliproxyexecutor.RequestScopedError
	return !errors.As(err, &requestErr) || !requestErr.IsRequestScoped()
}

type codexWebsocketMessageTooBigError struct {
	statusErr
}

func (codexWebsocketMessageTooBigError) IsRequestScoped() bool {
	return true
}

func mapCodexWebsocketReadError(err error) error {
	if err == nil {
		return nil
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) && closeErr.Code == websocket.CloseMessageTooBig {
		return codexWebsocketMessageTooBigError{statusErr: statusErr{
			code: http.StatusRequestEntityTooLarge,
			msg:  `{"error":{"message":"upstream websocket message too big","type":"invalid_request_error","code":"message_too_big"}}`,
		}}
	}
	return err
}

func normalizeCodexWebsocketParallelToolCalls(body []byte, headers http.Header) []byte {
	if !util.IsCodexResponsesLiteRequest(body, headers) {
		return body
	}
	body = helps.SetBoolIfDifferent(body, "parallel_tool_calls", false)
	return body
}

func buildCodexWebsocketRequestBody(body []byte) []byte {
	if len(body) == 0 {
		return nil
	}

	// Match codex-rs websocket v2 semantics: every request is `response.create`.
	// Incremental follow-up turns continue on the same websocket using
	// `previous_response_id` + incremental `input`, not `response.append`.
	body = helps.SanitizeCodexInputItemIDs(body)
	wsReqBody, errSet := sjson.SetBytes(body, "type", "response.create")
	if errSet == nil && len(wsReqBody) > 0 {
		return wsReqBody
	}
	return body
}

func readCodexWebsocketMessage(ctx context.Context, sess *codexWebsocketSession, conn helps.WebSocketConn, readCh chan codexWebsocketRead) (int, []byte, error) {
	if sess == nil {
		if conn == nil {
			return 0, nil, fmt.Errorf("codex websockets executor: websocket conn is nil")
		}
		_ = conn.SetReadDeadline(time.Now().Add(codexResponsesWebsocketIdleTimeout))
		msgType, payload, errRead := conn.ReadMessage()
		return msgType, payload, errRead
	}
	if conn == nil {
		return 0, nil, fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	if readCh == nil {
		return 0, nil, fmt.Errorf("codex websockets executor: session read channel is nil")
	}
	for {
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case ev, ok := <-readCh:
			if !ok {
				return 0, nil, fmt.Errorf("codex websockets executor: session read channel closed")
			}
			if ev.conn != conn {
				continue
			}
			if ev.err != nil {
				return 0, nil, ev.err
			}
			return ev.msgType, ev.payload, nil
		}
	}
}

func newProxyAwareWebsocketDialer(cfg *config.Config, auth *cliproxyauth.Auth) *websocket.Dialer {
	return helps.NewProxyAwareWebSocketDialer(cfg, auth)
}

func buildCodexResponsesWebsocketURL(httpURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(httpURL))
	if err != nil {
		return "", err
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	default:
		return "", fmt.Errorf("codex websockets executor: unsupported responses websocket URL scheme %q", parsed.Scheme)
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return "", fmt.Errorf("codex websockets executor: responses websocket URL host is empty")
	}
	return parsed.String(), nil
}

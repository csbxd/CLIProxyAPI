//go:build codex_rs

package executor

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestCodexRSExecutorPreservesDNSFailure(t *testing.T) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy", "CODEX_CA_CERTIFICATE", "SSL_CERT_FILE"} {
		t.Setenv(key, "")
	}
	for _, proxy := range []string{"", "direct"} {
		t.Run("proxy="+proxy, func(t *testing.T) {
			auth := &cliproxyauth.Auth{
				Provider: "codex", ProxyURL: proxy,
				Attributes: map[string]string{"base_url": "http://" + strings.Repeat("n", 64) + ".invalid"},
				Metadata:   map[string]any{"access_token": "fixture-token"},
			}
			executor := NewCodexExecutor(codexBufferingConfig(true))
			request, options := codexTestRequest()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			stream, err := executor.ExecuteStream(ctx, auth, request, options)
			var op *net.OpError
			var dns *net.DNSError
			if ctx.Err() != nil || stream != nil || !errors.As(err, &op) || !errors.As(err, &dns) {
				t.Fatalf("OAuth SSE lost DNS classification: %T %v (context: %v)", err, err, ctx.Err())
			}
		})
	}
}

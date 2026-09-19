//go:build !utls && !codex_rs

package helps

import (
	"net/http"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestFingerprintHTTPClientWithoutBackendTags(t *testing.T) {
	auth := &cliproxyauth.Auth{ProxyURL: "direct", Metadata: map[string]any{"access_token": "fixture"}}
	for _, fingerprint := range []Fingerprint{FingerprintNone, FingerprintCodex, FingerprintClaude} {
		client, errClient := NewFingerprintHTTPClient(t.Context(), nil, auth, fingerprint)
		if errClient != nil {
			t.Fatal(errClient)
		}
		if _, ok := client.Transport.(*http.Transport); !ok || client.Timeout != 0 {
			t.Fatalf("profile %d should use standard HTTP without backend tags: %T", fingerprint, client.Transport)
		}
	}
}

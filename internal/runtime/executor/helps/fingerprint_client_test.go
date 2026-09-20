package helps

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

type fingerprintTestRoundTripper func(*http.Request) (*http.Response, error)

func (f fingerprintTestRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestFingerprintHTTPClientContextOverride(t *testing.T) {
	for _, fingerprint := range []Fingerprint{FingerprintNone, FingerprintCodex, FingerprintClaude} {
		called := false
		transport := fingerprintTestRoundTripper(func(request *http.Request) (*http.Response, error) {
			called = true
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("fixture")), Request: request}, nil
		})
		ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", transport)
		auth := &cliproxyauth.Auth{Metadata: map[string]any{"access_token": "fixture"}}
		client, errClient := NewFingerprintHTTPClient(ctx, nil, auth, fingerprint)
		if errClient != nil {
			t.Fatal(errClient)
		}
		request, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, "https://chatgpt.com/backend-api/codex/responses", nil)
		if errRequest != nil {
			t.Fatal(errRequest)
		}
		response, errDo := client.Do(request)
		if errDo != nil {
			t.Fatal(errDo)
		}
		if errClose := response.Body.Close(); errClose != nil {
			t.Fatal(errClose)
		}
		if !called || client.Timeout != 0 {
			t.Fatalf("profile %d lost the injected transport or introduced a timeout", fingerprint)
		}
	}
}

func TestFingerprintHTTPClientNoneAndInvalidProfile(t *testing.T) {
	auth := &cliproxyauth.Auth{ProxyURL: "direct", Metadata: map[string]any{"access_token": "fixture"}}
	client, errClient := NewFingerprintHTTPClient(t.Context(), nil, auth, FingerprintNone)
	if errClient != nil {
		t.Fatal(errClient)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil || client.Timeout != 0 {
		t.Fatalf("None profile must use direct standard HTTP: %T", client.Transport)
	}
	if client, errClient := NewFingerprintHTTPClient(t.Context(), nil, auth, Fingerprint(255)); client != nil || errClient == nil {
		t.Fatal("invalid profile must return an error")
	}
}

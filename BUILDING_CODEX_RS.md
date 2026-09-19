# Codex Rust transport builds

The optional `codex_rs` Go build tag selects the pinned Codex Rust HTTP SDK for
Codex OAuth inference traffic through `github.com/csbxd/gocodex/httpclient`.
Executors call `helps.NewFingerprintHTTPClient` with a provider profile. The
profile expresses intent; build tags select the available transport for these
requests, including SSE, non-streaming requests that consume upstream SSE,
`/responses/compact`, and the Codex executor's generic HTTP request entry point.

```go
httpClient, err := helps.NewFingerprintHTTPClient(ctx, cfg, auth, helps.FingerprintCodex)
```

`Fingerprint` is a `uint8` enum defined with `iota`:

| Profile | No tags | `utls` | `codex_rs` | `codex_rs,utls` |
| --- | --- | --- | --- | --- |
| `FingerprintNone` | Standard HTTP | Standard HTTP | Standard HTTP | Standard HTTP |
| `FingerprintCodex` (OAuth) | Standard HTTP | ChatGPT Chrome uTLS | Codex Rust SDK | Codex Rust SDK |
| `FingerprintCodex` (API key) | Standard HTTP | ChatGPT Chrome uTLS | Standard HTTP | ChatGPT Chrome uTLS |
| `FingerprintClaude` | Standard HTTP | Claude Code uTLS | Standard HTTP | Claude Code uTLS |

uTLS profiles apply only to the matching provider's protected HTTPS hosts;
other destinations use standard HTTP. Unknown enum values return an error.
Proxy priority and explicit context transport overrides remain as described below.

## Build

Go 1.27 or later is required by the module. The `codex_rs` variant supports
Linux amd64 and arm64 with glibc 2.17 or later and cgo enabled. Install a C compiler;
Rust, Cargo, a Codex checkout, and OpenSSL development libraries are not needed.
The Go dependency includes both native static archives. Cross-compilation needs
a C compiler targeting the selected architecture, as with other cgo dependencies.

```sh
# Codex OAuth uses the Rust SDK; other providers use their existing transports.
CGO_ENABLED=1 go build -tags codex_rs -o cli-proxy-api ./cmd/server

# Also enable the existing uTLS transports for other applicable providers.
CGO_ENABLED=1 go build -tags 'codex_rs,utls' -o cli-proxy-api ./cmd/server

# Existing transport selection, without the Rust SDK linked into the executable.
go build -o cli-proxy-api ./cmd/server

docker build --build-arg BUILD_TAGS=codex_rs -t cli-proxy-api:codex-rs .
```

Do not enable `codex_rs` for `CGO_ENABLED=0`, musl/Alpine, Windows, macOS, or
other architectures. These targets retain the existing implementation when built
without the tag. The release workflows continue to build their existing variants;
the Docker build argument and local build commands opt into this variant.

## Routing and lifetime

- Only Codex OAuth credentials select the Rust transport. API-key credentials,
  OAuth login/token refresh, direct image endpoints, and WebSocket connections
  keep their existing implementations. HTTP fallback from WebSocket uses the
  same Codex OAuth HTTP transport.
- Proxy priority remains `auth.ProxyURL`, then global `proxy-url`, then an injected
  `cliproxy.roundtripper`. If none is supplied, the SDK uses its system/environment
  proxy policy. An injected transport intentionally overrides the SDK only when
  no explicit proxy setting exists.
- HTTP, HTTPS, SOCKS5 and SOCKS5h proxy URLs are supported, including URL-encoded
  credentials. `direct` and `none` force a direct connection. Unlike CPA's Go SOCKS
  dialer, the SDK's `socks5` resolves the destination locally; use `socks5h` for
  proxy-side DNS. Invalid explicit proxy settings return an error without silently
  falling back to another transport or a direct connection.
- Custom CAs use `CODEX_CA_CERTIFICATE`, falling back to `SSL_CERT_FILE`.
  Explicit routes load them when constructing a transport; automatic routing can
  report CA errors on its first request.
- Transports are shared through a bounded cache keyed by effective proxy and CA
  configuration. They do not retain OAuth tokens or install a Cookie jar. Each
  request still carries its own authorization headers.
- Clients and transports have no overall request timeout. Request contexts cancel
  upstream I/O. CPA closes each response body; cache eviction never calls
  `Transport.Close`, which would otherwise interrupt active SSE requests. Native
  pools are cleaned up after the transport and its active responses become
  unreachable and Go runs its cleanup.

The tag selects the pinned SDK's native TLS/HTTP implementation, not the previous
Chrome uTLS profile. Its wire fingerprint depends on the SDK revision, TLS backend
and target platform; it does not promise a byte-identical fingerprint to every
Codex CLI release or guarantee upstream acceptance. Existing CPA headers and SSE
translation remain in control of the executor.

## Validation

```sh
go test ./internal/runtime/executor/helps ./internal/runtime/executor
go test -tags codex_rs ./internal/runtime/executor/helps ./internal/runtime/executor
go test -tags 'codex_rs,utls' ./internal/runtime/executor/helps ./internal/runtime/executor
go test -race -tags codex_rs ./internal/runtime/executor/helps ./internal/runtime/executor
```

Tests use local HTTP/HTTPS and proxy fixtures with synthetic OAuth credentials.
They do not make authenticated requests to ChatGPT.

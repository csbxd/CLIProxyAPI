//go:build !http2

package api

import "net/http"

func configureHTTP2IfEnabled(*http.Server) error { return nil }

func httpServerNextProtos() []string {
	return []string{"http/1.1"}
}

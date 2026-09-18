//go:build http2

package api

import (
	"net/http"

	"golang.org/x/net/http2"
)

func configureHTTP2IfEnabled(server *http.Server) error {
	return http2.ConfigureServer(server, &http2.Server{})
}

func httpServerNextProtos() []string {
	return []string{"h2", "http/1.1"}
}

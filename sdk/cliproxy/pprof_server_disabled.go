//go:build !pprof

package cliproxy

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

// pprofServer is a no-op when the optional pprof feature is excluded.
type pprofServer struct{}

func newPprofServer() *pprofServer { return &pprofServer{} }

func (s *Service) applyPprofConfig(cfg *config.Config) {
	_ = s.applyPprofConfigContext(context.Background(), cfg)
}

func (s *Service) applyPprofConfigContext(ctx context.Context, cfg *config.Config) bool {
	if s == nil || cfg == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return false
	}
	if s.applyPprofConfigContextFn != nil {
		return s.applyPprofConfigContextFn(ctx, cfg)
	}
	return true
}

func (s *Service) shutdownPprof(context.Context) error { return nil }

func (*pprofServer) Apply(*config.Config) {}

func (p *pprofServer) ApplyContext(ctx context.Context, cfg *config.Config) bool {
	if p == nil || cfg == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx.Err() == nil
}

func (*pprofServer) Shutdown(context.Context) error { return nil }

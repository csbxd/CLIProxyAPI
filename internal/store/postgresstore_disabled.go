//go:build !postgres_store

package store

import (
	"context"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

// PostgresStoreConfig is retained so the server entry point remains source-compatible.
type PostgresStoreConfig struct {
	DSN           string
	Schema        string
	ConfigTable   string
	AuthTable     string
	CooldownTable string
	SpoolDir      string
}

// PostgresStore is unavailable when the PostgreSQL backend is excluded.
type PostgresStore struct{}

func NewPostgresStore(context.Context, PostgresStoreConfig) (*PostgresStore, error) {
	return nil, disabledStoreError("postgres store")
}

func (*PostgresStore) Close() error { return nil }
func (*PostgresStore) Bootstrap(context.Context, string) error {
	return disabledStoreError("postgres store")
}
func (*PostgresStore) ConfigPath() string { return "" }
func (*PostgresStore) AuthDir() string    { return "" }
func (*PostgresStore) WorkDir() string    { return "" }
func (*PostgresStore) SetBaseDir(string)  {}
func (*PostgresStore) Save(context.Context, *cliproxyauth.Auth) (string, error) {
	return "", disabledStoreError("postgres store")
}
func (*PostgresStore) List(context.Context) ([]*cliproxyauth.Auth, error) {
	return nil, disabledStoreError("postgres store")
}
func (*PostgresStore) Delete(context.Context, string) error {
	return disabledStoreError("postgres store")
}
func (*PostgresStore) CooldownStateStore() cliproxyauth.CooldownStateStore { return nil }

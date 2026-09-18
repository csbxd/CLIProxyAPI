//go:build !git_store

package store

import (
	"context"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

// GitTokenStore is unavailable when the Git backend is excluded.
type GitTokenStore struct{}

func NewGitTokenStore(string, string, string, string) *GitTokenStore {
	return &GitTokenStore{}
}

func (*GitTokenStore) SetBaseDir(string)       {}
func (*GitTokenStore) EnsureRepository() error { return disabledStoreError("git store") }
func (*GitTokenStore) ConfigPath() string      { return "" }
func (*GitTokenStore) AuthDir() string         { return "" }
func (*GitTokenStore) PersistConfig(context.Context) error {
	return disabledStoreError("git store")
}
func (*GitTokenStore) Save(context.Context, *cliproxyauth.Auth) (string, error) {
	return "", disabledStoreError("git store")
}
func (*GitTokenStore) List(context.Context) ([]*cliproxyauth.Auth, error) {
	return nil, disabledStoreError("git store")
}
func (*GitTokenStore) Delete(context.Context, string) error {
	return disabledStoreError("git store")
}

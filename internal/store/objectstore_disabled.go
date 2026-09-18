//go:build !object_store

package store

import (
	"context"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

// ObjectStoreConfig is retained so the server entry point remains source-compatible.
type ObjectStoreConfig struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
	Prefix    string
	LocalRoot string
	UseSSL    bool
	PathStyle bool
}

// ObjectTokenStore is unavailable when the object-storage backend is excluded.
type ObjectTokenStore struct{}

func NewObjectTokenStore(ObjectStoreConfig) (*ObjectTokenStore, error) {
	return nil, disabledStoreError("object store")
}

func (*ObjectTokenStore) SetBaseDir(string)  {}
func (*ObjectTokenStore) ConfigPath() string { return "" }
func (*ObjectTokenStore) AuthDir() string    { return "" }
func (*ObjectTokenStore) Bootstrap(context.Context, string) error {
	return disabledStoreError("object store")
}
func (*ObjectTokenStore) Save(context.Context, *cliproxyauth.Auth) (string, error) {
	return "", disabledStoreError("object store")
}
func (*ObjectTokenStore) List(context.Context) ([]*cliproxyauth.Auth, error) {
	return nil, disabledStoreError("object store")
}
func (*ObjectTokenStore) Delete(context.Context, string) error {
	return disabledStoreError("object store")
}

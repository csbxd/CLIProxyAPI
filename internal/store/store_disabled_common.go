//go:build !git_store || !object_store || !postgres_store

package store

import (
	"errors"
	"fmt"
)

var errRemoteStoreDisabled = errors.New("remote token store disabled by build tag")

func disabledStoreError(name string) error {
	return fmt.Errorf("%s: %w", name, errRemoteStoreDisabled)
}

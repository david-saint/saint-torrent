//go:build windows

package storage

import (
	"errors"
	"testing"
)

func TestMMapBackendUnsupportedOnWindows(t *testing.T) {
	if _, err := FactoryForBackend(BackendMMap); !errors.Is(err, errMMapUnsupported) {
		t.Fatalf("FactoryForBackend(BackendMMap) = %v, want errMMapUnsupported", err)
	}
}

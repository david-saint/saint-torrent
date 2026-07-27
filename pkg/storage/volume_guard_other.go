//go:build !darwin

package storage

import "os"

type externalVolumeGuard struct{}

func newExternalVolumeGuard(string, *os.Root) (*externalVolumeGuard, error) {
	return nil, nil
}

func (*externalVolumeGuard) validate() error {
	return nil
}

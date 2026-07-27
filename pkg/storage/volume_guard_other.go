//go:build !darwin

package storage

type externalVolumeGuard struct{}

func newExternalVolumeGuard(string, *DownloadRoot) (*externalVolumeGuard, error) {
	return nil, nil
}

func (*externalVolumeGuard) validate() error {
	return nil
}

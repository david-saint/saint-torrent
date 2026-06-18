//go:build windows

package storage

import "errors"

var errMMapUnsupported = errors.New("mmap storage is not supported on this platform")

func mmapFactory() (Factory, error) {
	return nil, errMMapUnsupported
}

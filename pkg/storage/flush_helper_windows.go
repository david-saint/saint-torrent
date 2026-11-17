package storage

import (
	"errors"
	"os"
)

// os/exec cannot hand extra descriptors to a child on Windows, so flushes stay
// in-process there.
const flushHelperSupported = false

func spawnFlushHelper(string, []*os.File) (func() error, error) {
	return nil, errors.New("flush helper unsupported on windows")
}

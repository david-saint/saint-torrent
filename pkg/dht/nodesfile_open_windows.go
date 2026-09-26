//go:build windows

package dht

import "os"

// openNodesFile is a plain open on Windows, which has no O_NOFOLLOW or FIFOs
// in the file system; creating a symlink there needs elevated rights, and the
// regular-file and same-file checks after the open still apply.
func openNodesFile(path string) (*os.File, error) {
	return os.Open(path)
}

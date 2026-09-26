//go:build !windows

package torrent

import (
	"os"
	"syscall"
)

// openForRead opens path read-only without blocking on a FIFO: a plain open
// of a FIFO waits for a writer, possibly forever, while O_NONBLOCK returns at
// once so the caller can refuse it. O_NONBLOCK has no effect on a regular
// file, which Go does not add to the poller. Symlinks are followed: users
// pass symlinked .torrent files.
func openForRead(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}

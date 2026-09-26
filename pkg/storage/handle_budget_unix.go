//go:build unix

package storage

import "golang.org/x/sys/unix"

// evictCachedHandles is true where closing a handle another goroutine may be
// using is safe: the close does not wait for the I/O, which fails with
// os.ErrClosed and is retried (see handleBudget).
const evictCachedHandles = true

// handleLimit derives the cached-handle limit from the soft RLIMIT_NOFILE,
// which the Go runtime has already raised to the hard limit (on macOS, to
// kern.maxfilesperproc) by the time a storage opens its first file.
func handleLimit() int64 {
	var rlimit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rlimit); err != nil {
		return minHandleBudget
	}
	return handleLimitFor(uint64(rlimit.Cur))
}

//go:build linux

package downloader

import (
	"os"

	"golang.org/x/sys/unix"
)

// evictFromCache drops a file's pages so the next read reaches the device.
func evictFromCache(f *os.File) {
	_ = f.Sync()
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}

const cacheEvictionSupported = true

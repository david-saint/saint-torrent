//go:build !linux

package downloader

import "os"

// evictFromCache cannot drop the page cache here (macOS has no posix_fadvise, and
// F_NOCACHE only affects reads through the descriptor it is set on, which the
// storage layer opens for itself). Size the corpus past RAM, or run `sudo purge`
// between policies, to keep the numbers honest.
func evictFromCache(f *os.File) { _ = f.Sync() }

const cacheEvictionSupported = false

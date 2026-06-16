//go:build !windows

package downloader

import (
	"errors"
	"os"
	"syscall"
)

func isDirectoryNotEmpty(err error) bool {
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) {
		return false
	}
	errno, ok := pathErr.Err.(syscall.Errno)
	return ok && errno == syscall.ENOTEMPTY
}

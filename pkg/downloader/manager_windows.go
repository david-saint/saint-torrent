//go:build windows

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
	// 145 is ERROR_DIR_NOT_EMPTY on Windows
	return ok && errno == 145
}

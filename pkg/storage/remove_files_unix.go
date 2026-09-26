//go:build !windows

package storage

import (
	"errors"
	"syscall"
)

// isDirNotEmpty reports whether removing a directory failed because it still
// has entries. POSIX lets rmdir report that as EEXIST as well as ENOTEMPTY.
func isDirNotEmpty(err error) bool {
	return errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST)
}

//go:build !windows

package storage

import (
	"errors"
	"os"
	"syscall"
)

// isDirNotEmpty reports whether removing a directory failed because it still
// has entries. POSIX lets rmdir report that as EEXIST as well as ENOTEMPTY.
func isDirNotEmpty(err error) bool {
	return errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST)
}

// openRegularReadOnly opens name inside dir for reading without blocking on a
// FIFO if a regular file was swapped for one between Lstat and Open.
func openRegularReadOnly(dir *os.Root, name string) (*os.File, error) {
	return dir.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}

package storage

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// isDirNotEmpty reports whether removing a directory failed because it still
// has entries.
func isDirNotEmpty(err error) bool {
	return errors.Is(err, windows.ERROR_DIR_NOT_EMPTY)
}

func openRegularReadOnly(dir *os.Root, name string) (*os.File, error) {
	return dir.Open(name)
}

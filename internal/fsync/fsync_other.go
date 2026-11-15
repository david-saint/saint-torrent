//go:build !darwin

package fsync

import "os"

// Elsewhere (*os.File).Sync already enters the system call properly.
func file(f *os.File) error {
	return f.Sync()
}

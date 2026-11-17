//go:build !darwin

package fsync

import "os"

// Elsewhere (*os.File).Sync already enters the system call properly.
func file(f *os.File) error {
	return f.Sync()
}

// Elsewhere a plain Sync is already the cheapest flush that writes the data.
func writeBack(f *os.File) error {
	return f.Sync()
}

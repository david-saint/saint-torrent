//go:build windows

package torrent

import "os"

// openForRead opens path read-only. Opening a named pipe on Windows does not
// wait for the other end, so no flag is needed.
func openForRead(path string) (*os.File, error) {
	return os.Open(path)
}

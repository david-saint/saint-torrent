//go:build !unix

package torrent

import "os"

// openForRead opens path read-only. Opening a named pipe on Windows does not
// wait for the other end, so no flag is needed; the other non-unix ports
// (plan9, js, wasip1) are left with a plain open too, since js and wasip1
// have no syscall.O_NONBLOCK.
func openForRead(path string) (*os.File, error) {
	return os.Open(path)
}

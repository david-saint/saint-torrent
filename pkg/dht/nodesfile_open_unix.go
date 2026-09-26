//go:build !windows

package dht

import (
	"os"
	"syscall"
)

// openNodesFile opens the nodes file for reading without following a symlink
// in its final component. O_NONBLOCK keeps a FIFO swapped in after the Lstat
// check from blocking the open, and so DHT startup, until some writer shows
// up; it has no effect on reads of a regular file.
func openNodesFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}

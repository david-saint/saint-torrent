//go:build !windows

package storage

import (
	"os"

	"golang.org/x/sys/unix"
)

func rootOpenNoFollow(root *os.Root, path string, flag int, perm os.FileMode) (*os.File, error) {
	return root.OpenFile(path, flag|unix.O_NOFOLLOW, perm)
}

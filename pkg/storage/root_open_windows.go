//go:build windows

package storage

import "os"

func rootOpenNoFollow(root *os.Root, path string, flag int, perm os.FileMode) (*os.File, error) {
	return root.OpenFile(path, flag, perm)
}

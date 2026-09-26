//go:build windows

package logging

import "os"

// openNoFollow is a plain open on Windows, which has no O_NOFOLLOW; creating
// symlinks there needs elevated rights, and the regular-file check still
// applies.
func openNoFollow(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag, perm)
}

// checkOwner is a no-op: Windows exposes no Unix owner or link count here.
func checkOwner(os.FileInfo) error {
	return nil
}

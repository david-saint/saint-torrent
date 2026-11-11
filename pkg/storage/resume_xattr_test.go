//go:build darwin || linux

package storage

import (
	"runtime"
	"testing"

	"golang.org/x/sys/unix"
)

// setLastUsedXattr writes the attribute macOS records when a file is opened (a
// user-namespace stand-in on Linux). A file system without xattr support skips
// it: the chmod beside it already moves the change timestamp.
func setLastUsedXattr(t *testing.T, path string) {
	t.Helper()
	name := "com.apple.lastuseddate#PS"
	if runtime.GOOS == "linux" {
		name = "user.sainttorrent.lastused"
	}
	if err := unix.Setxattr(path, name, []byte{1, 2, 3, 4, 5, 6, 7, 8}, 0); err != nil {
		t.Logf("setxattr %s: %v (skipped)", name, err)
	}
}

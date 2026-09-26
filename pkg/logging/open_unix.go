//go:build !windows

package logging

import (
	"fmt"
	"os"
	"syscall"
)

// openNoFollow opens path without following a symlink in its final
// component, so a link planted at a predictable log path cannot redirect our
// writes into another file. O_NONBLOCK keeps a planted FIFO from blocking the
// open; it has no effect on regular files.
func openNoFollow(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, perm)
}

// checkOwner refuses a file that is not ours or has more than one link. The
// link count catches a hard link to another of our files (for example a shell
// rc file), which O_NOFOLLOW cannot see.
func checkOwner(fi os.FileInfo) error {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot read file owner")
	}
	if st.Uid != uint32(os.Getuid()) {
		return fmt.Errorf("owned by uid %d, not the current user", st.Uid)
	}
	if st.Nlink != 1 {
		return fmt.Errorf("has %d hard links", st.Nlink)
	}
	return nil
}

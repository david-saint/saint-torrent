//go:build !windows

package storage

import (
	"fmt"
	"os"
	"reflect"
	"syscall"

	"golang.org/x/sys/unix"
)

const resumeMetadataReadFlags = os.O_RDONLY | unix.O_NONBLOCK

func fileIdentity(_ *os.File, info os.FileInfo) string {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	// The change timestamp catches in-place edits even if a tool restores mtime.
	// Stat_t names this field Ctim on Linux and Ctimespec on BSD/macOS.
	value := reflect.ValueOf(st).Elem()
	change := value.FieldByName("Ctim")
	if !change.IsValid() {
		change = value.FieldByName("Ctimespec")
	}
	if !change.IsValid() {
		return ""
	}
	return fmt.Sprintf("%d:%d:%v", st.Dev, st.Ino, change.Interface())
}

func replaceResumeFile(root *DownloadRoot, oldName, newName string, durable bool) error {
	directory, err := root.OpenFile(".", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err = unix.Renameat(int(directory.Fd()), oldName, int(directory.Fd()), newName); err != nil {
		return err
	}
	if durable {
		return directory.Sync()
	}
	return nil
}

func sameFileVersion(a, b os.FileInfo) bool {
	return b != nil && os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime()) && fileIdentity(nil, a) == fileIdentity(nil, b)
}

func fileObjectIdentity(_ *os.File, info os.FileInfo) string {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%d:%d", st.Dev, st.Ino)
}

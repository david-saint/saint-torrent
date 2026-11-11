//go:build !windows

package storage

import (
	"fmt"
	"os"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	resumeMetadataReadFlags = os.O_RDONLY | unix.O_NONBLOCK
	// fsync works on a read-only descriptor here, so a checkpoint never needs write
	// access to a payload file it only flushes.
	resumeSyncOpenFlags = os.O_RDONLY | unix.O_NONBLOCK
)

// resumeIdentityMode names the per-file identity a checkpoint records. On Unix it
// is device, inode and exact modification time, which with the size the
// checkpoint also compares is what libtorrent and Transmission trust. The change
// timestamp is deliberately left out: macOS moves it whenever a file is merely
// opened (LaunchServices records com.apple.lastuseddate#PS as an extended
// attribute), so every video a user watched was re-hashed on the next launch.
const resumeIdentityMode = "mtime"

// legacyIdentityMode is the mode checkpoints carried while the identity included
// the change timestamp ("dev:ino:ctime"). They are still honoured, by device and
// inode alone, so upgrading costs no recheck.
const legacyIdentityMode = "change_time"

func acceptsIdentityMode(mode string) bool {
	return mode == resumeIdentityMode || mode == legacyIdentityMode
}

func fileIdentity(_ *os.File, info os.FileInfo) string {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%d:%d:%d", st.Dev, st.Ino, info.ModTime().UnixNano())
}

// identityMatches reports whether a checkpoint's saved identity, recorded under
// mode, still describes the file open at h. The caller compares size and
// modification time separately, so a legacy identity only has to name the same
// device and inode.
func identityMatches(mode, saved string, h *os.File, info os.FileInfo) bool {
	if saved == "" {
		return false
	}
	if mode != legacyIdentityMode {
		return saved == fileIdentity(h, info)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	return strings.Count(saved, ":") == 2 && strings.HasPrefix(saved, fmt.Sprintf("%d:%d:", st.Dev, st.Ino))
}

// fileObjectKeyOf keys a file by device and inode, which every name of one
// file shares, and reports its link count. An inode of zero (some FUSE and
// network filesystems) names no file in particular and is reported as unknown
// rather than as a collision.
func fileObjectKeyOf(_ *os.File, info os.FileInfo) (fileObjectKey, uint64, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Ino == 0 {
		return fileObjectKey{}, 0, false
	}
	return fileObjectKey{a: uint64(st.Dev), b: uint64(st.Ino)}, uint64(st.Nlink), true
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

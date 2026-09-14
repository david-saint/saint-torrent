package storage

import (
	"errors"
	"os"
)

var errUnsafeRootPath = errors.New("path is a symbolic link or changed while opening")

// rootOpenNoFollow opens path beneath root and verifies that the directory entry
// still names the object that was opened. os.Root prevents escapes but permits
// symlinks that resolve inside the root; the identity check rejects those too.
// O_TRUNC is deliberately applied only after validation so a raced symlink can
// never truncate its target before being rejected.
func rootOpenNoFollow(root *DownloadRoot, path string, flag int, perm os.FileMode) (*os.File, error) {
	f, _, err := rootOpenNoFollowInfo(root, path, flag, perm)
	return f, err
}

// rootOpenNoFollowInfo is rootOpenNoFollow plus the FileInfo it already had to
// stat for the identity check, so metadata callers need no second stat syscall.
func rootOpenNoFollowInfo(root *DownloadRoot, path string, flag int, perm os.FileMode) (*os.File, os.FileInfo, error) {
	rawRoot, release, err := root.acquire()
	if err != nil {
		return nil, nil, err
	}
	if release {
		defer rawRoot.Close()
	}
	openFlag := flag &^ os.O_TRUNC
	f, err := rawRoot.OpenFile(path, openFlag, perm)
	if err != nil {
		return nil, nil, err
	}

	pathInfo, pathErr := rawRoot.Lstat(path)
	openedInfo, openedErr := f.Stat()
	if pathErr != nil || openedErr != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(pathInfo, openedInfo) {
		_ = f.Close()
		if pathErr != nil {
			return nil, nil, pathErr
		}
		if openedErr != nil {
			return nil, nil, openedErr
		}
		return nil, nil, &os.PathError{Op: "open", Path: path, Err: errUnsafeRootPath}
	}

	if flag&os.O_TRUNC != 0 {
		if err := f.Truncate(0); err != nil {
			_ = f.Close()
			return nil, nil, err
		}
		if openedInfo, err = f.Stat(); err != nil {
			_ = f.Close()
			return nil, nil, err
		}
	}
	return f, openedInfo, nil
}

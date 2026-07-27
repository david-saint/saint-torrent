package storage

import (
	"fmt"
	"os"
)

// DownloadRoot anchors storage operations to the directory that was verified
// when the torrent was opened. Unix keeps the root handle for the storage
// lifetime. Windows reopens it per control-path operation because Go 1.24's
// os.OpenRoot handle does not share delete access, then verifies file identity
// before use so the path cannot be redirected to a replacement directory.
type DownloadRoot struct {
	root       *os.Root
	reopenPath string
	identity   os.FileInfo
}

// OpenDownloadRoot creates a download directory when it is safe to do so and
// returns an anchored root handle for subsequent control-path operations.
// Platform implementations protect removable-volume mount points from path
// races while the directory is created/opened.
func OpenDownloadRoot(path string) (*DownloadRoot, error) {
	root, err := openPlatformDownloadRoot(path)
	if err != nil {
		return nil, err
	}
	identity, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	reopenPath, reopen, err := platformDownloadRootReopenPath(path)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	result := &DownloadRoot{root: root, identity: identity}
	if reopen {
		if err := root.Close(); err != nil {
			return nil, err
		}
		result.root = nil
		result.reopenPath = reopenPath
	}
	return result, nil
}

func (r *DownloadRoot) acquire() (*os.Root, bool, error) {
	if r.root != nil {
		return r.root, false, nil
	}
	root, err := os.OpenRoot(r.reopenPath)
	if err != nil {
		return nil, false, err
	}
	info, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return nil, false, err
	}
	if !os.SameFile(info, r.identity) {
		_ = root.Close()
		return nil, false, fmt.Errorf("download root changed identity: %s", r.reopenPath)
	}
	return root, true, nil
}

func (r *DownloadRoot) OpenFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	root, release, err := r.acquire()
	if err != nil {
		return nil, err
	}
	f, err := root.OpenFile(path, flag, perm)
	if release {
		_ = root.Close()
	}
	return f, err
}

func (r *DownloadRoot) Stat(path string) (os.FileInfo, error) {
	root, release, err := r.acquire()
	if err != nil {
		return nil, err
	}
	if release {
		defer root.Close()
	}
	return root.Stat(path)
}

func (r *DownloadRoot) Lstat(path string) (os.FileInfo, error) {
	root, release, err := r.acquire()
	if err != nil {
		return nil, err
	}
	if release {
		defer root.Close()
	}
	return root.Lstat(path)
}

func (r *DownloadRoot) Remove(path string) error {
	root, release, err := r.acquire()
	if err != nil {
		return err
	}
	if release {
		defer root.Close()
	}
	return root.Remove(path)
}

func (r *DownloadRoot) Close() error {
	if r.root == nil {
		return nil
	}
	return r.root.Close()
}

func mkdirAllInRoot(root *DownloadRoot, path string, perm os.FileMode) error {
	if path == "" || path == "." {
		return nil
	}
	rawRoot, release, err := root.acquire()
	if err != nil {
		return err
	}
	if release {
		defer rawRoot.Close()
	}
	current := rawRoot
	for _, component := range splitPathComponents(path) {
		next, err := current.OpenRoot(component)
		if os.IsNotExist(err) {
			if err := current.Mkdir(component, perm); err != nil && !os.IsExist(err) {
				if current != rawRoot {
					_ = current.Close()
				}
				return err
			}
			next, err = current.OpenRoot(component)
		}
		if current != rawRoot {
			_ = current.Close()
		}
		if err != nil {
			return err
		}
		current = next
	}
	if current != rawRoot {
		return current.Close()
	}
	return nil
}

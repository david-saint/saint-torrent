//go:build windows

package storage

import "path/filepath"

func platformDownloadRootReopenPath(path string) (string, bool, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", false, err
	}
	absPath, err := filepath.Abs(resolved)
	return absPath, true, err
}

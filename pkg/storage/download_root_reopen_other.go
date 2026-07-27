//go:build !windows

package storage

func platformDownloadRootReopenPath(string) (string, bool, error) {
	return "", false, nil
}

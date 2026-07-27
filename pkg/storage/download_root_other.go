//go:build !darwin

package storage

import (
	"os"
	"path/filepath"
	"strings"
)

func openPlatformDownloadRoot(path string) (*os.Root, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	return os.OpenRoot(path)
}

func splitPathComponents(path string) []string {
	return strings.FieldsFunc(filepath.Clean(path), func(r rune) bool {
		return r == filepath.Separator
	})
}

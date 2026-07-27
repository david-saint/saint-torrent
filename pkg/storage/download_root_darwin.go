//go:build darwin

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func openPlatformDownloadRoot(path string) (*os.Root, error) {
	cleanPath, err := canonicalDownloadPath(path)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel("/Volumes", cleanPath)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		if err := os.MkdirAll(cleanPath, 0755); err != nil {
			return nil, err
		}
		return os.OpenRoot(cleanPath)
	}

	parts := splitPathComponents(rel)
	if len(parts) == 0 {
		return nil, fmt.Errorf("invalid download path %q", path)
	}
	volumeRoot := filepath.Join("/Volumes", parts[0])
	expectedDevice, err := mountedVolumeDevice(volumeRoot)
	if err != nil {
		return nil, err
	}

	root, err := os.OpenRoot(volumeRoot)
	if err != nil {
		return nil, fmt.Errorf("open volume root: %w", err)
	}
	if err := validateOpenedRootDevice(root, expectedDevice, parts[0]); err != nil {
		_ = root.Close()
		return nil, err
	}
	current := root
	for _, component := range parts[1:] {
		if err := current.Mkdir(component, 0755); err != nil && !os.IsExist(err) {
			_ = current.Close()
			return nil, fmt.Errorf("create download directory: %w", err)
		}
		next, err := current.OpenRoot(component)
		_ = current.Close()
		if err != nil {
			return nil, fmt.Errorf("open download directory: %w", err)
		}
		current = next
	}
	if err := validateOpenedRootDevice(current, expectedDevice, parts[0]); err != nil {
		_ = current.Close()
		return nil, err
	}
	return current, nil
}

func validateOpenedRootDevice(root *os.Root, expectedDevice uint64, volumeName string) error {
	openedInfo, err := root.Stat(".")
	if err != nil {
		return fmt.Errorf("inspect opened download directory: %w", err)
	}
	openedStat, ok := openedInfo.Sys().(*syscall.Stat_t)
	if !ok || uint64(openedStat.Dev) != expectedDevice {
		return fmt.Errorf("download root is no longer on volume %q", volumeName)
	}
	return nil
}

func canonicalDownloadPath(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	parts := splitPathComponents(absPath)
	if len(parts) > 0 && strings.EqualFold(parts[0], "Volumes") {
		absPath = filepath.Join(append([]string{"/Volumes"}, parts[1:]...)...)
	}

	probe := absPath
	var suffix []string
	for {
		resolved, err := filepath.EvalSymlinks(probe)
		if err == nil {
			return filepath.Join(append([]string{resolved}, suffix...)...), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return "", err
		}
		suffix = append([]string{filepath.Base(probe)}, suffix...)
		probe = parent
	}
}

func splitPathComponents(path string) []string {
	return strings.FieldsFunc(filepath.Clean(path), func(r rune) bool {
		return r == filepath.Separator
	})
}

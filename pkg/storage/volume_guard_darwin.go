//go:build darwin

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type externalVolumeGuard struct {
	rootPath string
	device   uint64
}

func newExternalVolumeGuard(path string) (*externalVolumeGuard, error) {
	cleanPath, err := canonicalDownloadPath(path)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel("/Volumes", cleanPath)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, nil
	}
	parts := splitPathComponents(rel)
	if len(parts) == 0 {
		return nil, fmt.Errorf("invalid external volume path %q", path)
	}
	rootPath := filepath.Join("/Volumes", parts[0])
	device, err := mountedVolumeDevice(rootPath)
	if err != nil {
		return nil, err
	}
	return &externalVolumeGuard{rootPath: rootPath, device: device}, nil
}

func (g *externalVolumeGuard) validate() error {
	if g == nil {
		return nil
	}
	device, err := mountedVolumeDevice(g.rootPath)
	if err != nil {
		return err
	}
	if device != g.device {
		return fmt.Errorf("external volume at %s changed devices", g.rootPath)
	}
	return nil
}

func mountedVolumeDevice(rootPath string) (uint64, error) {
	rootInfo, err := os.Stat(rootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, fmt.Errorf("external volume at %s is not mounted", rootPath)
		}
		return 0, fmt.Errorf("inspect external volume %s: %w", rootPath, err)
	}
	volumesInfo, err := os.Stat("/Volumes")
	if err != nil {
		return 0, fmt.Errorf("inspect /Volumes: %w", err)
	}
	rootStat, rootOK := rootInfo.Sys().(*syscall.Stat_t)
	volumesStat, volumesOK := volumesInfo.Sys().(*syscall.Stat_t)
	if !rootOK || !volumesOK {
		return 0, fmt.Errorf("inspect device identity for %s", rootPath)
	}
	if rootStat.Dev == volumesStat.Dev {
		return 0, fmt.Errorf("external volume at %s is not mounted", rootPath)
	}
	return uint64(rootStat.Dev), nil
}

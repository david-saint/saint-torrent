//go:build darwin

package storage

import (
	"os"
	"strings"
	"syscall"
	"testing"
)

func TestOpenDownloadRootRejectsMissingVolume(t *testing.T) {
	_, err := OpenDownloadRoot("/Volumes/sainttorrent-definitely-not-mounted/downloads")
	if err == nil || !strings.Contains(err.Error(), "not mounted") {
		t.Fatalf("expected missing-volume error, got %v", err)
	}
}

func TestValidateOpenedRootDevice(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	info, err := root.Stat(".")
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("root stat did not expose device identity")
	}
	device := uint64(stat.Dev)
	if err := validateOpenedRootDevice(root, device, "test-volume"); err != nil {
		t.Fatalf("matching device rejected: %v", err)
	}
	if err := validateOpenedRootDevice(root, device+1, "test-volume"); err == nil {
		t.Fatal("expected mismatched device to be rejected")
	}
}

func TestExternalVolumeGuardValidatesHandleAndMountPath(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	info, err := root.Stat(".")
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	device := uint64(stat.Dev)
	guard := &externalVolumeGuard{
		rootPath: "test-volume",
		root:     root,
		device:   device,
		mountedDevice: func(string) (uint64, error) {
			return device, nil
		},
	}
	if err := guard.validate(); err != nil {
		t.Fatalf("matching handle and path rejected: %v", err)
	}
	guard.mountedDevice = func(string) (uint64, error) { return device + 1, nil }
	if err := guard.validate(); err == nil {
		t.Fatal("expected mount-path device mismatch to be rejected")
	}
}

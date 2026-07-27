//go:build darwin

package storage

import (
	"strings"
	"testing"
)

func TestOpenDownloadRootRejectsMissingVolume(t *testing.T) {
	_, err := OpenDownloadRoot("/Volumes/sainttorrent-definitely-not-mounted/downloads")
	if err == nil || !strings.Contains(err.Error(), "not mounted") {
		t.Fatalf("expected missing-volume error, got %v", err)
	}
}

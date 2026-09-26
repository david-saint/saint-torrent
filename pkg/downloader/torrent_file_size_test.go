package downloader

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAddTorrentFileRefusesOversizedFile: AddTorrentFile read a .torrent of any
// size into memory and decoded all of it into a tree, tens of times larger,
// before dropping unknown keys. A padded torrent that still parsed was cached
// and cost the same again on every launch.
func TestAddTorrentFileRefusesOversizedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "huge.torrent")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	// Sparse: refusing it must not require reading it.
	if err := f.Truncate(maxTorrentFileSize + 1); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	mgr := NewTorrentManager()
	defer mgr.Close()
	if _, err := mgr.AddTorrentFile(path, t.TempDir()); err == nil || !strings.Contains(err.Error(), "larger than the maximum") {
		t.Fatalf("AddTorrentFile of a %d-byte file = %v, want a size error", maxTorrentFileSize+1, err)
	}
}

// TestReadTorrentFileReturnsWholeFile: a torrent under the bound is read
// exactly.
func TestReadTorrentFileReturnsWholeFile(t *testing.T) {
	data, _ := testTorrent(t, "small.bin", false)
	path := filepath.Join(t.TempDir(), "small.torrent")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := readTorrentFile(path)
	if err != nil || string(got) != string(data) {
		t.Fatalf("readTorrentFile = %d bytes, %v; want the %d-byte file", len(got), err, len(data))
	}
}

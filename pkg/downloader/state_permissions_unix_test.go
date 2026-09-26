//go:build !windows

package downloader

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPersistedStateIsPrivate: session.json and the cached .torrent files
// (which carry private-tracker passkeys in their announce URLs) were written
// 0644 into directories created 0755, and restore-failures.log was 0644. With
// --config-dir or a custom XDG_CONFIG_HOME, other local users could read them.
func TestPersistedStateIsPrivate(t *testing.T) {
	configDir := filepath.Join(t.TempDir(), "config")
	torrentsDir := filepath.Join(configDir, "torrents")
	// A torrents directory an older version created world-readable.
	if err := os.MkdirAll(torrentsDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(torrentsDir, 0755); err != nil {
		t.Fatal(err)
	}
	// An entry that cannot be restored, so restore-failures.log is written.
	state, err := json.Marshal(PersistedState{Version: 1, Torrents: []PersistedTorrent{{InfoHashHex: strings.Repeat("ab", 20), DownloadDir: t.TempDir()}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "session.json"), state, 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewTorrentManager()
	defer mgr.Close()
	if _, err := mgr.EnablePersistence(configDir); err != nil {
		t.Fatal(err)
	}
	data, infoHash := testTorrent(t, "passkey.bin", false)
	torrentPath := filepath.Join(t.TempDir(), "passkey.torrent")
	if err := os.WriteFile(torrentPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.AddTorrentFile(torrentPath, t.TempDir()); err != nil {
		t.Fatal(err)
	}

	for path, want := range map[string]os.FileMode{
		torrentsDir:                                                     0700,
		filepath.Join(configDir, "session.json"):                        0600,
		filepath.Join(configDir, "restore-failures.log"):                0600,
		filepath.Join(torrentsDir, fmt.Sprintf("%x.torrent", infoHash)): 0600,
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got&^want != 0 {
			t.Errorf("%s has mode %v, want at most %v", filepath.Base(path), got, want)
		}
	}
	// Temporary files are renamed into place, never left behind.
	for _, dir := range []string{configDir, torrentsDir} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if strings.Contains(e.Name(), ".tmp") {
				t.Errorf("temporary file %s left in %s", e.Name(), dir)
			}
		}
	}
}

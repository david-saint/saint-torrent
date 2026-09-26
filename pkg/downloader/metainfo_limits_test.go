package downloader

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sainttorrent/pkg/bencode"
)

// TestRestoreRejectsCachedTorrentWithHugePieceLength is the crash-loop
// regression. A .torrent with a 2^62-byte piece length used to parse and be
// cached, and the first completed piece then died in make([]byte, pieceLength).
// Because restore re-adds the cached copy on every launch, the client crashed
// again each time. The cached copy must now be rejected at restore and
// surfaced as a restore failure instead of becoming a live session.
func TestRestoreRejectsCachedTorrentWithHugePieceLength(t *testing.T) {
	tempDir := t.TempDir()
	configDir := filepath.Join(tempDir, "config")
	if err := os.MkdirAll(filepath.Join(configDir, "torrents"), 0755); err != nil {
		t.Fatal(err)
	}

	info := map[string]interface{}{
		"name":         "poison.bin",
		"piece length": int64(1) << 62,
		"pieces":       string(make([]byte, 20)),
		"length":       int64(1),
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	infoHashHex := fmt.Sprintf("%x", sha1.Sum(infoBytes))
	data, err := bencode.Marshal(map[string]interface{}{"info": info})
	if err != nil {
		t.Fatal(err)
	}
	cachedPath := filepath.Join(configDir, "torrents", infoHashHex+".torrent")
	if err := os.WriteFile(cachedPath, data, 0644); err != nil {
		t.Fatal(err)
	}
	state, err := json.Marshal(PersistedState{
		Version:  1,
		Torrents: []PersistedTorrent{{InfoHashHex: infoHashHex, DownloadDir: tempDir}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "session.json"), state, 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewTorrentManager()
	defer mgr.Close()
	warning, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}
	if sess := mgr.GetSession(infoHashHex); sess != nil {
		t.Fatal("cached torrent with a 2^62-byte piece length was restored as a live session")
	}
	if !strings.Contains(warning, "failed to restore") {
		t.Fatalf("restore failure not surfaced, warning = %q", warning)
	}
	if _, err := mgr.AddTorrentFile(cachedPath, tempDir); err == nil {
		t.Fatal("AddTorrentFile accepted a 2^62-byte piece length")
	}
}

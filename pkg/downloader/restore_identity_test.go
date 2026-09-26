package downloader

import (
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/storage"
)

// writeRestoreState writes a state directory holding entries and, for each
// cached map entry, torrents/<hash>.torrent with the given contents.
func writeRestoreState(t *testing.T, entries []PersistedTorrent, cached map[string][]byte) string {
	t.Helper()
	configDir := filepath.Join(t.TempDir(), "config")
	if err := os.MkdirAll(filepath.Join(configDir, "torrents"), 0700); err != nil {
		t.Fatal(err)
	}
	for hashHex, data := range cached {
		if err := os.WriteFile(filepath.Join(configDir, "torrents", hashHex+".torrent"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	state, err := json.Marshal(PersistedState{Version: 1, Torrents: entries})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "session.json"), state, 0600); err != nil {
		t.Fatal(err)
	}
	return configDir
}

// testTorrent returns a single-file .torrent and its info-hash.
func testTorrent(t *testing.T, name string, private bool) ([]byte, [20]byte) {
	t.Helper()
	info := map[string]interface{}{
		"name":         name,
		"piece length": int64(1 << 14),
		"pieces":       string(make([]byte, 20)),
		"length":       int64(100),
	}
	if private {
		info["private"] = int64(1)
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	data, err := bencode.Marshal(map[string]interface{}{"info": info})
	if err != nil {
		t.Fatal(err)
	}
	return data, sha1.Sum(infoBytes)
}

// TestRestoreRefusesCachedTorrentOfAnotherHash: restore added whatever the
// cached torrents/<hash>.torrent held and keyed the session by that file's
// own hash, so a cached copy that did not match its entry brought back a
// different torrent and dropped the persisted one.
func TestRestoreRefusesCachedTorrentOfAnotherHash(t *testing.T) {
	_, wantHash := testTorrent(t, "wanted.bin", false)
	otherData, otherHash := testTorrent(t, "other.bin", false)
	wantHex := fmt.Sprintf("%x", wantHash)
	configDir := writeRestoreState(t, []PersistedTorrent{{
		InfoHashHex: wantHex,
		MagnetURI:   "magnet:?xt=urn:btih:" + wantHex,
		DownloadDir: t.TempDir(),
	}}, map[string][]byte{wantHex: otherData})

	mgr := NewTorrentManager()
	defer mgr.Close()
	if _, err := mgr.EnablePersistence(configDir); err != nil {
		t.Fatal(err)
	}
	if mgr.GetSession(fmt.Sprintf("%x", otherHash)) != nil {
		t.Fatal("a cached torrent with another info-hash was restored")
	}
	sess := mgr.GetSession(wantHex)
	if sess == nil || !sess.IsMetadataMode() {
		t.Fatalf("session for the persisted hash = %v, want it restored from its magnet URI", sess)
	}
}

// TestRestoreKeepsPrivateTorrentPrivate: when the cached .torrent of a private
// torrent could not be added (an unmounted volume, a macOS permission
// denial), restore fell back to the magnet URI as a public torrent. The
// session then looked up the private info-hash on the DHT and exchanged the
// private swarm's peers over PEX until its metadata arrived again.
func TestRestoreKeepsPrivateTorrentPrivate(t *testing.T) {
	data, infoHash := testTorrent(t, "private.bin", true)
	hashHex := fmt.Sprintf("%x", infoHash)
	magnet := "magnet:?xt=urn:btih:" + hashHex
	unavailable := func(string, []storage.FileInfo, int64) (storage.Storage, error) {
		return nil, errors.New("volume not mounted")
	}
	for name, tc := range map[string]struct {
		entry  PersistedTorrent
		cached map[string][]byte
	}{
		// The cached copy still parses and says private.
		"cached torrent": {PersistedTorrent{InfoHashHex: hashHex, MagnetURI: magnet}, map[string][]byte{hashHex: data}},
		// Nothing but the persisted flag says so.
		"persisted flag": {PersistedTorrent{InfoHashHex: hashHex, MagnetURI: magnet, Private: true}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			tc.entry.DownloadDir = t.TempDir()
			configDir := writeRestoreState(t, []PersistedTorrent{tc.entry}, tc.cached)
			mgr := NewTorrentManager()
			defer mgr.Close()
			mgr.SetStorageFactory(unavailable)
			if _, err := mgr.EnablePersistence(configDir); err != nil {
				t.Fatal(err)
			}
			sess := mgr.GetSession(hashHex)
			if sess == nil {
				t.Fatal("private torrent was not restored from its magnet URI")
			}
			sess.mu.RLock()
			decentralized := sess.allowsDecentralizedPeerDiscoveryLocked()
			sess.mu.RUnlock()
			if decentralized || sess.pexEnabled() {
				t.Fatal("known-private torrent restored as a magnet may use DHT and PEX")
			}
		})
	}
}

// TestPersistenceRecordsPrivateFlag: the flag must reach session.json for the
// fallback above to know it.
func TestPersistenceRecordsPrivateFlag(t *testing.T) {
	data, infoHash := testTorrent(t, "private.bin", true)
	torrentPath := filepath.Join(t.TempDir(), "private.torrent")
	if err := os.WriteFile(torrentPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(t.TempDir(), "config")
	mgr := NewTorrentManager()
	defer mgr.Close()
	if _, err := mgr.EnablePersistence(configDir); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.AddTorrentFile(torrentPath, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(configDir, "session.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state PersistedState
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	if len(state.Torrents) != 1 || state.Torrents[0].InfoHashHex != fmt.Sprintf("%x", infoHash) || !state.Torrents[0].Private {
		t.Fatalf("persisted torrents = %+v, want the private torrent marked private", state.Torrents)
	}
}

// TestRestoreLoadsTorrentWithRepeatedOuterKey: the whole cached .torrent was
// decoded strictly, so one whose editor had appended a second "comment"
// dropped out of the list after an upgrade, shown only by its hash.
func TestRestoreLoadsTorrentWithRepeatedOuterKey(t *testing.T) {
	data, infoHash := testTorrent(t, "repeated.bin", false)
	// d4:info... -> d7:comment1:a7:comment1:b4:info...
	data = append([]byte("d7:comment1:a7:comment1:b"), data[1:]...)
	hashHex := fmt.Sprintf("%x", infoHash)
	configDir := writeRestoreState(t, []PersistedTorrent{{InfoHashHex: hashHex, DownloadDir: t.TempDir()}},
		map[string][]byte{hashHex: data})
	mgr := NewTorrentManager()
	defer mgr.Close()
	warning, err := mgr.EnablePersistence(configDir)
	if err != nil || warning != "" {
		t.Fatalf("EnablePersistence = %q, %v; want a clean restore", warning, err)
	}
	if sess := mgr.GetSession(hashHex); sess == nil || sess.IsMetadataMode() {
		t.Fatalf("session = %v, want the cached torrent restored", sess)
	}
}

// TestRestoreReportsUnparsableTorrentAsSuch: a saved .torrent this version
// cannot parse was retried with backoff and then reported as something the
// next launch would retry, although parsing the same bytes fails the same way.
func TestRestoreReportsUnparsableTorrentAsSuch(t *testing.T) {
	// A repeated key inside the info dict, which the info-hash covers.
	info := []byte("d6:lengthi1e4:name1:a4:name1:b12:piece lengthi16384e6:pieces20:" + string(make([]byte, 20)) + "e")
	data := append(append([]byte("d4:info"), info...), 'e')
	hashHex := fmt.Sprintf("%x", sha1.Sum(info))
	configDir := writeRestoreState(t, []PersistedTorrent{{InfoHashHex: hashHex, DownloadDir: t.TempDir()}},
		map[string][]byte{hashHex: data})

	cachedPath := filepath.Join(configDir, "torrents", hashHex+".torrent")
	if _, _, err := loadTorrentFile(cachedPath, hashHex); !isTorrentParseError(err) {
		t.Fatalf("loadTorrentFile = %v, want a parse error, which restore does not retry", err)
	}
	if _, _, err := loadTorrentFile(filepath.Join(configDir, "missing.torrent"), ""); err == nil || isTorrentParseError(err) {
		t.Fatalf("loadTorrentFile(missing) = %v, want a read error, which restore retries", err)
	}

	mgr := NewTorrentManager()
	defer mgr.Close()
	warning, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(warning, "failed to restore because this version cannot load the saved .torrent") ||
		!strings.Contains(warning, "duplicate dictionary key") || strings.Contains(warning, "will retry") {
		t.Fatalf("warning = %q, want the parse failure reported as permanent", warning)
	}
}

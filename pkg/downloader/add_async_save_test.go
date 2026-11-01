package downloader

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"sainttorrent/pkg/torrent"
)

// An add used to write session.json and the cached .torrent durably on the
// caller's goroutine, often the TUI's: four file syncs (F_FULLFSYNC on macOS)
// that take seconds while other torrents keep the disk busy. It must now
// return while a state save is stuck in a sync, and the save must still sync
// the cached .torrent before the session.json that lists it.
func TestAddDoesNotWaitOnStateSyncs(t *testing.T) {
	configDir := filepath.Join(t.TempDir(), "config")
	mgr := NewTorrentManager()
	defer mgr.Close()
	if _, err := mgr.EnablePersistence(configDir); err != nil {
		t.Fatal(err)
	}

	gate := make(chan struct{})
	var (
		mu     sync.Mutex
		synced []string
	)
	hook := func(destPath string) {
		<-gate
		mu.Lock()
		synced = append(synced, destPath)
		mu.Unlock()
	}
	fileSyncHook.Store(&hook)
	t.Cleanup(func() { fileSyncHook.Store(nil) })
	opened := false
	open := func() {
		if !opened {
			opened = true
			close(gate)
		}
	}
	defer open() // before mgr.Close, which saves once more

	data, infoHash := testTorrent(t, "async.bin", false)
	torrentPath := filepath.Join(t.TempDir(), "async.torrent")
	if err := os.WriteFile(torrentPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	const magnetHash = "b42e85596f7a0dd05eefdb78b0ac1736496f8626"

	added := make(chan error, 2)
	go func() {
		// The magnet's save gets stuck in the hook; the torrent-file add that
		// follows must not wait for it either.
		_, err := mgr.AddMagnet("magnet:?xt=urn:btih:"+magnetHash+"&dn=Async", t.TempDir())
		added <- err
		_, err = mgr.AddTorrentFile(torrentPath, t.TempDir())
		added <- err
	}()
	for range 2 {
		select {
		case err := <-added:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("an add waited on a state-file sync")
		}
	}

	open()
	torrentHash := fmt.Sprintf("%x", infoHash)
	statePath := filepath.Join(configDir, "session.json")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if hashes := persistedHashes(t, statePath); slices.Contains(hashes, torrentHash) && slices.Contains(hashes, magnetHash) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("session.json never listed both adds: %v", persistedHashes(t, statePath))
		}
		time.Sleep(10 * time.Millisecond)
	}

	cachePath := filepath.Join(configDir, "torrents", torrentHash+".torrent")
	cached, err := os.ReadFile(cachePath)
	if err != nil || string(cached) != string(data) {
		t.Fatalf("cached .torrent is not the original file (err %v)", err)
	}
	mu.Lock()
	defer mu.Unlock()
	cacheAt := slices.Index(synced, cachePath)
	if cacheAt < 0 {
		t.Fatalf("cached .torrent was never synced (synced: %q)", synced)
	}
	if !slices.Contains(synced[cacheAt+1:], statePath) {
		t.Fatalf("no session.json sync after the cached .torrent's (synced: %q)", synced)
	}
}

func persistedHashes(t *testing.T, statePath string) []string {
	t.Helper()
	raw, err := os.ReadFile(statePath)
	if err != nil {
		return nil
	}
	var state PersistedState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil
	}
	var hashes []string
	for _, entry := range state.Torrents {
		hashes = append(hashes, entry.InfoHashHex)
	}
	return hashes
}

// A save that snapshotted a torrent before its removal must not rebuild the
// cached copy the removal deletes.
func TestEnsureCachedTorrentSkipsRemovedTorrent(t *testing.T) {
	stateDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(stateDir, "torrents"), 0700); err != nil {
		t.Fatal(err)
	}
	mgr := NewTorrentManager()
	defer mgr.Close()
	data, infoHash := testTorrent(t, "gone.bin", false)
	tor, err := torrent.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", infoHash)
	if err := mgr.ensureCachedTorrent(stateDir, hash, tor); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "torrents", hash+".torrent")); !os.IsNotExist(err) {
		t.Fatalf("cached copy of an unregistered torrent was rebuilt (stat: %v)", err)
	}
}

// saveStateInternal takes each session's lock in turn. Holding m.mu
// exclusively meanwhile made one busy session stall every m.mu reader, such as
// inbound handshake routing for every torrent.
func TestStateSaveDoesNotBlockManagerReaders(t *testing.T) {
	mgr := NewTorrentManager()
	defer mgr.Close()
	if _, err := mgr.EnablePersistence(filepath.Join(t.TempDir(), "config")); err != nil {
		t.Fatal(err)
	}
	const hash = "c42e85596f7a0dd05eefdb78b0ac1736496f8626"
	sess, err := mgr.AddMagnet("magnet:?xt=urn:btih:"+hash+"&dn=Busy", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.saveState(); err != nil { // drain the save the add scheduled
		t.Fatal(err)
	}

	sess.mu.Lock() // a session busy on its hot path
	saved := make(chan struct{})
	go func() {
		_ = mgr.saveState()
		close(saved)
	}()
	time.Sleep(50 * time.Millisecond) // let the save reach the session's lock

	read := make(chan struct{})
	go func() {
		mgr.GetSession(hash)
		mgr.AdvertisedPeerPort()
		close(read)
	}()
	select {
	case <-read:
	case <-time.After(2 * time.Second):
		sess.mu.Unlock()
		t.Fatal("a manager reader waited behind a state save blocked on one session")
	}
	sess.mu.Unlock()
	<-saved
}

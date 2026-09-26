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
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/storage"
)

// closeBlockingStorage holds the first Close until release is closed, which
// parks RemoveSession inside Session.Close.
type closeBlockingStorage struct {
	storage.Storage
	entered chan<- struct{}
	release <-chan struct{}
}

func (c *closeBlockingStorage) Close() error {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
	return c.Storage.Close()
}

// TestRemoveSessionRefusesReAddUntilDone: RemoveSession dropped the torrent
// from the manager first and only then closed it and deleted its state and
// payload. A re-add of the same torrent in that window opened storage on the
// same files and loaded the same .state, and the removal then deleted both
// from under the live session.
func TestRemoveSessionRefusesReAddUntilDone(t *testing.T) {
	downloadDir := t.TempDir()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	mgr := NewTorrentManager()
	defer mgr.Close()
	mgr.SetStorageFactory(func(dir string, files []storage.FileInfo, pieceLength int64) (storage.Storage, error) {
		st, err := storage.NewMemStorage(dir, files, pieceLength)
		if err != nil {
			return nil, err
		}
		return &closeBlockingStorage{Storage: st, entered: entered, release: release}, nil
	})

	torrentPath, infoHash := writeTestTorrent(t, t.TempDir(), "readd.bin", 1000, "a")
	hashHex := fmt.Sprintf("%x", infoHash)
	if _, err := mgr.AddTorrentFile(torrentPath, downloadDir); err != nil {
		t.Fatal(err)
	}
	removed := make(chan error, 1)
	go func() { removed <- mgr.RemoveSession(hashHex, true) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("RemoveSession never closed the session's storage")
	}

	_, fileErr := mgr.AddTorrentFile(torrentPath, downloadDir)
	_, magnetErr := mgr.AddMagnet("magnet:?xt=urn:btih:"+hashHex, downloadDir)
	close(release)
	if err := <-removed; err != nil {
		t.Fatalf("RemoveSession: %v", err)
	}
	if !errors.Is(fileErr, ErrRemovalInProgress) || !errors.Is(magnetErr, ErrRemovalInProgress) {
		t.Fatalf("re-add during removal: file err = %v, magnet err = %v; want ErrRemovalInProgress", fileErr, magnetErr)
	}
	if mgr.GetSession(hashHex) != nil {
		t.Fatal("a session added during the removal survived it")
	}
	if _, err := mgr.AddTorrentFile(torrentPath, downloadDir); err != nil {
		t.Fatalf("re-add after the removal finished: %v", err)
	}
}

// TestRemoveSessionDeletesFilesOfMetadataCompletedWhileClosing: RemoveSession
// read the torrent's (empty) file list before closing a magnet, so metadata
// that completed meanwhile created payload files a remove-and-delete left
// behind.
func TestRemoveSessionDeletesFilesOfMetadataCompletedWhileClosing(t *testing.T) {
	downloadDir := t.TempDir()
	entered := make(chan struct{}, 1)
	proceed := make(chan struct{})
	mgr := NewTorrentManager()
	defer mgr.Close()
	mgr.SetStorageFactory(blockingFactory(storage.NewStorage, entered, proceed))

	infoBytes, err := bencode.Marshal(map[string]interface{}{
		"name":         "late",
		"piece length": int64(1 << 14),
		"pieces":       string(make([]byte, 20)),
		"files": []interface{}{
			map[string]interface{}{"length": int64(100), "path": []interface{}{"a.bin"}},
			map[string]interface{}{"length": int64(100), "path": []interface{}{"sub", "b.bin"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	hashHex := fmt.Sprintf("%x", sha1.Sum(infoBytes))
	sess, err := mgr.AddMagnet("magnet:?xt=urn:btih:"+hashHex, downloadDir)
	if err != nil {
		t.Fatal(err)
	}
	metaDone := make(chan error, 1)
	go func() { metaDone <- sess.onMetadataDownloaded(infoBytes) }()
	<-entered

	removed := make(chan error, 1)
	go func() { removed <- mgr.RemoveSession(hashHex, true) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if sess.mu.TryRLock() {
			closing := sess.closing
			sess.mu.RUnlock()
			if closing {
				break
			}
		}
		if time.Now().After(deadline) {
			close(proceed)
			t.Fatal("RemoveSession never started closing the session")
		}
		time.Sleep(time.Millisecond)
	}
	close(proceed)
	<-metaDone
	if err := <-removed; err != nil {
		t.Fatalf("RemoveSession: %v", err)
	}
	if _, err := os.Stat(filepath.Join(downloadDir, "late")); !os.IsNotExist(err) {
		t.Fatalf("payload created while the magnet was closing survived remove-and-delete: %v", err)
	}
}

// TestRemoveSessionKeepsFilesAnotherTorrentUses: deleting a torrent with its
// files deleted every path its metadata listed, including one an active
// torrent owned. Here the removed torrent is an entry that failed to restore
// because the other torrent had taken its file in the meantime.
func TestRemoveSessionKeepsFilesAnotherTorrentUses(t *testing.T) {
	tempDir := t.TempDir()
	downloadDir := filepath.Join(tempDir, "dl")
	configDir := filepath.Join(tempDir, "config")
	if err := os.MkdirAll(filepath.Join(configDir, "torrents"), 0700); err != nil {
		t.Fatal(err)
	}

	owner, _ := writeTestTorrent(t, tempDir, "shared.bin", 1000, "owner")
	stale, staleHash := writeTestTorrent(t, tempDir, "shared.bin", 1000, "stale")
	staleHex := fmt.Sprintf("%x", staleHash)
	staleData, err := os.ReadFile(stale)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "torrents", staleHex+".torrent"), staleData, 0600); err != nil {
		t.Fatal(err)
	}
	state, err := json.Marshal(PersistedState{Version: 1, Torrents: []PersistedTorrent{{InfoHashHex: staleHex, DownloadDir: downloadDir}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "session.json"), state, 0600); err != nil {
		t.Fatal(err)
	}

	mgr := NewTorrentManager()
	defer mgr.Close()
	if _, err := mgr.AddTorrentFile(owner, downloadDir); err != nil {
		t.Fatal(err)
	}
	if warning, err := mgr.EnablePersistence(configDir); err != nil || !strings.Contains(warning, "failed to restore") {
		t.Fatalf("EnablePersistence = %q, %v; want the stale entry to fail to restore", warning, err)
	}

	err = mgr.RemoveSession(staleHex, true)
	if err == nil || !strings.Contains(err.Error(), "another torrent still uses") {
		t.Fatalf("RemoveSession = %v, want the shared file reported as kept", err)
	}
	if _, err := os.Stat(filepath.Join(downloadDir, "shared.bin")); err != nil {
		t.Fatalf("active torrent's file was deleted with another torrent: %v", err)
	}
}

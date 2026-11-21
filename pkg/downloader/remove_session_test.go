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
// because the other torrent had taken its file, at another length, in the
// meantime.
func TestRemoveSessionKeepsFilesAnotherTorrentUses(t *testing.T) {
	tempDir := t.TempDir()
	downloadDir := filepath.Join(tempDir, "dl")
	configDir := filepath.Join(tempDir, "config")
	if err := os.MkdirAll(filepath.Join(configDir, "torrents"), 0700); err != nil {
		t.Fatal(err)
	}

	owner, _ := writeTestTorrent(t, tempDir, "shared.bin", 1000, "owner")
	stale, staleHash := writeTestTorrent(t, tempDir, "shared.bin", 2000, "stale")
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

func writeMultiFileTorrent(t *testing.T, dir, fileName, rootName string, files []map[string]interface{}) (string, [20]byte) {
	t.Helper()
	info := map[string]interface{}{
		"name":         rootName,
		"piece length": int64(1 << 14),
		"pieces":       string(make([]byte, 20)),
		"files":        files,
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bencode.Marshal(map[string]interface{}{
		"info": info,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fileName)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path, sha1.Sum(infoBytes)
}

// TestRemoveSessionRemovesFolderWithOSMetadata verifies that deleting a
// multi-file torrent with its files removes its folder tree even when macOS
// Finder (.DS_Store, AppleDouble "._*") or Windows Explorer left OS metadata
// files inside those directories.
func TestRemoveSessionRemovesFolderWithOSMetadata(t *testing.T) {
	downloadDir := t.TempDir()
	torrentDir := t.TempDir()

	torrentPath, infoHash := writeMultiFileTorrent(t, torrentDir, "show.torrent", "Show", []map[string]interface{}{
		{"length": int64(100), "path": []interface{}{"Season 1", "ep1.mkv"}},
		{"length": int64(50), "path": []interface{}{"readme.txt"}},
	})
	hashHex := fmt.Sprintf("%x", infoHash)

	mgr := NewTorrentManager()
	defer mgr.Close()
	if _, err := mgr.AddTorrentFile(torrentPath, downloadDir); err != nil {
		t.Fatal(err)
	}

	// Simulate Finder browsing both the torrent root and its subfolder.
	if err := os.WriteFile(filepath.Join(downloadDir, "Show", ".DS_Store"), []byte("finder"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(downloadDir, "Show", "Season 1", ".DS_Store"), []byte("finder"), 0644); err != nil {
		t.Fatal(err)
	}
	appleDouble := []byte{0x00, 0x05, 0x16, 0x07, 0x00, 0x02, 0x00, 0x00}
	if err := os.WriteFile(filepath.Join(downloadDir, "Show", "Season 1", "._ep1.mkv"), appleDouble, 0644); err != nil {
		t.Fatal(err)
	}

	if err := mgr.RemoveSession(hashHex, true); err != nil {
		t.Fatalf("RemoveSession: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(downloadDir, "Show")); !os.IsNotExist(err) {
		t.Fatalf("torrent folder survived remove-and-delete: %v", err)
	}
	if _, err := os.Stat(downloadDir); err != nil {
		t.Fatalf("downloadDir should still exist: %v", err)
	}
}

// TestRemoveSessionKeepsCrossSeededDSStore ensures OS metadata cleanup never
// deletes a file another active torrent still claims.
func TestRemoveSessionKeepsCrossSeededDSStore(t *testing.T) {
	downloadDir := t.TempDir()
	torrentDir := t.TempDir()

	firstPath, firstHash := writeMultiFileTorrent(t, torrentDir, "first.torrent", "Pack", []map[string]interface{}{
		{"length": int64(100), "path": []interface{}{"movie.mkv"}},
		{"length": int64(16), "path": []interface{}{".DS_Store"}},
	})
	secondPath, _ := writeMultiFileTorrent(t, torrentDir, "second.torrent", "Pack", []map[string]interface{}{
		{"length": int64(16), "path": []interface{}{".DS_Store"}},
	})

	mgr := NewTorrentManager()
	defer mgr.Close()
	if _, err := mgr.AddTorrentFile(firstPath, downloadDir); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.AddTorrentFile(secondPath, downloadDir); err != nil {
		t.Fatal(err)
	}

	err := mgr.RemoveSession(fmt.Sprintf("%x", firstHash), true)
	if !errors.Is(err, ErrFilesKept) {
		t.Fatalf("RemoveSession = %v, want ErrFilesKept", err)
	}
	if _, err := os.Lstat(filepath.Join(downloadDir, "Pack", "movie.mkv")); !os.IsNotExist(err) {
		t.Fatalf("unshared file should be deleted: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(downloadDir, "Pack", ".DS_Store")); err != nil {
		t.Fatalf("cross-seeded .DS_Store was deleted: %v", err)
	}
}

// TestRemoveSessionKeepsUnlistedActiveTorrentMetadataFile ensures OS metadata
// cleanup consults live pathClaims so removing a torrent that does NOT list a
// metadata-named file never deletes one actively claimed by another torrent in
// the same or an overlapping download directory.
func TestRemoveSessionKeepsUnlistedActiveTorrentMetadataFile(t *testing.T) {
	for _, tc := range []struct {
		name      string
		nestedDir bool
	}{
		{name: "same download dir"},
		{name: "overlapping nested download dir", nestedDir: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			downloadDir := t.TempDir()
			torrentDir := t.TempDir()

			firstPath, firstHash := writeMultiFileTorrent(t, torrentDir, "first.torrent", "Pack", []map[string]interface{}{
				{"length": int64(100), "path": []interface{}{"movie.mkv"}},
			})
			var secondPath string
			secondDownloadDir := downloadDir
			if tc.nestedDir {
				secondDownloadDir = filepath.Join(downloadDir, "Pack")
				secondPath, _ = writeTestTorrent(t, torrentDir, ".DS_Store", 16, "second")
			} else {
				secondPath, _ = writeMultiFileTorrent(t, torrentDir, "second.torrent", "Pack", []map[string]interface{}{
					{"length": int64(16), "path": []interface{}{".DS_Store"}},
				})
			}

			mgr := NewTorrentManager()
			defer mgr.Close()
			if _, err := mgr.AddTorrentFile(firstPath, downloadDir); err != nil {
				t.Fatal(err)
			}
			if _, err := mgr.AddTorrentFile(secondPath, secondDownloadDir); err != nil {
				t.Fatal(err)
			}

			if err := mgr.RemoveSession(fmt.Sprintf("%x", firstHash), true); err != nil {
				t.Fatalf("RemoveSession: %v", err)
			}
			if _, err := os.Lstat(filepath.Join(downloadDir, "Pack", "movie.mkv")); !os.IsNotExist(err) {
				t.Fatalf("removed torrent's file should be deleted: %v", err)
			}
			if _, err := os.Lstat(filepath.Join(downloadDir, "Pack", ".DS_Store")); err != nil {
				t.Fatalf("other active torrent's .DS_Store was deleted: %v", err)
			}
		})
	}
}

// TestClaimAllFreePathsReservesMetadataAgainstConcurrentAdd verifies that
// dynamically discovered OS metadata files are reserved in pathClaims while
// being removed, blocking any concurrent torrent add on those paths until
// removal completes.
func TestClaimAllFreePathsReservesMetadataAgainstConcurrentAdd(t *testing.T) {
	downloadDir := t.TempDir()
	torrentDir := t.TempDir()

	metaTorrent, _ := writeMultiFileTorrent(t, torrentDir, "meta.torrent", "Pack", []map[string]interface{}{
		{"length": int64(16), "path": []interface{}{".DS_Store"}},
	})

	mgr := NewTorrentManager()
	defer mgr.Close()

	ok, release := mgr.claimAllFreePaths([20]byte{1}, downloadDir, []string{filepath.Join("Pack", ".DS_Store")})
	if !ok {
		t.Fatal("claimAllFreePaths refused an unheld metadata path")
	}

	if _, err := mgr.AddTorrentFile(metaTorrent, downloadDir); !errors.Is(err, ErrPathInUse) {
		release()
		t.Fatalf("concurrent add during metadata reservation = %v, want ErrPathInUse", err)
	}

	release()

	if _, err := mgr.AddTorrentFile(metaTorrent, downloadDir); err != nil {
		t.Fatalf("add after metadata reservation released: %v", err)
	}
}

package downloader

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"sainttorrent/pkg/bencode"
)

// writeTestTorrent writes a single-file .torrent named name with the given
// length. tag lands in the info dict so equal names can have distinct hashes.
func writeTestTorrent(t *testing.T, dir, name string, length int64, tag string) (path string, infoHash [20]byte) {
	t.Helper()
	pieceLength := int64(1 << 14)
	info := map[string]interface{}{
		"name":         name,
		"piece length": pieceLength,
		"pieces":       string(make([]byte, 20*int((length+pieceLength-1)/pieceLength))),
		"length":       length,
		"x-tag":        tag,
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	data, err := bencode.Marshal(map[string]interface{}{"info": info})
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(dir, fmt.Sprintf("%s-%s.torrent", name, tag))
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path, sha1.Sum(infoBytes)
}

// TestAddTorrentRefusesAnotherTorrentsFile: two torrents with different
// info-hashes that name the same file in the same directory were both
// accepted. The second one resized the first one's file and both wrote it, so
// the first torrent's verified data was corrupted, and deleting either one
// with its files deleted the other's payload.
func TestAddTorrentRefusesAnotherTorrentsFile(t *testing.T) {
	downloadDir := t.TempDir()
	torrentDir := t.TempDir()
	mgr := NewTorrentManager()
	defer mgr.Close()

	first, _ := writeTestTorrent(t, torrentDir, "shared.bin", 1000, "a")
	if _, err := mgr.AddTorrentFile(first, downloadDir); err != nil {
		t.Fatalf("add first torrent: %v", err)
	}
	payload := filepath.Join(downloadDir, "shared.bin")
	want := bytes.Repeat([]byte("A"), 1000)
	if err := os.WriteFile(payload, want, 0600); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"shared.bin", "SHARED.bin"} {
		second, _ := writeTestTorrent(t, torrentDir, name, 5000, "b")
		if _, err := mgr.AddTorrentFile(second, downloadDir); !errors.Is(err, ErrPathInUse) {
			t.Fatalf("add %q over another torrent's file: err = %v, want ErrPathInUse", name, err)
		}
	}
	got, err := os.ReadFile(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("first torrent's file changed to %d bytes by a refused add", len(got))
	}

	// The same file name in another directory is another file.
	second, _ := writeTestTorrent(t, torrentDir, "shared.bin", 5000, "b")
	if _, err := mgr.AddTorrentFile(second, t.TempDir()); err != nil {
		t.Fatalf("add a same-named torrent into another directory: %v", err)
	}
}

// TestRemovedTorrentReleasesItsFiles: the reservation ends with the session,
// so a torrent can be re-added over files whose owner was removed.
func TestRemovedTorrentReleasesItsFiles(t *testing.T) {
	downloadDir := t.TempDir()
	torrentDir := t.TempDir()
	mgr := NewTorrentManager()
	defer mgr.Close()

	first, firstHash := writeTestTorrent(t, torrentDir, "reused.bin", 1000, "a")
	second, _ := writeTestTorrent(t, torrentDir, "reused.bin", 1000, "b")
	if _, err := mgr.AddTorrentFile(first, downloadDir); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.AddTorrentFile(second, downloadDir); !errors.Is(err, ErrPathInUse) {
		t.Fatalf("second add err = %v, want ErrPathInUse", err)
	}
	if err := mgr.RemoveSession(fmt.Sprintf("%x", firstHash), false); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.AddTorrentFile(second, downloadDir); err != nil {
		t.Fatalf("add after the owner was removed: %v", err)
	}
}

// TestMagnetMetadataRefusesAnotherTorrentsFile: the same collision reached
// through a magnet's metadata must leave the magnet in metadata mode with the
// error shown, and the existing torrent's file untouched.
func TestMagnetMetadataRefusesAnotherTorrentsFile(t *testing.T) {
	downloadDir := t.TempDir()
	mgr := NewTorrentManager()
	defer mgr.Close()

	first, _ := writeTestTorrent(t, t.TempDir(), "taken.bin", 1000, "a")
	if _, err := mgr.AddTorrentFile(first, downloadDir); err != nil {
		t.Fatal(err)
	}
	infoBytes, err := bencode.Marshal(map[string]interface{}{
		"name":         "taken.bin",
		"piece length": int64(1 << 14),
		"pieces":       string(make([]byte, 20)),
		"length":       int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	sess, err := mgr.AddMagnet(fmt.Sprintf("magnet:?xt=urn:btih:%x", sha1.Sum(infoBytes)), downloadDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.onMetadataDownloaded(infoBytes); !errors.Is(err, ErrPathInUse) {
		t.Fatalf("metadata over another torrent's file: err = %v, want ErrPathInUse", err)
	}
	if !sess.IsMetadataMode() || sess.Status() != "Error" {
		t.Fatalf("metadataMode=%v status=%q, want an error shown in metadata mode", sess.IsMetadataMode(), sess.Status())
	}
	if info, err := os.Stat(filepath.Join(downloadDir, "taken.bin")); err != nil || info.Size() != 1000 {
		t.Fatalf("existing torrent's file = %v, %v; want it untouched at 1000 bytes", info, err)
	}
}

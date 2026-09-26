package downloader

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/storage"
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
	second, _ := writeTestTorrent(t, torrentDir, "reused.bin", 2000, "b")
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

// TestCrossSeedSharesFilesOfTheSameLength: the registry refused every second
// torrent on a path, which refused cross-seeding (one payload published under
// several info-hashes, which libtorrent, qBittorrent and Transmission all
// allow), and on restore it failed whichever cross-seed lost the race. Torrents
// declaring the same length may share a file. A different length is still
// refused while any sharer remains, and deleting one sharer keeps the file for
// the others.
func TestCrossSeedSharesFilesOfTheSameLength(t *testing.T) {
	downloadDir := t.TempDir()
	torrentDir := t.TempDir()
	mgr := NewTorrentManager()
	defer mgr.Close()

	first, firstHash := writeTestTorrent(t, torrentDir, "seeded.bin", 1000, "a")
	cross, crossHash := writeTestTorrent(t, torrentDir, "seeded.bin", 1000, "b")
	other, _ := writeTestTorrent(t, torrentDir, "seeded.bin", 2000, "c")
	if _, err := mgr.AddTorrentFile(first, downloadDir); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.AddTorrentFile(cross, downloadDir); err != nil {
		t.Fatalf("cross-seed of the same file and length: %v", err)
	}
	if _, err := mgr.AddTorrentFile(other, downloadDir); !errors.Is(err, ErrPathInUse) {
		t.Fatalf("add at another length over a cross-seeded file: err = %v, want ErrPathInUse", err)
	}

	payload := filepath.Join(downloadDir, "seeded.bin")
	err := mgr.RemoveSession(fmt.Sprintf("%x", firstHash), true)
	if err == nil || !strings.Contains(err.Error(), "another torrent still uses") {
		t.Fatalf("removing one cross-seed with its files = %v, want the shared file reported as kept", err)
	}
	if _, err := os.Stat(payload); err != nil {
		t.Fatalf("file the other cross-seed uses was deleted: %v", err)
	}
	// The remaining sharer still fixes the length.
	if _, err := mgr.AddTorrentFile(other, downloadDir); !errors.Is(err, ErrPathInUse) {
		t.Fatalf("add at another length after the first holder left: err = %v, want ErrPathInUse", err)
	}

	if err := mgr.RemoveSession(fmt.Sprintf("%x", crossHash), true); err != nil {
		t.Fatalf("removing the last holder: %v", err)
	}
	if _, err := os.Stat(payload); !os.IsNotExist(err) {
		t.Fatalf("last holder's file survived remove-and-delete: %v", err)
	}
	if _, err := mgr.AddTorrentFile(other, downloadDir); err != nil {
		t.Fatalf("add once every holder is gone: %v", err)
	}
}

// TestPathClaimsRefuseJoiningAFileBeingDeleted: a removal deleting a file
// reserves it, so not even a cross-seed of the same length can open it while
// it is being deleted from under it.
func TestPathClaimsRefuseJoiningAFileBeingDeleted(t *testing.T) {
	mgr := NewTorrentManager()
	defer mgr.Close()
	dir := t.TempDir()
	removed, joiner := [20]byte{1}, [20]byte{2}
	files := []storage.FileInfo{{Path: "a.bin", Length: 10}}

	releaseRemoved, err := mgr.claimPaths(removed, dir, files)
	if err != nil {
		t.Fatal(err)
	}
	free, kept, releaseDelete := mgr.claimFreePaths(removed, dir, []string{"a.bin"})
	if len(free) != 1 || len(kept) != 0 {
		t.Fatalf("free=%v kept=%v, want the removed torrent's own file free to delete", free, kept)
	}
	releaseRemoved()
	if _, err := mgr.claimPaths(joiner, dir, files); !errors.Is(err, ErrPathInUse) {
		t.Fatalf("joining a file being deleted: err = %v, want ErrPathInUse", err)
	}
	releaseDelete()
	releaseJoiner, err := mgr.claimPaths(joiner, dir, files)
	if err != nil {
		t.Fatalf("claim after the deletion finished: %v", err)
	}
	releaseJoiner()
	releaseJoiner() // idempotent

	mgr.claimMu.Lock()
	defer mgr.claimMu.Unlock()
	if len(mgr.pathClaims) != 0 || len(mgr.sharedClaims) != 0 {
		t.Fatalf("claims left after every release: %d paths, %d shared", len(mgr.pathClaims), len(mgr.sharedClaims))
	}
}

// TestPathClaimsBookkeeping drives holds and releases through the inline and
// shared slots, including a duplicate add of each torrent, and checks that
// every path is freed exactly when its last holder lets go.
func TestPathClaimsBookkeeping(t *testing.T) {
	mgr := NewTorrentManager()
	defer mgr.Close()
	dir := t.TempDir()
	a, b, c := [20]byte{1}, [20]byte{2}, [20]byte{3}
	files := []storage.FileInfo{{Path: "x", Length: 5}, {Path: "y", Length: 7}}
	claim := func(h [20]byte) func() {
		t.Helper()
		release, err := mgr.claimPaths(h, dir, files)
		if err != nil {
			t.Fatalf("claim by %x: %v", h[:1], err)
		}
		return release
	}
	claims := func() (int, int) {
		mgr.claimMu.Lock()
		defer mgr.claimMu.Unlock()
		return len(mgr.pathClaims), len(mgr.sharedClaims)
	}

	relA1, relB1, relA2, relB2 := claim(a), claim(b), claim(a), claim(b)
	if paths, shared := claims(); paths != 2 || shared != 2 {
		t.Fatalf("claims = %d paths, %d shared; want 2 and 2", paths, shared)
	}
	relA1()
	relA2() // the inline holder is gone; b still shares both paths
	if _, err := mgr.claimPaths(c, dir, []storage.FileInfo{{Path: "x", Length: 6}}); !errors.Is(err, ErrPathInUse) {
		t.Fatalf("claim at another length while b holds: err = %v, want ErrPathInUse", err)
	}
	relC := claim(c) // same lengths: takes the free inline slot
	relB1()
	relB2()
	if paths, shared := claims(); paths != 2 || shared != 0 {
		t.Fatalf("claims = %d paths, %d shared; want c's 2 paths only", paths, shared)
	}
	relC()
	if paths, shared := claims(); paths != 0 || shared != 0 {
		t.Fatalf("claims left after every release: %d paths, %d shared", paths, shared)
	}
}

// TestClaimsResolveADownloadDirectoryCreatedLater: the base was resolved
// through symlinks only when it already existed. The first add into a new
// directory under a symlinked parent was keyed by the link spelling; once the
// add had created the directory, an add through the real path was keyed by
// the resolved one, and the two torrents shared a file.
func TestClaimsResolveADownloadDirectoryCreatedLater(t *testing.T) {
	parent := t.TempDir()
	realDir := filepath.Join(parent, "real")
	if err := os.Mkdir(realDir, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(realDir, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	torrentDir := t.TempDir()
	mgr := NewTorrentManager()
	defer mgr.Close()

	first, _ := writeTestTorrent(t, torrentDir, "shared.bin", 1000, "a")
	if _, err := mgr.AddTorrentFile(first, filepath.Join(link, "new")); err != nil {
		t.Fatalf("add into a directory that does not exist yet: %v", err)
	}
	second, _ := writeTestTorrent(t, torrentDir, "shared.bin", 2000, "b")
	if _, err := mgr.AddTorrentFile(second, filepath.Join(realDir, "new")); !errors.Is(err, ErrPathInUse) {
		t.Fatalf("add over the same file through the resolved path: err = %v, want ErrPathInUse", err)
	}
}

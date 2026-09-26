package downloader

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/storage"
)

// legacyPayload is a torrent whose names the current sanitizer changes, with
// its payload and where an older version wrote each file.
type legacyPayload struct {
	info      map[string]interface{}
	infoBytes []byte
	infoHash  [20]byte
	data      []byte
	legacy    []string // per file, relative to the download directory
	current   []string
	lengths   []int64
}

const legacyPieceLength = 1 << 14

// newLegacyPayload builds a torrent named name holding files (single-file when
// files is nil) with real piece hashes. legacy and current are each file's
// old and new relative paths.
func newLegacyPayload(t *testing.T, name string, files []interface{}, lengths []int64, legacy, current []string) *legacyPayload {
	t.Helper()
	var total int64
	for _, n := range lengths {
		total += n
	}
	data := make([]byte, total)
	for i := range data {
		data[i] = byte(i*7 + i/251)
	}
	var pieces []byte
	for off := int64(0); off < total; off += legacyPieceLength {
		sum := sha1.Sum(data[off:min(off+legacyPieceLength, total)])
		pieces = append(pieces, sum[:]...)
	}
	info := map[string]interface{}{
		"name":         name,
		"piece length": int64(legacyPieceLength),
		"pieces":       string(pieces),
	}
	if files == nil {
		info["length"] = total
	} else {
		info["files"] = files
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	return &legacyPayload{info: info, infoBytes: infoBytes, infoHash: sha1.Sum(infoBytes), data: data,
		legacy: legacy, current: current, lengths: lengths}
}

// writeTorrent writes the .torrent into dir and returns its path.
func (p *legacyPayload) writeTorrent(t *testing.T, dir string) string {
	t.Helper()
	data, err := bencode.Marshal(map[string]interface{}{"info": p.info})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%x.torrent", p.infoHash))
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// writeLegacyFiles lays the payload out under the old names, as the version
// before the current sanitizer did, skipping the test where this filesystem
// cannot hold them.
func (p *legacyPayload) writeLegacyFiles(t *testing.T, downloadDir string) {
	t.Helper()
	var off int64
	for i, rel := range p.legacy {
		path := filepath.Join(downloadDir, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Skipf("this filesystem cannot hold %q: %v", rel, err)
		}
		if err := os.WriteFile(path, p.data[off:off+p.lengths[i]], 0644); err != nil {
			t.Skipf("this filesystem cannot hold %q: %v", rel, err)
		}
		off += p.lengths[i]
	}
}

// checkMigrated checks that every file now sits at its current path with its
// payload and that nothing is left at the old ones.
func (p *legacyPayload) checkMigrated(t *testing.T, downloadDir string) {
	t.Helper()
	var off int64
	for i, rel := range p.current {
		got, err := os.ReadFile(filepath.Join(downloadDir, rel))
		if err != nil || !bytes.Equal(got, p.data[off:off+p.lengths[i]]) {
			t.Fatalf("%q holds %d bytes (%v), want the payload the old version wrote", rel, len(got), err)
		}
		if _, err := os.Lstat(filepath.Join(downloadDir, p.legacy[i])); !os.IsNotExist(err) {
			t.Fatalf("old file %q still exists: %v", p.legacy[i], err)
		}
		off += p.lengths[i]
	}
}

// checkAllVerified hashes the session's pieces and checks that every one of
// them was found on disk rather than left to download again.
func checkAllVerified(t *testing.T, sess *Session) {
	t.Helper()
	sess.maybeStartVerification()
	sess.WaitVerified()
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	for i, state := range sess.PieceStates {
		if state != PieceCompleted {
			t.Fatalf("piece %d of %d is %v after verification, want completed from the moved files", i, len(sess.PieceStates), state)
		}
	}
}

func singleLegacyPayload(t *testing.T) *legacyPayload {
	return newLegacyPayload(t, "Movie \u200f(RLM).mkv", nil, []int64{40000},
		[]string{"Movie \u200f(RLM).mkv"}, []string{"Movie (RLM).mkv"})
}

func multiLegacyPayload(t *testing.T) *legacyPayload {
	files := []interface{}{
		map[string]interface{}{"length": int64(30000), "path": []interface{}{"ep1\ufeff.mkv"}},
		map[string]interface{}{"length": int64(9000), "path": []interface{}{"Caf\xe9", "notes\t1.txt"}},
		map[string]interface{}{"length": int64(1000), "path": []interface{}{"plain.nfo"}},
	}
	root, newRoot := "\ufeffShow \u200bZWSP", "Show ZWSP"
	return newLegacyPayload(t, root, files, []int64{30000, 9000, 1000},
		[]string{
			filepath.Join(root, "ep1\ufeff.mkv"),
			filepath.Join(root, "Caf\xe9", "notes\t1.txt"),
			filepath.Join(root, "plain.nfo"),
		},
		[]string{
			filepath.Join(newRoot, "ep1.mkv"),
			filepath.Join(newRoot, "Caf_", "notes_1.txt"),
			filepath.Join(newRoot, "plain.nfo"),
		})
}

// TestAddTorrentMovesLegacyFiles: the stricter sanitizer changed the on-disk
// names of files carrying bidi marks, a BOM, control characters or invalid
// UTF-8. After an upgrade their torrents found nothing at the new names,
// created empty files there and downloaded everything again, orphaning the
// old files. Adding the torrent now moves them and verifies them in place.
func TestAddTorrentMovesLegacyFiles(t *testing.T) {
	for name, build := range map[string]func(*testing.T) *legacyPayload{
		"single file": singleLegacyPayload,
		"multi file":  multiLegacyPayload,
	} {
		t.Run(name, func(t *testing.T) {
			p := build(t)
			downloadDir := t.TempDir()
			p.writeLegacyFiles(t, downloadDir)
			mgr := NewTorrentManager()
			defer mgr.Close()
			sess, err := mgr.AddTorrentFile(p.writeTorrent(t, t.TempDir()), downloadDir)
			if err != nil {
				t.Fatal(err)
			}
			p.checkMigrated(t, downloadDir)
			checkAllVerified(t, sess)
		})
	}
}

// TestRestoreMovesLegacyFilesAndSaysSo: restore is where an upgrade meets the
// old names, and the startup line reports the move.
func TestRestoreMovesLegacyFilesAndSaysSo(t *testing.T) {
	p := multiLegacyPayload(t)
	downloadDir := t.TempDir()
	p.writeLegacyFiles(t, downloadDir)
	hashHex := fmt.Sprintf("%x", p.infoHash)
	cached, err := bencode.Marshal(map[string]interface{}{"info": p.info})
	if err != nil {
		t.Fatal(err)
	}
	configDir := writeRestoreState(t, []PersistedTorrent{{InfoHashHex: hashHex, DownloadDir: downloadDir}},
		map[string][]byte{hashHex: cached})

	mgr := NewTorrentManager()
	defer mgr.Close()
	warning, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(warning, "renamed 3 file(s) of 1 torrent(s)") || !strings.Contains(warning, "Show ZWSP") {
		t.Fatalf("startup notice = %q, want the move reported", warning)
	}
	sess := mgr.GetSession(hashHex)
	if sess == nil {
		t.Fatal("torrent not restored")
	}
	p.checkMigrated(t, downloadDir)
	checkAllVerified(t, sess)

	// A later launch has nothing left to move and says nothing.
	mgr.Close()
	next := NewTorrentManager()
	defer next.Close()
	if warning, err := next.EnablePersistence(configDir); err != nil || strings.Contains(warning, "renamed") {
		t.Fatalf("second launch: %q, %v; want no notice", warning, err)
	}
}

// TestMagnetMetadataMovesLegacyFiles: a magnet restored without its metadata
// learns the names only when the info dict arrives.
func TestMagnetMetadataMovesLegacyFiles(t *testing.T) {
	p := singleLegacyPayload(t)
	downloadDir := t.TempDir()
	p.writeLegacyFiles(t, downloadDir)
	mgr := NewTorrentManager()
	defer mgr.Close()
	sess, err := mgr.AddMagnet(fmt.Sprintf("magnet:?xt=urn:btih:%x", p.infoHash), downloadDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.onMetadataDownloaded(p.infoBytes); err != nil {
		t.Fatalf("onMetadataDownloaded: %v", err)
	}
	p.checkMigrated(t, downloadDir)
	checkAllVerified(t, sess)
}

// TestLegacyFileHeldByAnotherTorrentStays: a legacy path another torrent holds
// is that torrent's file, not an old copy of this one's.
func TestLegacyFileHeldByAnotherTorrentStays(t *testing.T) {
	p := singleLegacyPayload(t)
	downloadDir := t.TempDir()
	p.writeLegacyFiles(t, downloadDir)
	mgr := NewTorrentManager()
	defer mgr.Close()
	release, err := mgr.claimPaths([20]byte{1}, downloadDir, []storage.FileInfo{{Path: p.legacy[0], Length: p.lengths[0]}})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := mgr.AddTorrentFile(p.writeTorrent(t, t.TempDir()), downloadDir); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(downloadDir, p.legacy[0])); err != nil || !bytes.Equal(got, p.data) {
		t.Fatalf("another torrent's file changed: %d bytes, %v", len(got), err)
	}
	checkNoMoveReservations(t, mgr)
}

// TestLegacyMigrationLeavesNoReservation: the reservation taken for the move is
// given back, so the old path can be claimed again afterwards.
func TestLegacyMigrationLeavesNoReservation(t *testing.T) {
	p := singleLegacyPayload(t)
	downloadDir := t.TempDir()
	p.writeLegacyFiles(t, downloadDir)
	mgr := NewTorrentManager()
	defer mgr.Close()
	if _, err := mgr.AddTorrentFile(p.writeTorrent(t, t.TempDir()), downloadDir); err != nil {
		t.Fatal(err)
	}
	p.checkMigrated(t, downloadDir)
	checkNoMoveReservations(t, mgr)
	release, err := mgr.claimPaths([20]byte{2}, downloadDir, []storage.FileInfo{{Path: p.legacy[0], Length: 1}})
	if err != nil {
		t.Fatalf("old path still reserved after the move: %v", err)
	}
	release()
}

func checkNoMoveReservations(t *testing.T, mgr *TorrentManager) {
	t.Helper()
	mgr.claimMu.Lock()
	defer mgr.claimMu.Unlock()
	for key, c := range mgr.pathClaims {
		if c.deleting != 0 {
			t.Fatalf("claim %v still reserved for a move: %+v", key, c)
		}
	}
}

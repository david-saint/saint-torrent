package downloader

import (
	"crypto/sha1"
	"os"
	"path/filepath"
	"testing"

	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func TestTorrentManager(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sainttorrent_manager_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tor := &torrent.Torrent{
		Name:        "managertest.txt",
		PieceLength: 32768,
		PieceHashes: [][20]byte{sha1.Sum([]byte("data"))},
		Files: []torrent.File{
			{Length: 32768, Path: []string{"managertest.txt"}},
		},
	}

	files := []storage.FileInfo{
		{Path: filepath.Join(tor.Files[0].Path...), Length: tor.Files[0].Length},
	}
	st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	peerID := [20]byte{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}
	sess, err := NewSession(tor, st, peerID, 6881, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	mgr := NewTorrentManager()
	infoHashHex := "abcdef0123456789abcd"

	// Initially no sessions
	if len(mgr.ListSessions()) != 0 {
		t.Errorf("expected 0 sessions, got %d", len(mgr.ListSessions()))
	}

	// Add session
	mgr.AddSession(infoHashHex, sess)
	if len(mgr.ListSessions()) != 1 {
		t.Errorf("expected 1 session, got %d", len(mgr.ListSessions()))
	}

	// Check retrieved session
	retrieved := mgr.GetSession(infoHashHex)
	if retrieved != sess {
		t.Error("failed to retrieve correct session")
	}

	// Test global limits propagation
	mgr.SetGlobalDownloadLimit(100000)
	mgr.SetGlobalUploadLimit(50000)

	if mgr.GlobalDownloadLimit() != 100000 {
		t.Errorf("expected global download limit 100000, got %d", mgr.GlobalDownloadLimit())
	}
	if mgr.GlobalUploadLimit() != 50000 {
		t.Errorf("expected global upload limit 50000, got %d", mgr.GlobalUploadLimit())
	}

	// Session's global limiters should be linked and reflect these limits
	sess.mu.RLock()
	globalDl := sess.GlobalDownloadLimiter
	globalUl := sess.GlobalUploadLimiter
	sess.mu.RUnlock()

	if globalDl == nil || globalDl.Limit() != 100000 {
		t.Errorf("expected session global download limit 100000, got %v", globalDl)
	}
	if globalUl == nil || globalUl.Limit() != 50000 {
		t.Errorf("expected session global upload limit 50000, got %v", globalUl)
	}

	// Test PauseAll / ResumeAll
	mgr.PauseAll()
	if !sess.IsPaused() {
		t.Error("expected session to be paused via PauseAll")
	}

	mgr.ResumeAll()
	if sess.IsPaused() {
		t.Error("expected session to be resumed via ResumeAll")
	}

	// Remove session
	mgr.RemoveSession(infoHashHex)
	if len(mgr.ListSessions()) != 0 {
		t.Errorf("expected 0 sessions after removal, got %d", len(mgr.ListSessions()))
	}

	mgr.Close()
}

func TestManagerCloseClosesSessionsBeforeDHT(t *testing.T) {
	tempDir := t.TempDir()
	mgr := NewTorrentManager()

	tor := &torrent.Torrent{
		Name:        "close-order.txt",
		InfoHash:    sha1.Sum([]byte("close-order")),
		PieceLength: 1,
		PieceHashes: [][20]byte{sha1.Sum([]byte("x"))},
		Files:       []torrent.File{{Length: 1, Path: []string{"close-order.txt"}}},
	}
	st, err := storage.NewStorage(tempDir, []storage.FileInfo{{Path: "close-order.txt", Length: 1}}, 1)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	mgr.AddSession("close-order", sess)
	if err := mgr.StartDHT(tempDir, 0); err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	if sess.DHT == nil {
		t.Fatal("expected DHT to attach to existing session")
	}

	mgr.Close()
	sess.mu.RLock()
	closed := sess.closed
	sess.mu.RUnlock()
	if !closed {
		t.Fatal("expected manager close to close session")
	}
}

func TestTorrentManagerAddMethods(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sainttorrent_manager_add_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mgr := NewTorrentManager()
	defer mgr.Close()

	// Test AddMagnet
	magnetURI := "magnet:?xt=urn:btih:2b2f67ccacfeef7b0bf21dffb0f6936e72c59f21&dn=TestTorrent&tr=http%3A%2F%2Ftracker.co%2Fannounce"
	sess, err := mgr.AddMagnet(magnetURI, tempDir)
	if err != nil {
		t.Fatalf("AddMagnet failed: %v", err)
	}
	if sess == nil {
		t.Fatal("AddMagnet returned nil session")
	}

	if sess.Torrent.Name != "TestTorrent" {
		t.Errorf("expected name TestTorrent, got %q", sess.Torrent.Name)
	}
	if !sess.IsMetadataMode() {
		t.Error("expected session to start in metadata mode")
	}

	// AddMagnet duplicate returns existing session
	sess2, err := mgr.AddMagnet(magnetURI, tempDir)
	if err != nil {
		t.Fatalf("AddMagnet duplicate failed: %v", err)
	}
	if sess2 != sess {
		t.Errorf("expected duplicate AddMagnet to return same session")
	}
}

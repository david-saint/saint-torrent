package downloader

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
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

func TestPersistenceRestorePaused(t *testing.T) {
	tempDir := t.TempDir()

	mgr := NewTorrentManager()
	
	// Create a dummy config dir
	configDir := filepath.Join(tempDir, "config")
	if err := os.MkdirAll(filepath.Join(configDir, "torrents"), 0755); err != nil {
		t.Fatalf("failed to create torrents dir: %v", err)
	}

	// Create a dummy bencoded torrent info bytes
	infoBytes := []byte("d6:lengthi100e4:name9:dummy.txt12:piece lengthi32768e6:pieces20:12345678901234567890e")
	dummyTorrent := []byte("d8:announce27:http://tracker.org/announce4:infod6:lengthi100e4:name9:dummy.txt12:piece lengthi32768e6:pieces20:12345678901234567890ee")

	infoHash := sha1.Sum(infoBytes)
	infoHashHex := fmt.Sprintf("%x", infoHash)

	// Write dummy torrent to cache
	cachedPath := filepath.Join(configDir, "torrents", infoHashHex+".torrent")
	if err := os.WriteFile(cachedPath, dummyTorrent, 0644); err != nil {
		t.Fatalf("failed to write cached torrent: %v", err)
	}

	// Write session.json with a paused torrent
	state := PersistedState{
		Version: 1,
		Torrents: []PersistedTorrent{
			{
				InfoHashHex: infoHashHex,
				DownloadDir: tempDir,
				Paused:      true,
			},
		},
	}
	data, _ := json.Marshal(state)
	if err := os.WriteFile(filepath.Join(configDir, "session.json"), data, 0644); err != nil {
		t.Fatalf("failed to write session.json: %v", err)
	}

	// Enable persistence
	warning, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}
	if warning != "" {
		t.Errorf("unexpected warning: %q", warning)
	}

	sessions := mgr.ListSessions()
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session restored, got %d", len(sessions))
	}

	sess := sessions[0]
	if !sess.IsPaused() {
		t.Error("expected session to be restored as paused")
	}

	// Start session loops
	sess.Start()

	// Verify it is still paused
	sess.mu.RLock()
	paused := sess.paused
	trackerEvents := sess.trackerEvents
	sess.mu.RUnlock()

	if !paused {
		t.Error("expected session to remain paused after Start()")
	}

	// Verify no "started" event is queued
	for _, ev := range trackerEvents {
		if ev == "started" {
			t.Error("expected no 'started' tracker event for paused startup")
		}
	}

	mgr.Close()
}

func TestPersistenceMagnetStateTransitions(t *testing.T) {
	tempDir := t.TempDir()

	mgr := NewTorrentManager()
	configDir := filepath.Join(tempDir, "config")
	
	magnetURI := "magnet:?xt=urn:btih:4cf469d37a3b7f65fbdb6bf8b36e6ab495e5a0fb&dn=TestTorrent"
	mag, _ := torrent.ParseMagnet(magnetURI)
	infoHashHex := fmt.Sprintf("%x", mag.InfoHash)

	// Write session.json with a magnet torrent and custom file priorities
	state := PersistedState{
		Version: 1,
		Torrents: []PersistedTorrent{
			{
				InfoHashHex:    infoHashHex,
				MagnetURI:      magnetURI,
				DownloadDir:    tempDir,
				Paused:         false,
				FilePriorities: []FilePriority{PriorityHigh, PrioritySkip},
			},
		},
	}
	data, _ := json.Marshal(state)
	_ = os.MkdirAll(filepath.Join(configDir, "torrents"), 0755)
	_ = os.WriteFile(filepath.Join(configDir, "session.json"), data, 0644)

	_, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}

	sess := mgr.GetSession(infoHashHex)
	if sess == nil {
		t.Fatal("session not restored")
	}

	// Verify pending file priorities are stored
	sess.mu.RLock()
	pending := sess.PendingFilePriorities
	sess.mu.RUnlock()

	if len(pending) != 2 || pending[0] != PriorityHigh || pending[1] != PrioritySkip {
		t.Errorf("unexpected pending priorities: %v", pending)
	}

	// Simulate metadata download completion
	// metadata info bytes: dict with files list
	infoBytes := []byte("d5:filesld6:lengthi100e4:pathl9:file1.txteed6:lengthi200e4:pathl9:file2.txteee4:name4:test12:piece lengthi32768e6:pieces20:12345678901234567890e")
	err = sess.onMetadataDownloaded(infoBytes)
	if err != nil {
		t.Fatalf("onMetadataDownloaded failed: %v", err)
	}

	// Verify priorities are applied
	sess.mu.RLock()
	priorities := sess.FilePriorities
	clearedPending := sess.PendingFilePriorities
	sess.mu.RUnlock()

	if len(priorities) != 2 || priorities[0] != PriorityHigh || priorities[1] != PrioritySkip {
		t.Errorf("expected priorities to be applied, got %v", priorities)
	}

	if clearedPending != nil {
		t.Error("expected PendingFilePriorities to be cleared after application")
	}

	mgr.Close()
}

func TestPersistenceAtomicWriteCorruption(t *testing.T) {
	tempDir := t.TempDir()

	mgr := NewTorrentManager()
	configDir := filepath.Join(tempDir, "config")
	_ = os.MkdirAll(filepath.Join(configDir, "torrents"), 0755)

	// Write invalid JSON to session.json
	statePath := filepath.Join(configDir, "session.json")
	_ = os.WriteFile(statePath, []byte("{invalid json}"), 0644)

	warning, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("expected nil error on corruption recovery, got %v", err)
	}
	if warning == "" {
		t.Error("expected non-fatal warning on corruption recovery")
	}

	// Verify corrupted file was renamed and timestamped
	files, _ := os.ReadDir(configDir)
	foundCorrupt := false
	for _, f := range files {
		if len(f.Name()) > len("session.json.corrupt_") && f.Name()[:len("session.json.corrupt_")] == "session.json.corrupt_" {
			foundCorrupt = true
			break
		}
	}
	if !foundCorrupt {
		t.Error("expected corrupted session.json to be renamed with a timestamp suffix")
	}

	mgr.Close()
}

func TestRawTorrentReconstruction(t *testing.T) {
	tempDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(tempDir, "torrents"), 0755)

	// Create dummy info bytes
	infoBytes := []byte("d6:lengthi100e4:name9:dummy.txt12:piece lengthi32768e6:pieces20:12345678901234567890e")
	infoHash := sha1.Sum(infoBytes)

	tor := &torrent.Torrent{
		Announce:  "http://tracker.org/announce",
		Trackers:  []string{"http://tracker.org/announce", "http://tracker2.org/announce"},
		InfoHash:  infoHash,
		InfoBytes: infoBytes,
	}

	err := reconstructAndWriteTorrent(tempDir, fmt.Sprintf("%x", infoHash), tor)
	if err != nil {
		t.Fatalf("reconstructAndWriteTorrent failed: %v", err)
	}

	destPath := filepath.Join(tempDir, "torrents", fmt.Sprintf("%x", infoHash)+".torrent")
	data, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("failed to read reconstructed torrent: %v", err)
	}

	// Parse reconstructed torrent
	parsed, err := torrent.Parse(data)
	if err != nil {
		t.Fatalf("torrent parsing failed: %v", err)
	}

	if parsed.Announce != "http://tracker.org/announce" {
		t.Errorf("expected announce 'http://tracker.org/announce', got %q", parsed.Announce)
	}
	if len(parsed.Trackers) != 2 || parsed.Trackers[1] != "http://tracker2.org/announce" {
		t.Errorf("unexpected trackers: %v", parsed.Trackers)
	}
	if fmt.Sprintf("%x", parsed.InfoHash) != fmt.Sprintf("%x", infoHash) {
		t.Errorf("info hash mismatch: expected %x, got %x", infoHash, parsed.InfoHash)
	}
}

func TestPersistenceLimitsOnlyRestore(t *testing.T) {
	tempDir := t.TempDir()

	mgr := NewTorrentManager()
	configDir := filepath.Join(tempDir, "config")
	_ = os.MkdirAll(filepath.Join(configDir, "torrents"), 0755)

	// Write session.json with only global limits
	state := PersistedState{
		Version:             1,
		GlobalDownloadLimit: 123456,
		GlobalUploadLimit:   654321,
		Torrents:            nil,
	}
	data, _ := json.Marshal(state)
	_ = os.WriteFile(filepath.Join(configDir, "session.json"), data, 0644)

	_, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}

	if mgr.GlobalDownloadLimit() != 123456 {
		t.Errorf("expected download limit 123456, got %d", mgr.GlobalDownloadLimit())
	}
	if mgr.GlobalUploadLimit() != 654321 {
		t.Errorf("expected upload limit 654321, got %d", mgr.GlobalUploadLimit())
	}

	mgr.Close()
}

func TestPersistenceCorruptCacheFallback(t *testing.T) {
	tempDir := t.TempDir()

	mgr := NewTorrentManager()
	configDir := filepath.Join(tempDir, "config")
	_ = os.MkdirAll(filepath.Join(configDir, "torrents"), 0755)

	magnetURI := "magnet:?xt=urn:btih:4cf469d37a3b7f65fbdb6bf8b36e6ab495e5a0fb&dn=TestTorrent"
	mag, _ := torrent.ParseMagnet(magnetURI)
	infoHashHex := fmt.Sprintf("%x", mag.InfoHash)

	// Write corrupted bencoded data to cached torrent file path
	cachedPath := filepath.Join(configDir, "torrents", infoHashHex+".torrent")
	_ = os.WriteFile(cachedPath, []byte("invalid bencode data"), 0644)

	// Write session.json referencing the corrupted cached torrent and magnet link
	state := PersistedState{
		Version: 1,
		Torrents: []PersistedTorrent{
			{
				InfoHashHex: infoHashHex,
				MagnetURI:      magnetURI,
				DownloadDir:    tempDir,
				Paused:         false,
			},
		},
	}
	data, _ := json.Marshal(state)
	_ = os.WriteFile(filepath.Join(configDir, "session.json"), data, 0644)

	_, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}

	sess := mgr.GetSession(infoHashHex)
	if sess == nil {
		t.Fatal("expected session to be restored via magnet URI fallback")
	}

	if !sess.IsMetadataMode() {
		t.Error("expected fallback session to be in metadata mode")
	}

	mgr.Close()
}

func TestPersistenceFailedRestorePreservation(t *testing.T) {
	tempDir := t.TempDir()

	mgr := NewTorrentManager()
	configDir := filepath.Join(tempDir, "config")
	_ = os.MkdirAll(filepath.Join(configDir, "torrents"), 0755)

	// Save a torrent that cannot be loaded (no cached torrent, no magnet URI)
	infoHashHex := "1111111111111111111111111111111111111111"
	state := PersistedState{
		Version: 1,
		Torrents: []PersistedTorrent{
			{
				InfoHashHex: infoHashHex,
				DownloadDir: tempDir,
				Paused:      true,
			},
		},
	}
	data, _ := json.Marshal(state)
	_ = os.WriteFile(filepath.Join(configDir, "session.json"), data, 0644)

	_, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}

	// Verify it was NOT loaded in sessions list
	if sess := mgr.GetSession(infoHashHex); sess != nil {
		t.Fatal("expected session to fail loading")
	}

	// Trigger manual save
	mgr.saveState()

	// Verify the entry was preserved in session.json
	restoredData, err := os.ReadFile(filepath.Join(configDir, "session.json"))
	if err != nil {
		t.Fatalf("failed to read session.json: %v", err)
	}

	var restoredState PersistedState
	if err := json.Unmarshal(restoredData, &restoredState); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	found := false
	for _, entry := range restoredState.Torrents {
		if entry.InfoHashHex == infoHashHex {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected failed torrent entry to be preserved in session.json")
	}

	mgr.Close()
}

func TestPersistenceConcurrentSaves(t *testing.T) {
	tempDir := t.TempDir()

	mgr := NewTorrentManager()
	configDir := filepath.Join(tempDir, "config")
	_ = os.MkdirAll(filepath.Join(configDir, "torrents"), 0755)

	_, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}

	// Add a dummy session
	sess := &Session{
		Torrent: &torrent.Torrent{
			InfoHash: [20]byte{1, 2, 3},
		},
		downloadDir: tempDir,
	}
	mgr.AddSession("010203", sess)

	// Run concurrent state saves
	const goroutines = 20
	done := make(chan bool)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			for j := 0; j < 50; j++ {
				mgr.SetGlobalDownloadLimit(int64(1000 + id*j))
			}
			done <- true
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}

	mgr.Close()
}

func TestPersistenceRemoveSessionNoOp(t *testing.T) {
	tempDir := t.TempDir()

	mgr := NewTorrentManager()
	configDir := filepath.Join(tempDir, "config")
	_ = os.MkdirAll(filepath.Join(configDir, "torrents"), 0755)

	_, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}

	// Remove session.json so we can check if it gets created
	sessionPath := filepath.Join(configDir, "session.json")
	_ = os.Remove(sessionPath)

	mgr.RemoveSession("0000000000000000000000000000000000000000")

	if _, err := os.Stat(sessionPath); !os.IsNotExist(err) {
		t.Error("expected session.json to NOT be created on no-op RemoveSession")
	}

	mgr.Close()
}

func TestPersistenceCloseReconstructsMagnet(t *testing.T) {
	tempDir := t.TempDir()

	mgr := NewTorrentManager()
	configDir := filepath.Join(tempDir, "config")
	_ = os.MkdirAll(filepath.Join(configDir, "torrents"), 0755)

	_, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}

	// Create dummy info bytes representing a valid bencoded dict
	dummyInfoBytes := []byte("d6:lengthi100e4:name4:test12:piece lengthi256e6:pieces20:01234567890123456789e")
	infoHash := sha1.Sum(dummyInfoBytes)
	infoHashHex := fmt.Sprintf("%x", infoHash)
	
	sess := &Session{
		Torrent: &torrent.Torrent{
			InfoHash:  infoHash,
			InfoBytes: dummyInfoBytes,
			Trackers:  []string{"http://tracker.org"},
		},
		downloadDir:  tempDir,
		metadataMode: false, // completed
		MagnetURI:    "magnet:?xt=urn:btih:0a141e&dn=test",
	}

	mgr.AddSession(infoHashHex, sess)

	// Verify the .torrent file does not exist yet
	cachedPath := filepath.Join(configDir, "torrents", infoHashHex+".torrent")
	if _, err := os.Stat(cachedPath); !os.IsNotExist(err) {
		t.Fatal("expected cached .torrent to not exist initially")
	}

	// Close the manager, which should trigger saveStateInternal(true) and reconstruct the torrent file
	mgr.Close()

	if _, err := os.Stat(cachedPath); os.IsNotExist(err) {
		t.Error("expected cached .torrent file to be reconstructed and written on Close")
	} else {
		// Verify bencode parser can parse the reconstructed torrent
		data, err := os.ReadFile(cachedPath)
		if err != nil {
			t.Fatalf("failed to read reconstructed file: %v", err)
		}
		parsed, err := torrent.Parse(data)
		if err != nil {
			t.Fatalf("failed to parse reconstructed torrent: %v", err)
		}
		if bytes.Compare(parsed.InfoHash[:], infoHash[:]) != 0 {
			t.Errorf("info hash mismatch: expected %v, got %v", infoHash, parsed.InfoHash)
		}
	}
}

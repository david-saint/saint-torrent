package downloader

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
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
	if err := mgr.RemoveSession(infoHashHex, false); err != nil {
		t.Errorf("unexpected error on RemoveSession: %v", err)
	}
	if len(mgr.ListSessions()) != 0 {
		t.Errorf("expected 0 sessions after removal, got %d", len(mgr.ListSessions()))
	}
	if _, err := st.ReadBlock(0, 0, make([]byte, tor.PieceLength)); !errors.Is(err, storage.ErrStorageClosed) {
		t.Fatalf("expected removal to close session storage, got %v", err)
	}

	mgr.Close()
}

func TestManagerSaveStateReturnsWriteErrors(t *testing.T) {
	mgr := NewTorrentManager()
	mgr.stateDir = filepath.Join(t.TempDir(), "missing")

	if err := mgr.saveState(); err == nil {
		t.Fatal("expected saveState to return an error for a missing state directory")
	}
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

func TestTorrentManagerStorageBackendSelection(t *testing.T) {
	tempDir := t.TempDir()
	mgr := NewTorrentManager()
	defer mgr.Close()
	if err := mgr.SetStorageBackend(storage.BackendMemory); err != nil {
		t.Fatalf("SetStorageBackend: %v", err)
	}

	torrentData := []byte("d8:announce27:http://tracker.org/announce4:infod6:lengthi100e4:name9:dummy.txt12:piece lengthi32768e6:pieces20:12345678901234567890ee")
	torrentPath := filepath.Join(tempDir, "dummy.torrent")
	if err := os.WriteFile(torrentPath, torrentData, 0644); err != nil {
		t.Fatalf("write torrent: %v", err)
	}

	sess, err := mgr.AddTorrentFile(torrentPath, tempDir)
	if err != nil {
		t.Fatalf("AddTorrentFile: %v", err)
	}
	if _, ok := sess.Storage.(*storage.MemStorage); !ok {
		t.Fatalf("session storage = %T, want *storage.MemStorage", sess.Storage)
	}
	if _, err := os.Stat(filepath.Join(tempDir, "dummy.txt")); !os.IsNotExist(err) {
		t.Fatalf("memory backend should not create content file, stat err: %v", err)
	}
}

func TestTorrentManagerStorageBackendSelectionForMagnetMetadata(t *testing.T) {
	tempDir := t.TempDir()
	mgr := NewTorrentManager()
	defer mgr.Close()
	if err := mgr.SetStorageBackend(storage.BackendMemory); err != nil {
		t.Fatalf("SetStorageBackend: %v", err)
	}

	infoBytes := []byte("d6:lengthi100e4:name9:dummy.txt12:piece lengthi32768e6:pieces20:12345678901234567890e")
	infoHash := sha1.Sum(infoBytes)
	magnetURI := "magnet:?xt=urn:btih:" + fmt.Sprintf("%x", infoHash) + "&dn=Dummy"
	sess, err := mgr.AddMagnet(magnetURI, tempDir)
	if err != nil {
		t.Fatalf("AddMagnet: %v", err)
	}

	if err := sess.onMetadataDownloaded(infoBytes); err != nil {
		t.Fatalf("onMetadataDownloaded: %v", err)
	}
	sess.WaitVerified()
	if _, ok := sess.Storage.(*storage.MemStorage); !ok {
		t.Fatalf("session storage = %T, want *storage.MemStorage", sess.Storage)
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
	pending := sess.pendingFilePriorities
	announceBeforeMetadata := sess.allowsDHTAnnounceLocked()
	sess.mu.RUnlock()

	if len(pending) != 2 || pending[0] != PriorityHigh || pending[1] != PrioritySkip {
		t.Errorf("unexpected pending priorities: %v", pending)
	}
	if announceBeforeMetadata {
		t.Fatal("restored magnet should suppress DHT announce until metadata is known")
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
	priorities := sess.filePriorities
	clearedPending := sess.pendingFilePriorities
	announceAfterMetadata := sess.allowsDHTAnnounceLocked()
	sess.mu.RUnlock()

	if len(priorities) != 2 || priorities[0] != PriorityHigh || priorities[1] != PrioritySkip {
		t.Errorf("expected priorities to be applied, got %v", priorities)
	}

	if clearedPending != nil {
		t.Error("expected PendingFilePriorities to be cleared after application")
	}
	if !announceAfterMetadata {
		t.Error("expected restored public magnet to allow DHT announce after metadata is known")
	}

	mgr.Close()
}

func TestPersistenceRestoreTorrentPrioritiesRebuildNeededBuckets(t *testing.T) {
	tempDir := t.TempDir()
	configDir := filepath.Join(tempDir, "config")
	if err := os.MkdirAll(filepath.Join(configDir, "torrents"), 0755); err != nil {
		t.Fatalf("failed to create torrent cache dir: %v", err)
	}

	pieceData := [][]byte{
		[]byte("low-priority----"),
		[]byte("high-priority---"),
	}
	var pieces []byte
	for _, data := range pieceData {
		hash := sha1.Sum(data)
		pieces = append(pieces, hash[:]...)
	}
	info := map[string]interface{}{
		"name":         "priority-restore",
		"piece length": int64(16),
		"pieces":       pieces,
		"files": []interface{}{
			map[string]interface{}{
				"length": int64(16),
				"path":   []interface{}{"low.bin"},
			},
			map[string]interface{}{
				"length": int64(16),
				"path":   []interface{}{"high.bin"},
			},
		},
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatalf("failed to encode info dict: %v", err)
	}
	infoHash := sha1.Sum(infoBytes)
	infoHashHex := fmt.Sprintf("%x", infoHash)
	torrentBytes, err := bencode.Marshal(map[string]interface{}{
		"announce": "http://tracker.org/announce",
		"info":     info,
	})
	if err != nil {
		t.Fatalf("failed to encode torrent: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "torrents", infoHashHex+".torrent"), torrentBytes, 0644); err != nil {
		t.Fatalf("failed to write cached torrent: %v", err)
	}

	state := PersistedState{
		Version: 1,
		Torrents: []PersistedTorrent{
			{
				InfoHashHex:    infoHashHex,
				DownloadDir:    tempDir,
				FilePriorities: []FilePriority{PriorityNormal, PriorityHigh},
			},
		},
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("failed to encode persisted state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "session.json"), stateBytes, 0644); err != nil {
		t.Fatalf("failed to write session.json: %v", err)
	}

	mgr := NewTorrentManager()
	_, err = mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}
	defer mgr.Close()

	sess := mgr.GetSession(infoHashHex)
	if sess == nil {
		t.Fatal("session not restored")
	}
	sess.mu.Lock()
	got := sess.selectNeededPieceLocked(func(int64) bool { return true })
	sess.mu.Unlock()

	if got != 1 {
		t.Fatalf("expected restored PriorityHigh file piece 1 to be selected first, got %d", got)
	}
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
				MagnetURI:   magnetURI,
				DownloadDir: tempDir,
				Paused:      false,
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

	warning, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}

	// Verify it was NOT loaded in sessions list
	if sess := mgr.GetSession(infoHashHex); sess != nil {
		t.Fatal("expected session to fail loading")
	}

	// The failure must be surfaced to the user, not swallowed silently.
	if !strings.Contains(warning, "failed to restore") {
		t.Errorf("expected restore failure to be surfaced in warning, got %q", warning)
	}
	if !strings.Contains(warning, infoHashHex[:12]) {
		t.Errorf("expected warning to identify the failed torrent %q, got %q", infoHashHex[:12], warning)
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

	_ = mgr.RemoveSession("0000000000000000000000000000000000000000", false)

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
		if !bytes.Equal(parsed.InfoHash[:], infoHash[:]) {
			t.Errorf("info hash mismatch: expected %v, got %v", infoHash, parsed.InfoHash)
		}
	}
}

func TestRemoveSession_DeleteTaskOnly(t *testing.T) {
	tempDir := t.TempDir()
	configDir := filepath.Join(tempDir, "config")
	downloadDir := filepath.Join(tempDir, "download")
	_ = os.MkdirAll(filepath.Join(configDir, "torrents"), 0755)
	_ = os.MkdirAll(downloadDir, 0755)

	mgr := NewTorrentManager()
	_, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}

	// Create dummy torrent files on disk
	filePath := filepath.Join(downloadDir, "testfile.txt")
	_ = os.WriteFile(filePath, []byte("dummy data"), 0644)

	// Create a dummy session
	infoHash := sha1.Sum([]byte("task-only"))
	infoHashHex := fmt.Sprintf("%x", infoHash)
	tor := &torrent.Torrent{
		Name:        "testfile.txt",
		InfoHash:    infoHash,
		PieceLength: 10,
		Files: []torrent.File{
			{Length: 10, Path: []string{"testfile.txt"}},
		},
	}
	st, _ := storage.NewStorage(downloadDir, []storage.FileInfo{{Path: "testfile.txt", Length: 10}}, 10)
	sess, err := NewSession(tor, st, [20]byte{}, 0, downloadDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	mgr.AddSession(infoHashHex, sess)

	// Write mock .state and .torrent cached files
	cachedTorrentPath := filepath.Join(configDir, "torrents", infoHashHex+".torrent")
	_ = os.WriteFile(cachedTorrentPath, []byte("d6:lengthi10e4:name12:testfile.txt12:piece lengthi10e6:pieces20:01234567890123456789e"), 0644)

	stateFilePath := filepath.Join(downloadDir, "."+infoHashHex+".state")
	_ = os.WriteFile(stateFilePath, []byte("state info"), 0644)

	// Remove session: delete task only (deleteFiles = false)
	err = mgr.RemoveSession(infoHashHex, false)
	if err != nil {
		t.Fatalf("RemoveSession failed: %v", err)
	}

	// Verify manager states
	if len(mgr.ListSessions()) != 0 {
		t.Error("session still exists in manager list")
	}

	// Verify cached .torrent file is deleted
	if _, err := os.Stat(cachedTorrentPath); !os.IsNotExist(err) {
		t.Error("expected cached .torrent file to be deleted")
	}

	// Verify fast-resume state file is deleted
	if _, err := os.Stat(stateFilePath); !os.IsNotExist(err) {
		t.Error("expected fast-resume state file to be deleted")
	}

	// Verify downloaded file is NOT deleted
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("expected downloaded file to remain on disk")
	}

	mgr.Close()
}

func TestRemoveSession_DeleteTaskAndFiles(t *testing.T) {
	tempDir := t.TempDir()
	configDir := filepath.Join(tempDir, "config")
	downloadDir := filepath.Join(tempDir, "download")
	_ = os.MkdirAll(filepath.Join(configDir, "torrents"), 0755)
	_ = os.MkdirAll(downloadDir, 0755)

	mgr := NewTorrentManager()
	_, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}

	// Create dummy torrent files on disk
	filePath := filepath.Join(downloadDir, "testfile.txt")
	_ = os.WriteFile(filePath, []byte("dummy data"), 0644)

	// Create a dummy session
	infoHash := sha1.Sum([]byte("task-and-files"))
	infoHashHex := fmt.Sprintf("%x", infoHash)
	tor := &torrent.Torrent{
		Name:        "testfile.txt",
		InfoHash:    infoHash,
		PieceLength: 10,
		Files: []torrent.File{
			{Length: 10, Path: []string{"testfile.txt"}},
		},
	}
	st, _ := storage.NewStorage(downloadDir, []storage.FileInfo{{Path: "testfile.txt", Length: 10}}, 10)
	sess, err := NewSession(tor, st, [20]byte{}, 0, downloadDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	mgr.AddSession(infoHashHex, sess)

	// Write mock .state and .torrent cached files
	cachedTorrentPath := filepath.Join(configDir, "torrents", infoHashHex+".torrent")
	_ = os.WriteFile(cachedTorrentPath, []byte("d6:lengthi10e4:name12:testfile.txt12:piece lengthi10e6:pieces20:01234567890123456789e"), 0644)

	stateFilePath := filepath.Join(downloadDir, "."+infoHashHex+".state")
	_ = os.WriteFile(stateFilePath, []byte("state info"), 0644)

	// Remove session: delete task and files (deleteFiles = true)
	err = mgr.RemoveSession(infoHashHex, true)
	if err != nil {
		t.Fatalf("RemoveSession failed: %v", err)
	}

	// Verify manager states
	if len(mgr.ListSessions()) != 0 {
		t.Error("session still exists in manager list")
	}

	// Verify cached .torrent file is deleted
	if _, err := os.Stat(cachedTorrentPath); !os.IsNotExist(err) {
		t.Error("expected cached .torrent file to be deleted")
	}

	// Verify fast-resume state file is deleted
	if _, err := os.Stat(stateFilePath); !os.IsNotExist(err) {
		t.Error("expected fast-resume state file to be deleted")
	}

	// Verify downloaded file is deleted
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Error("expected downloaded file to be deleted from disk")
	}

	mgr.Close()
}

func TestRemoveSession_DirectoryCleanupBoundaries(t *testing.T) {
	tempDir := t.TempDir()
	configDir := filepath.Join(tempDir, "config")
	downloadDir := filepath.Join(tempDir, "download")
	_ = os.MkdirAll(filepath.Join(configDir, "torrents"), 0755)

	// Create nested structure:
	// download/my_torrent/sub1/empty/ - should be cleaned up
	// download/my_torrent/sub2/file_to_keep.txt - should be preserved
	// download/my_torrent/sub1/file_to_delete.txt - should be deleted, making sub1/empty empty, so both deleted
	sub1Empty := filepath.Join(downloadDir, "my_torrent", "sub1", "empty")
	sub2 := filepath.Join(downloadDir, "my_torrent", "sub2")
	_ = os.MkdirAll(sub1Empty, 0755)
	_ = os.MkdirAll(sub2, 0755)

	deleteFilePath := filepath.Join(downloadDir, "my_torrent", "sub1", "empty", "file_to_delete.txt")
	keepFilePath := filepath.Join(sub2, "file_to_keep.txt")

	_ = os.WriteFile(deleteFilePath, []byte("delete me"), 0644)
	_ = os.WriteFile(keepFilePath, []byte("keep me"), 0644)

	mgr := NewTorrentManager()
	_, _ = mgr.EnablePersistence(configDir)

	infoHash := sha1.Sum([]byte("dir-cleanup"))
	infoHashHex := fmt.Sprintf("%x", infoHash)
	tor := &torrent.Torrent{
		Name:        "my_torrent",
		InfoHash:    infoHash,
		PieceLength: 9,
		Files: []torrent.File{
			{Length: 9, Path: []string{"my_torrent", "sub1", "empty", "file_to_delete.txt"}},
		},
	}
	st, _ := storage.NewStorage(downloadDir, []storage.FileInfo{{Path: filepath.Join("my_torrent", "sub1", "empty", "file_to_delete.txt"), Length: 9}}, 9)
	sess, _ := NewSession(tor, st, [20]byte{}, 0, downloadDir)
	mgr.AddSession(infoHashHex, sess)

	// Remove session with deleteFiles = true
	err := mgr.RemoveSession(infoHashHex, true)
	if err != nil {
		t.Fatalf("RemoveSession failed: %v", err)
	}

	// 1. file_to_delete.txt is deleted
	if _, err := os.Stat(deleteFilePath); !os.IsNotExist(err) {
		t.Error("file_to_delete.txt was not deleted")
	}

	// 2. Empty directory sub1/empty and sub1 are deleted
	sub1EmptyDir := filepath.Dir(deleteFilePath)
	if _, err := os.Stat(sub1EmptyDir); !os.IsNotExist(err) {
		t.Error("sub1/empty directory was not cleaned up")
	}
	sub1Dir := filepath.Dir(sub1EmptyDir)
	if _, err := os.Stat(sub1Dir); !os.IsNotExist(err) {
		t.Error("sub1 directory was not cleaned up")
	}

	// 3. sub2 and keepFilePath are NOT deleted
	if _, err := os.Stat(keepFilePath); os.IsNotExist(err) {
		t.Error("file_to_keep.txt was deleted incorrectly")
	}
	if _, err := os.Stat(sub2); os.IsNotExist(err) {
		t.Error("sub2 directory was deleted incorrectly")
	}

	// 4. Root downloadDir is NOT deleted
	if _, err := os.Stat(downloadDir); os.IsNotExist(err) {
		t.Error("root downloadDir was deleted incorrectly")
	}

	mgr.Close()
}

func createMockSession(t *testing.T, tempDir, name string, hash [20]byte, addedAt time.Time, paused bool, completed bool) *Session {
	tor := &torrent.Torrent{
		Name:        name,
		InfoHash:    hash,
		PieceLength: 32768,
		PieceHashes: [][20]byte{sha1.Sum([]byte("data"))},
		Files: []torrent.File{
			{Length: 32768, Path: []string{name}},
		},
	}

	files := []storage.FileInfo{
		{Path: filepath.Join(tor.Files[0].Path...), Length: tor.Files[0].Length},
	}
	st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	peerID := [20]byte{1, 2, 3}
	sess, err := NewSession(tor, st, peerID, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.mu.Lock()
	sess.AddedAt = addedAt
	sess.paused = paused
	if completed {
		sess.PieceStates[0] = PieceCompleted
	}
	sess.recomputeStatsLocked()
	sess.mu.Unlock()

	return sess
}

func TestListSessionsSorting(t *testing.T) {
	tempDir := t.TempDir()
	mgr := NewTorrentManager()
	defer mgr.Close()

	now := time.Now()

	// Create sessions in random order
	// 1. Stopped (paused & completed), added now, name "Z", hash [20]byte{5} -> Priority 2
	s1 := createMockSession(t, tempDir, "Z", [20]byte{5}, now, true, true)

	// 2. Seeding (not paused & completed), added 1 hour ago, name "Y", hash [20]byte{4} -> Priority 1
	s2 := createMockSession(t, tempDir, "Y", [20]byte{4}, now.Add(-time.Hour), false, true)

	// 3. Downloading (not paused & not completed), added 2 hours ago, name "X", hash [20]byte{3} -> Priority 0
	s3 := createMockSession(t, tempDir, "X", [20]byte{3}, now.Add(-2*time.Hour), false, false)

	// 4. Downloading (not paused & not completed), added 3 hours ago, name "W", hash [20]byte{2} -> Priority 0
	s4 := createMockSession(t, tempDir, "W", [20]byte{2}, now.Add(-3*time.Hour), false, false)

	// 5. Downloading (not paused & not completed), added 3 hours ago, name "W", hash [20]byte{1} -> Priority 0 (same name and time as s4, but smaller hash)
	s5 := createMockSession(t, tempDir, "W", [20]byte{1}, now.Add(-3*time.Hour), false, false)

	// Add them to manager in a random order
	mgr.AddSession("05", s1)
	mgr.AddSession("04", s2)
	mgr.AddSession("03", s3)
	mgr.AddSession("02", s4)
	mgr.AddSession("01", s5)

	sessions := mgr.ListSessions()
	if len(sessions) != 5 {
		t.Fatalf("expected 5 sessions, got %d", len(sessions))
	}

	// Expected sorted order:
	// 1st: s5 (Priority 0/Downloading, added 3h ago, name "W", hash 01) -> oldest downloading, first by hash tiebreaker
	// 2nd: s4 (Priority 0/Downloading, added 3h ago, name "W", hash 02) -> oldest downloading, second by hash tiebreaker
	// 3rd: s3 (Priority 0/Downloading, added 2h ago, name "X")
	// 4th: s2 (Priority 1/Seeding, added 1h ago, name "Y")
	// 5th: s1 (Priority 2/Stopped, added now, name "Z")

	expectedOrder := []*Session{s5, s4, s3, s2, s1}
	for i, expected := range expectedOrder {
		if sessions[i] != expected {
			t.Errorf("at index %d: expected session %s (hash %v), got %s (hash %v)",
				i, expected.Torrent.Name, expected.Torrent.InfoHash,
				sessions[i].Torrent.Name, sessions[i].Torrent.InfoHash)
		}
	}
}

func TestPersistenceAddedAt(t *testing.T) {
	tempDir := t.TempDir()
	configDir := filepath.Join(tempDir, "config")
	_ = os.MkdirAll(filepath.Join(configDir, "torrents"), 0755)

	mgr := NewTorrentManager()

	// 1. Create a dummy torrent file in cache
	infoBytes := []byte("d6:lengthi100e4:name9:dummy.txt12:piece lengthi32768e6:pieces20:12345678901234567890e")
	dummyTorrent := []byte("d8:announce27:http://tracker.org/announce4:infod6:lengthi100e4:name9:dummy.txt12:piece lengthi32768e6:pieces20:12345678901234567890ee")
	infoHash := sha1.Sum(infoBytes)
	infoHashHex := fmt.Sprintf("%x", infoHash)

	cachedPath := filepath.Join(configDir, "torrents", infoHashHex+".torrent")
	if err := os.WriteFile(cachedPath, dummyTorrent, 0644); err != nil {
		t.Fatalf("failed to write cached torrent: %v", err)
	}

	// 2. Set the modification time of cachedPath to a specific time
	modTime := time.Date(2025, 5, 20, 10, 0, 0, 0, time.UTC)
	if err := os.Chtimes(cachedPath, modTime, modTime); err != nil {
		t.Fatalf("failed to set mod times: %v", err)
	}

	// 3. Write session.json with three torrent entries:
	// Entry 1: infoHashHex (has cached torrent file), added_at is zero/nil (fallback to cached file modTime)
	// Entry 2: magnet-only info hash, added_at is non-nil (persisted time)
	// Entry 3: magnet-only info hash, added_at is zero/nil (fallback to now)
	persistedTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	magnetHash := sha1.Sum([]byte("magnet-only"))
	magnetHashHex := fmt.Sprintf("%x", magnetHash)
	magnetURI := "magnet:?xt=urn:btih:" + magnetHashHex + "&dn=MagnetTorrent"

	magnetNoTimeHash := sha1.Sum([]byte("magnet-no-time"))
	magnetNoTimeHashHex := fmt.Sprintf("%x", magnetNoTimeHash)
	magnetNoTimeURI := "magnet:?xt=urn:btih:" + magnetNoTimeHashHex + "&dn=MagnetNoTimeTorrent"

	state := PersistedState{
		Version: 1,
		Torrents: []PersistedTorrent{
			{
				InfoHashHex: infoHashHex,
				DownloadDir: tempDir,
				Paused:      true,
			},
			{
				InfoHashHex: magnetHashHex,
				MagnetURI:   magnetURI,
				DownloadDir: tempDir,
				Paused:      true,
				AddedAt:     &persistedTime,
			},
			{
				InfoHashHex: magnetNoTimeHashHex,
				MagnetURI:   magnetNoTimeURI,
				DownloadDir: tempDir,
				Paused:      true,
			},
		},
	}

	data, _ := json.Marshal(state)
	if err := os.WriteFile(filepath.Join(configDir, "session.json"), data, 0644); err != nil {
		t.Fatalf("failed to write session.json: %v", err)
	}

	// Enable persistence to restore
	_, err := mgr.EnablePersistence(configDir)
	if err != nil {
		t.Fatalf("EnablePersistence failed: %v", err)
	}

	// Retrieve session 1 and verify AddedAt is the file's modification time (modTime)
	s1 := mgr.GetSession(infoHashHex)
	if s1 == nil {
		t.Fatal("session 1 was not restored")
	}
	s1.mu.RLock()
	s1AddedAt := s1.AddedAt
	s1.mu.RUnlock()
	if !s1AddedAt.Equal(modTime) {
		t.Errorf("expected session 1 AddedAt to fallback to cached file modTime %v, got %v", modTime, s1AddedAt)
	}

	// Retrieve session 2 and verify AddedAt is the persisted time
	s2 := mgr.GetSession(magnetHashHex)
	if s2 == nil {
		t.Fatal("session 2 was not restored")
	}
	s2.mu.RLock()
	s2AddedAt := s2.AddedAt
	s2.mu.RUnlock()
	if !s2AddedAt.Equal(persistedTime) {
		t.Errorf("expected session 2 AddedAt to be %v, got %v", persistedTime, s2AddedAt)
	}

	// Retrieve session 3 and verify AddedAt is non-zero (fallback to now)
	s3 := mgr.GetSession(magnetNoTimeHashHex)
	if s3 == nil {
		t.Fatal("session 3 was not restored")
	}
	s3.mu.RLock()
	s3AddedAt := s3.AddedAt
	s3.mu.RUnlock()
	if s3AddedAt.IsZero() {
		t.Error("expected session 3 AddedAt to be non-zero (fallback to now)")
	}
	if time.Since(s3AddedAt) > 10*time.Second {
		t.Errorf("expected session 3 AddedAt to be close to current time, got %v", s3AddedAt)
	}

	// 4. Close manager and check that session.json is rewritten with correct AddedAt values
	mgr.Close()

	restoredData, err := os.ReadFile(filepath.Join(configDir, "session.json"))
	if err != nil {
		t.Fatalf("failed to read written session.json: %v", err)
	}

	var restoredState PersistedState
	if err := json.Unmarshal(restoredData, &restoredState); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Verify entries now have correct added_at persisted in JSON
	foundS1 := false
	foundS2 := false
	foundS3 := false
	for _, entry := range restoredState.Torrents {
		if entry.InfoHashHex == infoHashHex {
			foundS1 = true
			if entry.AddedAt == nil || !entry.AddedAt.Equal(modTime) {
				t.Errorf("expected persisted s1 AddedAt to be %v, got %v", modTime, entry.AddedAt)
			}
		} else if entry.InfoHashHex == magnetHashHex {
			foundS2 = true
			if entry.AddedAt == nil || !entry.AddedAt.Equal(persistedTime) {
				t.Errorf("expected persisted s2 AddedAt to be %v, got %v", persistedTime, entry.AddedAt)
			}
		} else if entry.InfoHashHex == magnetNoTimeHashHex {
			foundS3 = true
			if entry.AddedAt == nil || entry.AddedAt.IsZero() {
				t.Errorf("expected persisted s3 AddedAt to be non-zero, got %v", entry.AddedAt)
			}
		}
	}
	if !foundS1 || !foundS2 || !foundS3 {
		t.Errorf("did not find all sessions in written session.json (foundS1=%v, foundS2=%v, foundS3=%v)", foundS1, foundS2, foundS3)
	}
}

func TestAddSessionBackfillsAddedAt(t *testing.T) {
	mgr := NewTorrentManager()
	defer mgr.Close()

	sess := &Session{
		Torrent: &torrent.Torrent{
			InfoHash: [20]byte{1, 2, 3},
		},
	}

	// sess.AddedAt is zero time
	if !sess.AddedAt.IsZero() {
		t.Fatalf("expected initial AddedAt to be zero")
	}

	mgr.AddSession("010203", sess)

	sess.mu.RLock()
	addedAt := sess.AddedAt
	sess.mu.RUnlock()

	if addedAt.IsZero() {
		t.Errorf("expected AddedAt to be backfilled, got zero time")
	}
	if time.Since(addedAt) > 10*time.Second {
		t.Errorf("expected AddedAt to be close to time.Now(), got %v", addedAt)
	}
}

// closeTrackingStorage is a minimal storage.Storage fake that records whether Close was called,
// so tests can assert a session's storage was released without touching the filesystem.
type closeTrackingStorage struct {
	closed atomic.Bool
}

func (s *closeTrackingStorage) BaseDir() string                    { return "" }
func (s *closeTrackingStorage) TotalSize() int64                   { return 0 }
func (s *closeTrackingStorage) PieceLengthValue() int64            { return 0 }
func (s *closeTrackingStorage) PieceLength(pieceIndex int64) int64 { return 0 }
func (s *closeTrackingStorage) ReadBlock(int64, int64, []byte) (int, error) {
	return 0, nil
}
func (s *closeTrackingStorage) WriteBlock(int64, int64, []byte) error { return nil }
func (s *closeTrackingStorage) VerifyPiece(int64, [20]byte) (bool, error) {
	return false, nil
}
func (s *closeTrackingStorage) SaveState(string, []int) error   { return nil }
func (s *closeTrackingStorage) LoadState(string) ([]int, error) { return nil, nil }
func (s *closeTrackingStorage) Close() error {
	s.closed.Store(true)
	return nil
}

// TestAddSessionClosesReplacedSession is a regression test for issue #61: replacing a session
// for an info hash that already has one (e.g. via a racing AddTorrentFile check-then-insert)
// must close the displaced session's storage instead of leaking its file handles.
func TestAddSessionClosesReplacedSession(t *testing.T) {
	mgr := NewTorrentManager()
	defer mgr.Close()

	oldStorage := &closeTrackingStorage{}
	oldSess := &Session{
		Torrent: &torrent.Torrent{InfoHash: [20]byte{9, 9, 9}},
		Storage: oldStorage,
	}

	newStorage := &closeTrackingStorage{}
	newSess := &Session{
		Torrent: &torrent.Torrent{InfoHash: [20]byte{9, 9, 9}},
		Storage: newStorage,
	}

	mgr.AddSession("090909", oldSess)
	mgr.AddSession("090909", newSess)

	if !oldStorage.closed.Load() {
		t.Error("expected replaced session's storage to be closed, but it was left open (FD leak)")
	}
	if newStorage.closed.Load() {
		t.Error("did not expect the surviving session's storage to be closed")
	}
	if got := mgr.GetSession("090909"); got != newSess {
		t.Errorf("expected surviving session to be the most recently added one, got %v", got)
	}
}

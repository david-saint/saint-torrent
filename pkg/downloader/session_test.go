package downloader

import (
	"crypto/sha1"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func TestSessionInitAndBlockCalculations(t *testing.T) {
	// Create temporary directory for storage
	tempDir, err := os.MkdirTemp("", "sainttorrent_downloader_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tor := &torrent.Torrent{
		Name:        "test.txt",
		PieceLength: 65536, // 64KB pieces
		PieceHashes: [][20]byte{
			sha1.Sum([]byte("piece0")),
			sha1.Sum([]byte("piece1")),
		},
		Files: []torrent.File{
			{Length: 100000, Path: []string{"test.txt"}},
		},
	}

	files := make([]storage.FileInfo, len(tor.Files))
	for i, f := range tor.Files {
		files[i] = storage.FileInfo{
			Path:   filepath.Join(f.Path...),
			Length: f.Length,
		}
	}

	st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to initialize storage: %v", err)
	}

	peerID := [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	sess, err := NewSession(tor, st, peerID, 6881, tempDir)
	if err != nil {
		t.Fatalf("failed to create downloader session: %v", err)
	}

	if sess.TotalPieces() != 2 {
		t.Errorf("expected 2 pieces, got %d", sess.TotalPieces())
	}

	// Verify piece lengths:
	// piece 0 start: 0, end: 65536 (len 65536)
	// piece 1 start: 65536, end: 100000 (len 34464)
	if sess.Storage.PieceLength(0) != 65536 {
		t.Errorf("expected piece 0 length 65536, got %d", sess.Storage.PieceLength(0))
	}
	if sess.Storage.PieceLength(1) != 34464 {
		t.Errorf("expected piece 1 length 34464, got %d", sess.Storage.PieceLength(1))
	}

	// Verify block calculations
	// 65536 bytes piece with 16384 block size should have 4 blocks
	p0Blocks := sess.blocksInPiece(0)
	if p0Blocks != 4 {
		t.Errorf("expected 4 blocks in piece 0, got %d", p0Blocks)
	}

	// 34464 bytes piece with 16384 block size should have 3 blocks (16384, 16384, 1696)
	p1Blocks := sess.blocksInPiece(1)
	if p1Blocks != 3 {
		t.Errorf("expected 3 blocks in piece 1, got %d", p1Blocks)
	}
}

func TestSessionPieceSelection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sainttorrent_downloader_test_priority")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// A multi-file torrent where:
	// File 0: 32KB (belongs to piece 0)
	// File 1: 32KB (belongs to piece 1)
	tor := &torrent.Torrent{
		Name:        "multifile",
		PieceLength: 32768, // 32KB
		PieceHashes: [][20]byte{
			sha1.Sum([]byte("p0")),
			sha1.Sum([]byte("p1")),
		},
		Files: []torrent.File{
			{Length: 32768, Path: []string{"file0.txt"}},
			{Length: 32768, Path: []string{"file1.txt"}},
		},
	}

	files := make([]storage.FileInfo, len(tor.Files))
	for i, f := range tor.Files {
		files[i] = storage.FileInfo{
			Path:   filepath.Join(f.Path...),
			Length: f.Length,
		}
	}

	st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to initialize storage: %v", err)
	}

	peerID := [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	sess, err := NewSession(tor, st, peerID, 6881, tempDir)
	if err != nil {
		t.Fatalf("failed to create downloader session: %v", err)
	}

	// Initially, both files are PriorityNormal (2), so they are wanted
	if !sess.isPieceWanted(0) || !sess.isPieceWanted(1) {
		t.Error("expected both pieces to be wanted initially")
	}

	if sess.piecePriority(0) != PriorityNormal || sess.piecePriority(1) != PriorityNormal {
		t.Error("expected both pieces to have Normal priority initially")
	}

	// Skip File 0 (so piece 0 is no longer wanted)
	sess.SetFilePriority(0, PrioritySkip)
	if sess.isPieceWanted(0) {
		t.Error("expected piece 0 to not be wanted after file 0 skip")
	}
	if !sess.isPieceWanted(1) {
		t.Error("expected piece 1 to be wanted")
	}

	// Raise File 1 priority to High (3)
	sess.SetFilePriority(1, PriorityHigh)
	if sess.piecePriority(1) != PriorityHigh {
		t.Errorf("expected piece 1 priority to be High, got %v", sess.piecePriority(1))
	}
}

func TestSessionFileSelectionCompletionUsesWantedBytes(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sainttorrent_downloader_test_wanted_progress")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tor := &torrent.Torrent{
		Name:        "multifile",
		PieceLength: 32768,
		PieceHashes: [][20]byte{
			sha1.Sum([]byte("p0")),
			sha1.Sum([]byte("p1")),
		},
		Files: []torrent.File{
			{Length: 32768, Path: []string{"file0.txt"}},
			{Length: 32768, Path: []string{"file1.txt"}},
		},
	}

	files := []storage.FileInfo{
		{Path: filepath.Join(tor.Files[0].Path...), Length: tor.Files[0].Length},
		{Path: filepath.Join(tor.Files[1].Path...), Length: tor.Files[1].Length},
	}
	st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to initialize storage: %v", err)
	}

	peerID := [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	sess, err := NewSession(tor, st, peerID, 6881, tempDir)
	if err != nil {
		t.Fatalf("failed to create downloader session: %v", err)
	}

	sess.SetFilePriority(0, PrioritySkip)
	sess.mu.Lock()
	sess.PieceStates[1] = PieceCompleted
	sess.mu.Unlock()

	if pct := sess.PercentComplete(); pct != 100.0 {
		t.Fatalf("expected wanted progress to be 100%%, got %.2f", pct)
	}
	if status := sess.Status(); status != "Seeding" {
		t.Fatalf("expected wanted content status to be Seeding, got %q", status)
	}
}

func TestSessionProgressTracking(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sainttorrent_downloader_test3")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	data0 := []byte("hello world first piece")
	data1 := []byte("second piece data")
	h0 := sha1.Sum(data0)
	h1 := sha1.Sum(data1)

	tor := &torrent.Torrent{
		Name:        "test3.txt",
		PieceLength: int64(len(data0)),
		PieceHashes: [][20]byte{h0, h1},
		Files: []torrent.File{
			{Length: int64(len(data0) + len(data1)), Path: []string{"test3.txt"}},
		},
	}

	files := make([]storage.FileInfo, len(tor.Files))
	for i, f := range tor.Files {
		files[i] = storage.FileInfo{
			Path:   filepath.Join(f.Path...),
			Length: f.Length,
		}
	}

	st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to initialize storage: %v", err)
	}

	peerID := [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	sess, err := NewSession(tor, st, peerID, 6881, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	// Manually write block data
	err = sess.Storage.WriteBlock(0, 0, data0)
	if err != nil {
		t.Fatalf("failed to write block: %v", err)
	}

	// Verify piece 0
	ok, err := sess.Storage.VerifyPiece(0, h0)
	if err != nil {
		t.Fatalf("failed to verify piece: %v", err)
	}
	if !ok {
		t.Fatal("piece verification failed")
	}

	// Set piece 0 as completed in session
	sess.markPieceCompleted(0)

	pct := sess.PercentComplete()
	expectedPct := float64(len(data0)) / float64(len(data0)+len(data1)) * 100.0
	if pct != expectedPct {
		t.Errorf("expected %f%% completed, got %f%%", expectedPct, pct)
	}

	// Calculate speed
	sess.Downloaded += int64(len(data0))
	time.Sleep(10 * time.Millisecond) // brief sleep to ensure elapsed time > 0
	speed := sess.DownloadSpeed()
	if speed <= 0 {
		t.Logf("download speed returned: %f bytes/sec", speed)
	}
}

func TestSessionFastResume(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sainttorrent_session_fastresume")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	data := []byte("piece data for fast resume test")
	h := sha1.Sum(data)

	tor := &torrent.Torrent{
		Name:        "fast_resume_test.txt",
		PieceLength: int64(len(data)),
		PieceHashes: [][20]byte{h},
		Files: []torrent.File{
			{Length: int64(len(data)), Path: []string{"fast_resume_test.txt"}},
		},
	}

	files := []storage.FileInfo{
		{Path: filepath.Join(tor.Files[0].Path...), Length: tor.Files[0].Length},
	}
	st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	peerID := [20]byte{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}
	sess, err := NewSession(tor, st, peerID, 6881, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	// Set piece 0 as completed and verify it saves state
	err = sess.Storage.WriteBlock(0, 0, data)
	if err != nil {
		t.Fatalf("failed to write block: %v", err)
	}
	sess.markPieceCompleted(0)

	// Create a new session on the same storage and verify fast resume loads it
	sess2, err := NewSession(tor, st, peerID, 6882, tempDir)
	if err != nil {
		t.Fatalf("failed to create second session: %v", err)
	}

	sess2.mu.RLock()
	state := sess2.PieceStates[0]
	sess2.mu.RUnlock()

	if state != PieceCompleted {
		t.Errorf("expected piece 0 to be completed via fast resume, got %v", state)
	}
}

func TestSessionFastResumeCorrupt(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sainttorrent_session_fastresume_corrupt")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	data := []byte("piece data for fast resume test")
	h := sha1.Sum(data)

	tor := &torrent.Torrent{
		Name:        "fast_resume_test.txt",
		PieceLength: int64(len(data)),
		PieceHashes: [][20]byte{h},
		Files: []torrent.File{
			{Length: int64(len(data)), Path: []string{"fast_resume_test.txt"}},
		},
	}

	files := []storage.FileInfo{
		{Path: filepath.Join(tor.Files[0].Path...), Length: tor.Files[0].Length},
	}
	st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	peerID := [20]byte{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}
	sess, err := NewSession(tor, st, peerID, 6881, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	// Set piece 0 as completed and verify it saves state
	err = sess.Storage.WriteBlock(0, 0, data)
	if err != nil {
		t.Fatalf("failed to write block: %v", err)
	}
	sess.markPieceCompleted(0)

	// Close storage to release files
	st.Close()

	// Corrupt the file on disk by overwriting it with different data
	filePath := filepath.Join(tempDir, "fast_resume_test.txt")
	err = os.WriteFile(filePath, []byte("corrupted data of the same length"), 0644)
	if err != nil {
		t.Fatalf("failed to corrupt file: %v", err)
	}

	// Reopen storage
	st2, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to reopen storage: %v", err)
	}
	defer st2.Close()

	// Create a new session on the same storage and verify it rejects the corrupt piece
	sess2, err := NewSession(tor, st2, peerID, 6882, tempDir)
	if err != nil {
		t.Fatalf("failed to create second session: %v", err)
	}

	sess2.mu.RLock()
	state := sess2.PieceStates[0]
	sess2.mu.RUnlock()

	if state == PieceCompleted {
		t.Errorf("expected piece 0 to NOT be completed due to corruption, but it was marked as completed")
	}
}

func TestSessionPauseResume(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sainttorrent_session_pauseresume")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tor := &torrent.Torrent{
		Name:        "pause_resume_test.txt",
		PieceLength: 1024,
		PieceHashes: [][20]byte{sha1.Sum([]byte("data"))},
		Files: []torrent.File{
			{Length: 1024, Path: []string{"pause_resume_test.txt"}},
		},
	}

	files := []storage.FileInfo{
		{Path: filepath.Join(tor.Files[0].Path...), Length: tor.Files[0].Length},
	}
	st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	peerID := [20]byte{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4}
	sess, err := NewSession(tor, st, peerID, 0, tempDir) // random port
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	if sess.IsPaused() {
		t.Error("expected session to not be paused initially")
	}

	if status := sess.Status(); status != "Downloading" {
		t.Errorf("expected status 'Downloading', got %q", status)
	}

	sess.Pause()
	if !sess.IsPaused() {
		t.Error("expected session to be paused")
	}
	if status := sess.Status(); status != "Paused" {
		t.Errorf("expected status 'Paused', got %q", status)
	}

	sess.Resume()
	if sess.IsPaused() {
		t.Error("expected session to be resumed")
	}
	if status := sess.Status(); status != "Downloading" {
		t.Errorf("expected status 'Downloading', got %q", status)
	}
}

func TestSessionStartIsIdempotent(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sainttorrent_session_start_idempotent")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tor := &torrent.Torrent{
		Name:        "start_once.txt",
		PieceLength: 1024,
		PieceHashes: [][20]byte{sha1.Sum([]byte("data"))},
		Files: []torrent.File{
			{Length: 1024, Path: []string{"start_once.txt"}},
		},
	}
	files := []storage.FileInfo{
		{Path: filepath.Join(tor.Files[0].Path...), Length: tor.Files[0].Length},
	}
	st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	peerID := [20]byte{7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}
	sess, err := NewSession(tor, st, peerID, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.Start()
	firstPort := sess.Port
	if firstPort == 0 {
		t.Fatal("expected dynamic listener port to be assigned")
	}
	sess.Start()
	if sess.Port != firstPort {
		t.Fatalf("expected second Start to preserve listener port %d, got %d", firstPort, sess.Port)
	}
	sess.Close()
}

func TestSessionUploadSpeedIsCalculated(t *testing.T) {
	tor := &torrent.Torrent{Name: "speed", PieceLength: 1, PieceHashes: [][20]byte{sha1.Sum([]byte("x"))}}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, "")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.wg.Add(1)
	go sess.speedMonitorLoop()

	sess.mu.Lock()
	sess.Peers["127.0.0.1:1"] = &PeerState{Active: true, Uploaded: 2048}
	sess.mu.Unlock()

	time.Sleep(1200 * time.Millisecond)
	sess.mu.RLock()
	got := sess.Peers["127.0.0.1:1"].UploadSpeed
	sess.mu.RUnlock()
	sess.Close()

	if got <= 0 {
		t.Fatalf("expected upload speed to be calculated, got %.2f", got)
	}
}

func TestAnnounceMagnetUsesNonZeroLeft(t *testing.T) {
	oldTimeout := trackerAnnounceTimeout
	trackerAnnounceTimeout = 2 * time.Second
	defer func() { trackerAnnounceTimeout = oldTimeout }()

	leftCh := make(chan string, 1)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 2048)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		requestLine := string(buf[:n])
		parts := strings.Split(requestLine, " ")
		if len(parts) < 2 {
			return
		}
		u, err := url.Parse(parts[1])
		if err != nil {
			return
		}
		leftCh <- u.Query().Get("left")
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\nd8:intervali5ee"))
	}()

	tor := &torrent.Torrent{
		Name:     "magnet",
		InfoHash: sha1.Sum([]byte("magnet")),
		Trackers: []string{"http://" + ln.Addr().String() + "/announce"},
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, tempDirOrDot(t))
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.announceAndConnect()

	select {
	case got := <-leftCh:
		if got != "1" {
			t.Fatalf("expected magnet announce left=1 before metadata, got %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for announce")
	}
}

func TestAnnounceKeepsStartedEventAfterFailure(t *testing.T) {
	oldTimeout := trackerAnnounceTimeout
	trackerAnnounceTimeout = 50 * time.Millisecond
	defer func() { trackerAnnounceTimeout = oldTimeout }()

	tor := &torrent.Torrent{
		Name:        "announce-fail",
		InfoHash:    sha1.Sum([]byte("announce-fail")),
		PieceLength: 1,
		PieceHashes: [][20]byte{sha1.Sum([]byte("x"))},
		Files:       []torrent.File{{Length: 1, Path: []string{"x"}}},
		Trackers:    []string{"http://127.0.0.1:1/announce"},
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, tempDirOrDot(t))
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.announceAndConnect()
	sess.mu.RLock()
	event := sess.nextTrackerEvent
	sess.mu.RUnlock()
	if event != "started" {
		t.Fatalf("expected failed announce to preserve started event, got %q", event)
	}
}

func TestPauseClosesHandshakingPeerBeforeActivation(t *testing.T) {
	tor := &torrent.Torrent{
		Name:        "pause-handshake",
		InfoHash:    sha1.Sum([]byte("pause-handshake")),
		PieceLength: 1,
		PieceHashes: [][20]byte{sha1.Sum([]byte("x"))},
		Files:       []torrent.File{{Length: 1, Path: []string{"x"}}},
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, tempDirOrDot(t))
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	client := peer.NewClient(clientConn, tor.InfoHash, sess.PeerID)

	sess.Pause()
	sess.runPeerMessageLoop(client, clientConn, "127.0.0.1:4444", "127.0.0.1", 4444, [8]byte{})

	sess.mu.RLock()
	_, active := sess.activePeers["127.0.0.1:4444"]
	sess.mu.RUnlock()
	if active {
		t.Fatal("paused session activated a handshaking peer")
	}
}

func tempDirOrDot(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sainttorrent_magnet_announce")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestSessionChokingStrategy(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sainttorrent_session_choking")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tor := &torrent.Torrent{
		Name:        "choking_test.txt",
		PieceLength: 1024,
		PieceHashes: [][20]byte{sha1.Sum([]byte("data"))},
		Files: []torrent.File{
			{Length: 1024, Path: []string{"choking_test.txt"}},
		},
	}

	files := []storage.FileInfo{
		{Path: filepath.Join(tor.Files[0].Path...), Length: tor.Files[0].Length},
	}
	st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	peerID := [20]byte{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4}
	sess, err := NewSession(tor, st, peerID, 6881, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	// Create 5 active, interested peers. We will give them different download speeds.
	// Since we only unchoke top 4, the 5th one should be choked, unless optimistic unchoke picks it.
	pConns := make([]net.Conn, 5)
	for i := 0; i < 5; i++ {
		p1, p2 := net.Pipe()
		pConns[i] = p2
		defer p1.Close()
		defer p2.Close()

		addr := fmt.Sprintf("127.0.0.1:1000%d", i)
		client := peer.NewClient(p1, tor.InfoHash, peerID)
		sess.activePeers[addr] = client
		sess.Peers[addr] = &PeerState{
			IP:            "127.0.0.1",
			Port:          uint16(10000 + i),
			Active:        true,
			Interested:    true,
			DownloadSpeed: float64(10 * (i + 1)), // 10, 20, 30, 40, 50 bytes/sec
			AmChoking:     true,
		}
	}

	// Recalculate choking: should unchoke the top 4 (i.e. ports 10004, 10003, 10002, 10001)
	// and keep port 10000 choked.
	var optimisticPeer string
	sess.recalculateChoking(&optimisticPeer)

	sess.mu.RLock()
	if sess.Peers["127.0.0.1:10004"].AmChoking {
		t.Error("expected peer 4 (highest speed) to be unchoked")
	}
	if sess.Peers["127.0.0.1:10003"].AmChoking {
		t.Error("expected peer 3 to be unchoked")
	}
	if sess.Peers["127.0.0.1:10002"].AmChoking {
		t.Error("expected peer 2 to be unchoked")
	}
	if sess.Peers["127.0.0.1:10001"].AmChoking {
		t.Error("expected peer 1 to be unchoked")
	}
	if !sess.Peers["127.0.0.1:10000"].AmChoking {
		t.Error("expected peer 0 (lowest speed) to be choked")
	}
	sess.mu.RUnlock()

	// Optimistic unchoke: since port 10000 is the only choked interested peer,
	// recalculateOptimistic must pick it.
	sess.recalculateOptimistic(&optimisticPeer)
	if optimisticPeer != "127.0.0.1:10000" {
		t.Errorf("expected optimistic peer to be port 10000, got %q", optimisticPeer)
	}

	sess.mu.RLock()
	if sess.Peers["127.0.0.1:10000"].AmChoking {
		t.Error("expected optimistic peer to be unchoked")
	}
	sess.mu.RUnlock()
}

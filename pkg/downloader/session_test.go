package downloader

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

type delayedCloseConn struct {
	net.Conn
}

func (c *delayedCloseConn) Close() error {
	return nil
}

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
	sess.Downloaded.Add(int64(len(data0)))
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

	// Fast-resume verification now runs in the background; trigger and await it.
	sess2.maybeStartVerification()
	sess2.WaitVerified()

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

	// Background verification must catch the corruption and reset the piece for
	// re-download — the "never trust resume data blindly" invariant.
	sess2.maybeStartVerification()
	sess2.WaitVerified()

	sess2.mu.RLock()
	state := sess2.PieceStates[0]
	sess2.mu.RUnlock()

	if state != PieceEmpty {
		t.Errorf("expected corrupt piece 0 to be reset to PieceEmpty after verification, got %v", state)
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
	sess.Uploaded.Store(2048)
	sess.mu.Unlock()

	time.Sleep(1200 * time.Millisecond)
	sess.mu.RLock()
	peerSpeed := sess.Peers["127.0.0.1:1"].UploadSpeed
	sessionSpeed := sess.currentUploadSpeed
	sess.mu.RUnlock()
	sess.Close()

	if peerSpeed <= 0 {
		t.Fatalf("expected peer upload speed to be calculated, got %.2f", peerSpeed)
	}
	if sessionSpeed <= 0 {
		t.Fatalf("expected session upload speed to be calculated, got %.2f", sessionSpeed)
	}
}

func TestSessionServesRequestedPieceAndCountsUpload(t *testing.T) {
	data := []byte("seed block")
	tempDir := t.TempDir()
	tor := &torrent.Torrent{
		Name:        "seed.bin",
		PieceLength: int64(len(data)),
		PieceHashes: [][20]byte{sha1.Sum(data)},
		Files:       []torrent.File{{Length: int64(len(data)), Path: []string{"seed.bin"}}},
	}
	st, err := storage.NewStorage(tempDir, []storage.FileInfo{{
		Path:   "seed.bin",
		Length: int64(len(data)),
	}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	if err := st.WriteBlock(0, 0, data); err != nil {
		t.Fatalf("failed to seed storage: %v", err)
	}

	sess, err := NewSession(tor, st, [20]byte{}, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	defer sess.Close()
	sess.mu.Lock()
	sess.PieceStates[0] = PieceCompleted
	sess.mu.Unlock()

	clientConn, remoteConn := net.Pipe()
	defer remoteConn.Close()
	client := peer.NewClient(clientConn, tor.InfoHash, sess.PeerID)
	done := make(chan struct{})
	go func() {
		sess.runPeerMessageLoop(client, clientConn, "127.0.0.1:6000", "127.0.0.1", 6000, [8]byte{})
		close(done)
	}()

	_ = remoteConn.SetDeadline(time.Now().Add(2 * time.Second))
	bitfield, err := peer.ParseMessage(remoteConn)
	if err != nil {
		t.Fatalf("failed to read bitfield: %v", err)
	}
	if bitfield == nil || bitfield.ID != peer.MsgBitfield {
		t.Fatalf("expected bitfield, got %#v", bitfield)
	}
	notInterested, err := peer.ParseMessage(remoteConn)
	if err != nil {
		t.Fatalf("failed to read not-interested message: %v", err)
	}
	if notInterested == nil || notInterested.ID != peer.MsgNotInterested {
		t.Fatalf("expected not-interested message, got %#v", notInterested)
	}

	if _, err := remoteConn.Write((&peer.Message{ID: peer.MsgInterested}).Serialize()); err != nil {
		t.Fatalf("failed to send interested message: %v", err)
	}
	unchoke, err := peer.ParseMessage(remoteConn)
	if err != nil {
		t.Fatalf("failed to read unchoke: %v", err)
	}
	if unchoke == nil || unchoke.ID != peer.MsgUnchoke {
		t.Fatalf("expected unchoke, got %#v", unchoke)
	}
	stats := sess.GetUploadPeerStats()
	if stats.Connected != 1 || stats.Interested != 1 || stats.Unchoked != 1 {
		t.Fatalf("unexpected upload peer stats: %+v", stats)
	}

	requestPayload := make([]byte, 12)
	binary.BigEndian.PutUint32(requestPayload[8:12], uint32(len(data)))
	if _, err := remoteConn.Write((&peer.Message{ID: peer.MsgRequest, Payload: requestPayload}).Serialize()); err != nil {
		t.Fatalf("failed to request piece: %v", err)
	}
	pieceMsg, err := peer.ParseMessage(remoteConn)
	if err != nil {
		t.Fatalf("failed to read piece: %v", err)
	}
	if pieceMsg == nil || pieceMsg.ID != peer.MsgPiece || !bytes.Equal(pieceMsg.Payload[8:], data) {
		t.Fatalf("unexpected piece response: %#v", pieceMsg)
	}
	deadline := time.Now().Add(time.Second)
	for sess.UploadedBytes() != int64(len(data)) {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d uploaded bytes, got %d", len(data), sess.UploadedBytes())
		}
		time.Sleep(time.Millisecond)
	}

	_ = remoteConn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("peer loop did not exit")
	}
}

func TestAnnounceMagnetUsesNonZeroLeft(t *testing.T) {
	oldTimeout := trackerAnnounceTimeout
	trackerAnnounceTimeout = 2 * time.Second
	defer func() { trackerAnnounceTimeout = oldTimeout }()

	leftCh := make(chan url.Values, 1)
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
		leftCh <- u.Query()
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
		if got.Get("left") != "1" {
			t.Fatalf("expected magnet announce left=1 before metadata, got %q", got.Get("left"))
		}
		if got.Get("numwant") != strconv.Itoa(trackerDefaultNumWant) {
			t.Fatalf("expected announce numwant=%d, got %q", trackerDefaultNumWant, got.Get("numwant"))
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
	events := sess.trackerEvents
	sess.mu.RUnlock()
	if len(events) != 1 || events[0] != "started" {
		t.Fatalf("expected failed announce to preserve started event, got %v", events)
	}
}

func TestAnnounceQueriesAllTrackers(t *testing.T) {
	oldTimeout := trackerAnnounceTimeout
	trackerAnnounceTimeout = 2 * time.Second
	defer func() { trackerAnnounceTimeout = oldTimeout }()

	tracker1Seen := make(chan struct{}, 1)
	tracker1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case tracker1Seen <- struct{}{}:
		default:
		}
		_, _ = w.Write([]byte("d8:intervali60ee"))
	}))
	defer tracker1.Close()

	tracker2Seen := make(chan struct{}, 1)
	tracker2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case tracker2Seen <- struct{}{}:
		default:
		}
		_, _ = w.Write([]byte("d8:intervali60ee"))
	}))
	defer tracker2.Close()

	tor := &torrent.Torrent{
		Name:     "multi-tracker",
		InfoHash: sha1.Sum([]byte("multi-tracker")),
		Trackers: []string{tracker1.URL, tracker2.URL},
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, "")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.announceAndConnect()

	select {
	case <-tracker1Seen:
	case <-time.After(3 * time.Second):
		t.Fatal("first tracker was not announced to")
	}
	select {
	case <-tracker2Seen:
	case <-time.After(3 * time.Second):
		t.Fatal("second tracker was not announced to")
	}
}

func TestAnnounceSucceedsWhenOneTrackerFails(t *testing.T) {
	oldTimeout := trackerAnnounceTimeout
	trackerAnnounceTimeout = 2 * time.Second
	defer func() { trackerAnnounceTimeout = oldTimeout }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	failedTrackerURL := "http://" + ln.Addr().String() + "/announce"
	ln.Close()

	workingTracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("d8:intervali60ee"))
	}))
	defer workingTracker.Close()

	tor := &torrent.Torrent{
		Name:     "partial-tracker-failure",
		InfoHash: sha1.Sum([]byte("partial-tracker-failure")),
		Trackers: []string{failedTrackerURL, workingTracker.URL},
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, "")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.announceAndConnect()

	sess.mu.RLock()
	lastTrackerErr := sess.lastTrackerErr
	events := append([]string(nil), sess.trackerEvents...)
	sess.mu.RUnlock()
	if lastTrackerErr != nil {
		t.Fatalf("expected successful tracker announce to clear lastTrackerErr, got %v", lastTrackerErr)
	}
	if len(events) != 0 {
		t.Fatalf("expected successful tracker announce to consume started event, got %v", events)
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

func TestQuickResumeMakesTearingDownPeerImmediatelyRetryable(t *testing.T) {
	tor := &torrent.Torrent{
		Name:     "quick-resume",
		InfoHash: sha1.Sum([]byte("quick-resume")),
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, tempDirOrDot(t))
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	clientConn, serverConn := net.Pipe()
	wrappedConn := &delayedCloseConn{Conn: clientConn}
	client := peer.NewClient(wrappedConn, tor.InfoHash, sess.PeerID)
	done := make(chan struct{})
	go func() {
		sess.runPeerMessageLoop(client, wrappedConn, "127.0.0.1:4444", "127.0.0.1", 4444, [8]byte{})
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(sess.GetActivePeers()) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(sess.GetActivePeers()) != 1 {
		t.Fatal("peer did not become active")
	}

	// Pause closes the wrapped connection, but delayedCloseConn keeps the read loop
	// alive until after Resume returns. This reproduces the teardown ordering that
	// previously imposed a 60-second redial cooldown.
	sess.Pause()
	sess.Resume()
	_ = serverConn.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("peer loop did not exit")
	}
	_ = clientConn.Close()

	sess.mu.RLock()
	lastAttempt := sess.Peers["127.0.0.1:4444"].LastAttempt
	sess.mu.RUnlock()
	if !lastAttempt.IsZero() {
		t.Fatalf("expected peer to be immediately retryable after quick resume, got %v", lastAttempt)
	}
}

func TestHashFailureDisconnectsPeerWithoutErrorStatus(t *testing.T) {
	tempDir := t.TempDir()
	goodData := []byte("good")
	tor := &torrent.Torrent{
		Name:        "hash-retry",
		InfoHash:    sha1.Sum([]byte("hash-retry")),
		PieceLength: int64(len(goodData)),
		PieceHashes: [][20]byte{sha1.Sum(goodData)},
		Files:       []torrent.File{{Length: int64(len(goodData)), Path: []string{"hash-retry.bin"}}},
	}
	st, err := storage.NewStorage(tempDir, []storage.FileInfo{{
		Path:   "hash-retry.bin",
		Length: int64(len(goodData)),
	}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	client := peer.NewClient(clientConn, tor.InfoHash, sess.PeerID)
	done := make(chan struct{})
	go func() {
		sess.runPeerMessageLoop(client, clientConn, "127.0.0.1:5555", "127.0.0.1", 5555, [8]byte{})
		close(done)
	}()

	_ = serverConn.SetDeadline(time.Now().Add(time.Second))
	msg, err := peer.ParseMessage(serverConn)
	if err != nil {
		t.Fatalf("failed to read interested message: %v", err)
	}
	if msg == nil || msg.ID != peer.MsgInterested {
		t.Fatalf("expected interested message, got %#v", msg)
	}
	if _, err := serverConn.Write((&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x80}}).Serialize()); err != nil {
		t.Fatalf("failed to send bitfield: %v", err)
	}
	if _, err := serverConn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize()); err != nil {
		t.Fatalf("failed to send unchoke: %v", err)
	}

	request, err := peer.ParseMessage(serverConn)
	if err != nil {
		t.Fatalf("failed to read piece request: %v", err)
	}
	if request == nil || request.ID != peer.MsgRequest || len(request.Payload) != 12 {
		t.Fatalf("expected piece request, got %#v", request)
	}
	index := binary.BigEndian.Uint32(request.Payload[0:4])
	begin := binary.BigEndian.Uint32(request.Payload[4:8])
	payload := make([]byte, 8+len(goodData))
	binary.BigEndian.PutUint32(payload[0:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	copy(payload[8:], []byte("bad!"))
	if _, err := serverConn.Write((&peer.Message{ID: peer.MsgPiece, Payload: payload}).Serialize()); err != nil {
		t.Fatalf("failed to send corrupt piece: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("session retried the corrupt piece on the same peer")
	}

	if status := sess.Status(); status != "Downloading" {
		t.Fatalf("expected recoverable hash failure to keep Downloading status, got %q", status)
	}
	if err := sess.LastError(); err == nil || !strings.Contains(err.Error(), "failed hash verification") {
		t.Fatalf("expected hash failure to remain visible as the last issue, got %v", err)
	}
	if state := sess.GetPieceStates()[0]; state != PieceEmpty {
		t.Fatalf("expected corrupt piece to return to the pool, got state %v", state)
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

func TestSessionStatusStopped(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sainttorrent_session_stopped_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tor := &torrent.Torrent{
		Name:        "stopped_test.txt",
		PieceLength: 1024,
		PieceHashes: [][20]byte{sha1.Sum([]byte("data"))},
		Files: []torrent.File{
			{Length: 1024, Path: []string{"stopped_test.txt"}},
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
	sess, err := NewSession(tor, st, peerID, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	if sess.IsCompleted() {
		t.Error("expected session not to be completed initially")
	}

	// Complete the session
	sess.mu.Lock()
	sess.PieceStates[0] = PieceCompleted
	sess.mu.Unlock()

	if !sess.IsCompleted() {
		t.Error("expected session to be completed")
	}

	if status := sess.Status(); status != "Seeding" {
		t.Errorf("expected status 'Seeding', got %q", status)
	}

	sess.Pause()
	if !sess.IsPaused() {
		t.Error("expected session to be paused/stopped")
	}

	if status := sess.Status(); status != "Stopped" {
		t.Errorf("expected status 'Stopped' for completed paused session, got %q", status)
	}

	sess.Resume()
	if sess.IsPaused() {
		t.Error("expected session to be resumed/started")
	}

	if status := sess.Status(); status != "Seeding" {
		t.Errorf("expected status 'Seeding', got %q", status)
	}
}

func TestSessionPauseResumeAnnounceQueue(t *testing.T) {
	tor := &torrent.Torrent{
		Name:     "announce_test",
		InfoHash: sha1.Sum([]byte("announce_test")),
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, "")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	// Verify initial event is "started"
	sess.mu.RLock()
	events := sess.trackerEvents
	sess.mu.RUnlock()
	if len(events) != 1 || events[0] != "started" {
		t.Errorf("expected initial trackerEvents [started], got %v", events)
	}

	// Manually queue completed event
	sess.mu.Lock()
	sess.queueTrackerEventLocked("completed")
	sess.mu.Unlock()

	// Pause
	sess.Pause()

	// Resume
	sess.Resume()

	sess.mu.RLock()
	events = sess.trackerEvents
	sess.mu.RUnlock()

	// Queue should now contain: started, completed, stopped, started in order
	expected := []string{"started", "completed", "stopped", "started"}
	if len(events) != len(expected) {
		t.Fatalf("expected queue length %d, got %d (events: %v)", len(expected), len(events), events)
	}
	for i, ev := range expected {
		if events[i] != ev {
			t.Errorf("expected event[%d] = %q, got %q", i, ev, events[i])
		}
	}
}

func TestSessionAnnounceFailureRetainsQueue(t *testing.T) {
	// Dynamically find a closed port to prevent flakiness
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close() // Immediately close to guarantee it fails

	tor := &torrent.Torrent{
		Name:     "announce_fail_test",
		InfoHash: sha1.Sum([]byte("announce_fail_test")),
		Trackers: []string{"http://" + addr + "/announce"},
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, "")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	// Queue should have "started" initially. Let's call announceAndConnect.
	// Since the announce fails, the "started" event should remain in trackerEvents.
	sess.announceAndConnect()

	sess.mu.RLock()
	events := sess.trackerEvents
	lastTrackerErr := sess.lastTrackerErr
	sess.mu.RUnlock()

	if len(events) != 1 || events[0] != "started" {
		t.Errorf("expected event 'started' to be retained on failure, got %v", events)
	}
	if lastTrackerErr == nil {
		t.Error("expected lastTrackerErr to be set after failed announce")
	}
	if sess.LastError() == nil {
		t.Error("expected tracker failure to remain visible as the last issue")
	}
	if status := sess.Status(); status != "Metadata" {
		t.Errorf("expected active magnet to remain in Metadata status, got %q", status)
	}
}

func TestTrackerFailureDoesNotOverrideDownloadingStatus(t *testing.T) {
	tor := &torrent.Torrent{
		Name:        "active-download",
		InfoHash:    sha1.Sum([]byte("active-download")),
		PieceLength: 1,
		PieceHashes: [][20]byte{sha1.Sum([]byte("x"))},
		Files:       []torrent.File{{Length: 1, Path: []string{"x"}}},
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, "")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.mu.Lock()
	sess.lastTrackerErr = fmt.Errorf("tracker unavailable")
	sess.mu.Unlock()

	if status := sess.Status(); status != "Downloading" {
		t.Errorf("expected tracker failure not to override Downloading status, got %q", status)
	}
	if sess.LastError() == nil {
		t.Error("expected tracker failure to remain visible as the last issue")
	}
}

func TestBlockingStorageIssueUsesErrorStatus(t *testing.T) {
	tor := &torrent.Torrent{
		Name:        "storage-error",
		InfoHash:    sha1.Sum([]byte("storage-error")),
		PieceLength: 1,
		PieceHashes: [][20]byte{sha1.Sum([]byte("x"))},
		Files:       []torrent.File{{Length: 1, Path: []string{"x"}}},
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, "")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	storageErr := fmt.Errorf("write failed")
	sess.mu.Lock()
	sess.lastErr = storageErr
	sess.statusErr = storageErr
	sess.mu.Unlock()

	if status := sess.Status(); status != "Error" {
		t.Fatalf("expected blocking storage issue to use Error status, got %q", status)
	}
	if err := sess.LastError(); err != storageErr {
		t.Fatalf("expected storage issue to be reported, got %v", err)
	}
}

func TestSessionAnnounceNoTrackersQueue(t *testing.T) {
	// No trackers configured
	tor := &torrent.Torrent{
		Name:     "no_trackers_test",
		InfoHash: sha1.Sum([]byte("no_trackers_test")),
		Trackers: []string{},
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, "")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.mu.Lock()
	sess.queueTrackerEventLocked("completed")
	sess.queueTrackerEventLocked("stopped")
	sess.mu.Unlock()

	sess.announceAndConnect()

	sess.mu.RLock()
	events := sess.trackerEvents
	stoppedAnn := sess.stoppedAnnounced
	sess.mu.RUnlock()

	if len(events) != 0 {
		t.Errorf("expected queue to be fully consumed for no-tracker session, got %v", events)
	}
	if !stoppedAnn {
		t.Error("expected stoppedAnnounced to be true after consuming stopped event in no-tracker path")
	}
}

func TestSessionStatusStoppedPrecedence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sainttorrent_session_precedence_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tor := &torrent.Torrent{
		Name:        "precedence.txt",
		PieceLength: 1024,
		PieceHashes: [][20]byte{sha1.Sum([]byte("data"))},
		Files: []torrent.File{
			{Length: 1024, Path: []string{"precedence.txt"}},
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
	sess, err := NewSession(tor, st, peerID, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	// 1. Mark completed
	sess.mu.Lock()
	sess.PieceStates[0] = PieceCompleted
	sess.mu.Unlock()

	// 2. Set a blocking storage error
	sess.mu.Lock()
	storageErr := fmt.Errorf("storage unavailable")
	sess.lastErr = storageErr
	sess.statusErr = storageErr
	sess.mu.Unlock()

	// 3. Pause
	sess.Pause()

	// Verify status is "Stopped" rather than "Error" because paused/stopped has priority!
	if status := sess.Status(); status != "Stopped" {
		t.Errorf("expected status 'Stopped' (precedence), got %q", status)
	}
}

func TestSessionStartResumesQueue(t *testing.T) {
	var eventsReceived []string
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		eventsReceived = append(eventsReceived, r.URL.Query().Get("event"))
		mu.Unlock()
		_, _ = w.Write([]byte("d8:intervali60ee"))
	}))
	defer ts.Close()

	tor := &torrent.Torrent{
		Name:     "start_resume_test",
		InfoHash: sha1.Sum([]byte("start_resume_test")),
		Trackers: []string{ts.URL},
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, "")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	defer sess.Close()

	// Start the session (sends initial "started")
	sess.Start()

	// Wait for the "started" event to be received by the test server
	waitForEvent := func(target string, expectedCount int) {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			count := 0
			for _, ev := range eventsReceived {
				if ev == target {
					count++
				}
			}
			mu.Unlock()
			if count >= expectedCount {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		mu.Lock()
		actual := eventsReceived
		mu.Unlock()
		t.Fatalf("timed out waiting for event %q to reach count %d, actual: %v", target, expectedCount, actual)
	}

	waitForEvent("started", 1)

	// Pause the session (queues and sends "stopped")
	sess.Pause()
	waitForEvent("stopped", 1)

	// Start again (resumes, queues and sends "started")
	sess.Start()
	waitForEvent("started", 2)
}

func TestSessionContentPath(t *testing.T) {
	peerID := [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

	t.Run("single-file torrent points at the file", func(t *testing.T) {
		tempDir := t.TempDir()
		tor := &torrent.Torrent{
			Name:        "movie.mp4",
			PieceLength: 65536,
			PieceHashes: [][20]byte{sha1.Sum([]byte("p0"))},
			Files:       []torrent.File{{Length: 1000, Path: []string{"movie.mp4"}}},
		}
		st, err := storage.NewStorage(tempDir, []storage.FileInfo{{Path: "movie.mp4", Length: 1000}}, tor.PieceLength)
		if err != nil {
			t.Fatalf("failed to init storage: %v", err)
		}
		sess, err := NewSession(tor, st, peerID, 0, tempDir)
		if err != nil {
			t.Fatalf("failed to create session: %v", err)
		}
		path, ok := sess.ContentPath()
		if !ok {
			t.Fatal("expected ContentPath to be available")
		}
		if want := filepath.Join(tempDir, "movie.mp4"); path != want {
			t.Errorf("ContentPath = %q, want %q", path, want)
		}
	})

	t.Run("multi-file torrent points at the top-level folder", func(t *testing.T) {
		tempDir := t.TempDir()
		tor := &torrent.Torrent{
			Name:        "MyShow",
			PieceLength: 65536,
			PieceHashes: [][20]byte{sha1.Sum([]byte("p0"))},
			Files: []torrent.File{
				{Length: 500, Path: []string{"MyShow", "ep1.mkv"}},
				{Length: 500, Path: []string{"MyShow", "ep2.mkv"}},
			},
		}
		files := []storage.FileInfo{
			{Path: filepath.Join("MyShow", "ep1.mkv"), Length: 500},
			{Path: filepath.Join("MyShow", "ep2.mkv"), Length: 500},
		}
		st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
		if err != nil {
			t.Fatalf("failed to init storage: %v", err)
		}
		sess, err := NewSession(tor, st, peerID, 0, tempDir)
		if err != nil {
			t.Fatalf("failed to create session: %v", err)
		}
		path, ok := sess.ContentPath()
		if !ok {
			t.Fatal("expected ContentPath to be available")
		}
		if want := filepath.Join(tempDir, "MyShow"); path != want {
			t.Errorf("ContentPath = %q, want %q", path, want)
		}
	})

	t.Run("metadata-mode magnet has no content path yet", func(t *testing.T) {
		tempDir := t.TempDir()
		tor := &torrent.Torrent{
			Name:        "magnet-pending",
			PieceLength: 0, // zero piece length puts the session in metadata mode
		}
		sess, err := NewSession(tor, nil, peerID, 0, tempDir)
		if err != nil {
			t.Fatalf("failed to create session: %v", err)
		}
		if path, ok := sess.ContentPath(); ok {
			t.Errorf("expected no ContentPath in metadata mode, got %q", path)
		}
	})
}

//go:build !windows

package downloader

import (
	"context"
	"crypto/sha1"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func TestVerifyExistingPiecesDoesNotBlockManagerOrControls(t *testing.T) {
	tempDir := t.TempDir()
	const fileName = "blocked-piece"

	st, err := storage.NewStorage(tempDir, []storage.FileInfo{
		{Path: fileName, Length: 1},
	}, 1)
	if err != nil {
		t.Fatalf("failed to initialize storage: %v", err)
	}

	filePath := filepath.Join(tempDir, fileName)
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("failed to replace storage file: %v", err)
	}
	if err := syscall.Mkfifo(filePath, 0600); err != nil {
		t.Fatalf("failed to create verification FIFO: %v", err)
	}

	tor := &torrent.Torrent{
		Name:        "verification-lock-test",
		InfoHash:    sha1.Sum([]byte("verification-lock-test")),
		PieceLength: 1,
		PieceHashes: [][20]byte{sha1.Sum([]byte{0})},
		Files:       []torrent.File{{Length: 1, Path: []string{fileName}}},
	}
	sess := &Session{
		Torrent:     tor,
		Storage:     st,
		PieceStates: make([]PieceState, 1),
		resumeCh:    make(chan struct{}, 1),
	}
	mgr := NewTorrentManager()
	mgr.AddSession("verification-lock-test", sess)
	defer mgr.Close()

	sess.loadResumeState()
	verifyDone := make(chan struct{})
	go func() {
		sess.verifyResume(context.Background())
		close(verifyDone)
	}()

	// VerifyPiece blocks opening the FIFO until a writer connects. Session
	// refresh and control calls must remain responsive while disk I/O is pending.
	time.Sleep(50 * time.Millisecond)

	refreshDone := make(chan []*Session, 1)
	go func() {
		refreshDone <- mgr.ListSessions()
	}()

	select {
	case sessions := <-refreshDone:
		if len(sessions) != 1 || sessions[0] != sess {
			t.Errorf("unexpected manager sessions during verification: %v", sessions)
		}
	case <-time.After(250 * time.Millisecond):
		t.Error("manager refresh was blocked by piece verification")
	}

	controlsDone := make(chan bool, 1)
	go func() {
		sess.Pause()
		paused := sess.IsPaused()
		sess.Resume()
		controlsDone <- paused && !sess.IsPaused()
	}()

	select {
	case controlsWorked := <-controlsDone:
		if !controlsWorked {
			t.Error("pause/resume did not update session state during verification")
		}
	case <-time.After(250 * time.Millisecond):
		t.Error("pause/resume was blocked by piece verification")
	}

	writer, err := os.OpenFile(filePath, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("failed to unblock verification FIFO: %v", err)
	}
	_ = writer.Close()

	select {
	case <-verifyDone:
	case <-time.After(time.Second):
		t.Fatal("piece verification did not finish after FIFO was released")
	}
}

func TestResumedMetadataSessionVerifiesExistingDownload(t *testing.T) {
	tempDir := t.TempDir()
	const fileName = "existing-download.bin"
	pieceData := []byte("already downloaded")
	pieceHash := sha1.Sum(pieceData)

	if err := os.WriteFile(filepath.Join(tempDir, fileName), pieceData, 0644); err != nil {
		t.Fatalf("failed to create existing download: %v", err)
	}

	infoBytes, err := bencode.Marshal(map[string]interface{}{
		"length":       int64(len(pieceData)),
		"name":         fileName,
		"piece length": int64(len(pieceData)),
		"pieces":       string(pieceHash[:]),
	})
	if err != nil {
		t.Fatalf("failed to encode metadata: %v", err)
	}

	tor := &torrent.Torrent{
		InfoHash: sha1.Sum(infoBytes),
		Name:     "unknown",
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create metadata session: %v", err)
	}

	sess.Pause()
	if !sess.IsPaused() {
		t.Fatal("expected restored metadata session to be paused")
	}
	sess.Resume()
	if sess.IsPaused() {
		t.Fatal("expected metadata session to resume")
	}

	if err := sess.onMetadataDownloaded(infoBytes); err != nil {
		t.Fatalf("metadata completion failed: %v", err)
	}

	// Verification now runs in the background; wait for it before asserting state.
	sess.WaitVerified()

	if sess.IsMetadataMode() {
		t.Fatal("expected metadata mode to finish")
	}
	states := sess.GetPieceStates()
	if len(states) != 1 || states[0] != PieceCompleted {
		t.Fatalf("expected existing piece to be verified, got %v", states)
	}
	if status := sess.Status(); status != "Seeding" {
		t.Fatalf("expected verified existing download to seed, got %q", status)
	}
}

// TestCloseUnblockedByBackgroundVerification ensures Close() returns promptly even when a
// background piece verification is stuck on slow I/O (here, a FIFO open that blocks until
// a writer connects). Regression test for verification being decoupled from s.wg.
func TestCloseUnblockedByBackgroundVerification(t *testing.T) {
	tempDir := t.TempDir()
	const fileName = "blocked-piece"

	st, err := storage.NewStorage(tempDir, []storage.FileInfo{{Path: fileName, Length: 1}}, 1)
	if err != nil {
		t.Fatalf("failed to initialize storage: %v", err)
	}

	filePath := filepath.Join(tempDir, fileName)
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("failed to replace storage file: %v", err)
	}
	if err := syscall.Mkfifo(filePath, 0600); err != nil {
		t.Fatalf("failed to create verification FIFO: %v", err)
	}

	tor := &torrent.Torrent{
		Name:        "close-verify",
		InfoHash:    sha1.Sum([]byte("close-verify")),
		PieceLength: 1,
		PieceHashes: [][20]byte{sha1.Sum([]byte{0})},
		Files:       []torrent.File{{Length: 1, Path: []string{fileName}}},
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.maybeStartVerification()     // VerifyPiece will block opening the FIFO
	time.Sleep(50 * time.Millisecond) // let it reach the blocking open

	closed := make(chan struct{})
	go func() {
		sess.Close()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked on background verification")
	}
	if sess.IsVerifying() {
		t.Fatal("closed session still reports background verification in progress")
	}
	sess.WaitVerified()

	// Unblock the FIFO so the detached verification goroutine can exit, then give it a
	// moment to observe the closed session and return.
	if w, err := os.OpenFile(filePath, os.O_WRONLY, 0600); err == nil {
		_ = w.Close()
	}
	time.Sleep(50 * time.Millisecond)
}

// TestVerificationSlotReclaimedOnClose proves a verification slot held by a wedged
// VerifyPiece is returned to the global gate when the session closes, so a closed torrent
// can't permanently consume verification capacity.
func TestVerificationSlotReclaimedOnClose(t *testing.T) {
	tempDir := t.TempDir()
	const fileName = "blocked-piece"

	st, err := storage.NewStorage(tempDir, []storage.FileInfo{{Path: fileName, Length: 1}}, 1)
	if err != nil {
		t.Fatalf("failed to initialize storage: %v", err)
	}

	filePath := filepath.Join(tempDir, fileName)
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("failed to replace storage file: %v", err)
	}
	if err := syscall.Mkfifo(filePath, 0600); err != nil {
		t.Fatalf("failed to create verification FIFO: %v", err)
	}

	tor := &torrent.Torrent{
		Name:        "slot-reclaim",
		InfoHash:    sha1.Sum([]byte("slot-reclaim")),
		PieceLength: 1,
		PieceHashes: [][20]byte{sha1.Sum([]byte{0})},
		Files:       []torrent.File{{Length: 1, Path: []string{fileName}}},
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	before := len(verifyGate)
	sess.maybeStartVerification()

	// Wait until the verification goroutine has taken a slot and wedged on the FIFO.
	deadline := time.Now().Add(time.Second)
	for len(verifyGate) <= before {
		if time.Now().After(deadline) {
			t.Fatal("verification never acquired a gate slot")
		}
		time.Sleep(5 * time.Millisecond)
	}

	sess.Close()

	// The slot must come back even though VerifyPiece is still wedged on the FIFO.
	deadline = time.Now().Add(time.Second)
	for len(verifyGate) > before {
		if time.Now().After(deadline) {
			t.Fatal("verification slot was not reclaimed after Close")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Cleanup: unblock the FIFO so the detached goroutine can exit.
	if w, err := os.OpenFile(filePath, os.O_WRONLY, 0600); err == nil {
		_ = w.Close()
	}
	time.Sleep(50 * time.Millisecond)
}

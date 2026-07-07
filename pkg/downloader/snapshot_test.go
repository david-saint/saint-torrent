package downloader

import (
	"errors"
	"fmt"
	"testing"
)

// assertSnapshotMatchesGetters checks that Snapshot() agrees with the individual
// locked getters it consolidates, so the TUI's per-tick snapshot renders the same
// values the old per-frame getters produced.
func assertSnapshotMatchesGetters(t *testing.T, s *Session) {
	t.Helper()
	snap := s.Snapshot()

	if got := s.PercentComplete(); snap.Percent != got {
		t.Errorf("snapshot Percent=%v want %v", snap.Percent, got)
	}
	if got := s.Status(); snap.Status != got {
		t.Errorf("snapshot Status=%q want %q", snap.Status, got)
	}
	if got := s.IsCompleted(); snap.Completed != got {
		t.Errorf("snapshot Completed=%v want %v", snap.Completed, got)
	}
	if got := s.IsPaused(); snap.Paused != got {
		t.Errorf("snapshot Paused=%v want %v", snap.Paused, got)
	}
	if got := s.IsMetadataMode(); snap.MetadataMode != got {
		t.Errorf("snapshot MetadataMode=%v want %v", snap.MetadataMode, got)
	}
	if got := s.TotalSize(); snap.TotalSize != got {
		t.Errorf("snapshot TotalSize=%v want %v", snap.TotalSize, got)
	}
	if got := s.CurrentSpeed(); snap.DownloadSpeed != got {
		t.Errorf("snapshot DownloadSpeed=%v want %v", snap.DownloadSpeed, got)
	}
	if got := s.CurrentUploadSpeed(); snap.UploadSpeed != got {
		t.Errorf("snapshot UploadSpeed=%v want %v", snap.UploadSpeed, got)
	}
	if got := s.UploadedBytes(); snap.UploadedBytes != got {
		t.Errorf("snapshot UploadedBytes=%v want %v", snap.UploadedBytes, got)
	}
	if got := s.Name(); snap.Name != got {
		t.Errorf("snapshot Name=%q want %q", snap.Name, got)
	}
	wantErr := s.LastError()
	if (snap.LastError == nil) != (wantErr == nil) ||
		(snap.LastError != nil && wantErr != nil && snap.LastError.Error() != wantErr.Error()) {
		t.Errorf("snapshot LastError=%v want %v", snap.LastError, wantErr)
	}
}

func TestSnapshotMatchesLiveGetters(t *testing.T) {
	sess := newTestSessionBuilder(t, 16, []int64{16, 16}, nil)

	// Fresh: nothing complete.
	assertSnapshotMatchesGetters(t, sess)

	// Simulate rolling speeds and an upload counter the way the speed monitor and
	// peer hot path do, then re-check parity.
	sess.mu.Lock()
	sess.currentSpeed = 128 * 1024
	sess.currentUploadSpeed = 8 * 1024
	sess.mu.Unlock()
	sess.Uploaded.Store(4096)
	assertSnapshotMatchesGetters(t, sess)

	// One piece complete: percent advances, still not seeding.
	sess.markPieceCompleted(0)
	assertSnapshotMatchesGetters(t, sess)
	if snap := sess.Snapshot(); snap.Percent <= 0 || snap.Percent >= 100 {
		t.Fatalf("expected partial completion percent, got %v", snap.Percent)
	}

	// Both pieces complete: seeding / completed.
	sess.markPieceCompleted(1)
	assertSnapshotMatchesGetters(t, sess)
	if snap := sess.Snapshot(); !snap.Completed || snap.Percent != 100 {
		t.Fatalf("expected completed snapshot, got completed=%v percent=%v", snap.Completed, snap.Percent)
	}

	// Paused surfaces in both the getter and the snapshot.
	sess.Pause()
	assertSnapshotMatchesGetters(t, sess)

	// A status error takes precedence in LastError, and Snapshot must mirror it.
	sess.mu.Lock()
	sess.statusErr = errors.New("boom")
	sess.lastErr = fmt.Errorf("secondary")
	sess.mu.Unlock()
	assertSnapshotMatchesGetters(t, sess)
	if snap := sess.Snapshot(); snap.LastError == nil || snap.LastError.Error() != "boom" {
		t.Fatalf("expected statusErr to win in snapshot, got %v", snap.LastError)
	}
}

func TestSnapshotInfoHashMatchesTorrent(t *testing.T) {
	sess := newTestSessionBuilder(t, 16, []int64{16}, nil)
	snap := sess.Snapshot()
	if snap.InfoHash != sess.Torrent.InfoHash {
		t.Fatalf("snapshot InfoHash=%x want %x", snap.InfoHash, sess.Torrent.InfoHash)
	}
	if snap.Name != sess.Torrent.Name {
		t.Fatalf("snapshot Name=%q want %q", snap.Name, sess.Torrent.Name)
	}
}

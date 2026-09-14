package downloader

import (
	"crypto/sha1"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

type countedVerificationStorage struct {
	*storage.FileStorage
	checks []int64
}

func (s *countedVerificationStorage) VerifyPiece(idx int64, hash [20]byte) (bool, error) {
	s.checks = append(s.checks, idx)
	return s.FileStorage.VerifyPiece(idx, hash)
}

func resumeSessionFixture(tb testing.TB, size int) (*torrent.Torrent, *countedVerificationStorage) {
	tb.Helper()
	data := []byte{1, 2, 3, 4}
	tor := &torrent.Torrent{Name: "resume", InfoHash: sha1.Sum([]byte("resume")), PieceLength: 4, Files: []torrent.File{{Path: []string{"a"}, Length: int64(size / 2)}, {Path: []string{"b"}, Length: int64(size / 2)}}}
	for i := 0; i < size/4; i++ {
		tor.PieceHashes = append(tor.PieceHashes, sha1.Sum(data))
	}
	st, err := storage.NewFileStorage(tb.TempDir(), []storage.FileInfo{{Path: "a", Length: int64(size / 2)}, {Path: "b", Length: int64(size / 2)}}, 4)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	for i := range tor.PieceHashes {
		if err = st.WriteBlock(int64(i), 0, data); err != nil {
			tb.Fatal(err)
		}
	}
	return tor, &countedVerificationStorage{FileStorage: st}
}

func TestCompletedRestartSkipsAllHashing(t *testing.T) {
	tor, st := resumeSessionFixture(t, 16)
	sess, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	sess.verifyResume(sess.ctx)
	if len(st.checks) != 4 {
		t.Fatalf("initial scan checked %v", st.checks)
	}
	st.checks = nil
	restarted, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	restarted.verifyResume(restarted.ctx)
	if !restarted.IsCompleted() || restarted.IsVerifying() || len(st.checks) != 0 {
		t.Fatalf("restart hashed %v, snapshot %+v", st.checks, restarted.Snapshot())
	}
}

func TestSelectiveRestartAndForcedRecheck(t *testing.T) {
	tor, st := resumeSessionFixture(t, 16)
	hash := fmt.Sprintf("%x", tor.InfoHash)
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(st.BaseDir(), "b")
	stamp := time.Now().Add(time.Hour)
	if err := os.Chtimes(path, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	sess.Pause()
	if got := sess.Snapshot(); got.Status != "Checking" || !got.Paused || got.Verification.TotalBytes != 8 {
		t.Fatalf("snapshot %+v", got)
	}
	sess.verifyResume(sess.ctx)
	if !reflect.DeepEqual(st.checks, []int64{2, 3}) {
		t.Fatalf("selective checks: %v", st.checks)
	}
	if got := sess.Snapshot(); got.Verification.CheckedBytes != 8 || got.Verification.Active {
		t.Fatalf("progress %+v", got)
	}
	st.checks = nil
	strict, err := newSession(tor, st, [20]byte{}, 0, st.BaseDir(), true)
	if err != nil {
		t.Fatal(err)
	}
	strict.verifyResume(strict.ctx)
	if !reflect.DeepEqual(st.checks, []int64{0, 1, 2, 3}) {
		t.Fatalf("forced checks: %v", st.checks)
	}
}

func TestInterruptedVerificationDoesNotBlessHints(t *testing.T) {
	tor, st := resumeSessionFixture(t, 16)
	hash := fmt.Sprintf("%x", tor.InfoHash)
	if err := st.SaveState(hash, []int{0, 1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	sess.mu.Lock()
	sess.setPieceStateLocked(0, PieceCompleted)
	sess.stateDirty = true
	sess.mu.Unlock()
	sess.flushState()
	next, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range next.GetPieceStates() {
		if state == PieceCompleted {
			t.Fatal("interrupted hints trusted without checking")
		}
	}
}

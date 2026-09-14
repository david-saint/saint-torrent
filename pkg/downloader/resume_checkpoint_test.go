package downloader

import (
	"crypto/sha1"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

type countedVerificationStorage struct {
	*storage.FileStorage
	checks []int64

	mu    sync.Mutex
	saves []bool
}

func (s *countedVerificationStorage) VerifyPiece(idx int64, hash [20]byte) (bool, error) {
	s.checks = append(s.checks, idx)
	return s.FileStorage.VerifyPiece(idx, hash)
}

func (s *countedVerificationStorage) SaveResumeState(hash string, verified, unverified []int, durable bool) error {
	s.mu.Lock()
	s.saves = append(s.saves, durable)
	s.mu.Unlock()
	return s.FileStorage.SaveResumeState(hash, verified, unverified, durable)
}

// savedStates returns the durable flag of every resume write since the last reset.
func (s *countedVerificationStorage) savedStates() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bool(nil), s.saves...)
}

func (s *countedVerificationStorage) resetSaves() {
	s.mu.Lock()
	s.saves = nil
	s.mu.Unlock()
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

// A fresh torrent has no checkpoint to distrust. Marking its pieces unverified
// would remove every one of them from the picker until the hasher had read and
// rejected the preallocated zeros, so nothing could be requested from a peer
// until a scan of the whole file set had finished.
func TestForcedRecheckLeavesFreshTorrentDownloadable(t *testing.T) {
	tor, st := resumeSessionFixture(t, 16)
	sess, err := newSession(tor, st, [20]byte{}, 0, st.BaseDir(), true)
	if err != nil {
		t.Fatal(err)
	}
	for idx, state := range sess.GetPieceStates() {
		if state != PieceEmpty {
			t.Fatalf("piece %d is %v, want PieceEmpty", idx, state)
		}
	}
	snap := sess.Snapshot()
	if snap.Status != "Downloading" || snap.Verification.Active {
		t.Fatalf("fresh torrent presented as checking: %+v", snap)
	}
	sess.mu.RLock()
	needed := len(sess.neededPieces)
	sess.mu.RUnlock()
	if needed != len(tor.PieceHashes) {
		t.Fatalf("requestable pieces = %d, want %d", needed, len(tor.PieceHashes))
	}
}

// The opportunistic scan of a freshly added torrent runs alongside a normal
// download, so the row must keep showing transfer progress rather than disk-scan
// progress for the length of the scan.
func TestFullScanIsNotPresentedAsChecking(t *testing.T) {
	tor, st := resumeSessionFixture(t, 16)
	sess, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	if !sess.IsVerifying() {
		t.Fatal("expected an opportunistic full scan")
	}
	if snap := sess.Snapshot(); snap.Verification.Active || snap.Status != "Downloading" {
		t.Fatalf("full scan presented as checking: %+v", snap)
	}
}

// Pausing must not run the per-file flush on the caller's goroutine: it is invoked
// straight from the TUI update loop, which cannot redraw or accept a keypress while
// a multi-gigabyte writeback is in flight.
func TestPauseDefersDurableCheckpointToPersistLoop(t *testing.T) {
	tor, st := resumeSessionFixture(t, 16)
	sess, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	sess.mu.Lock()
	for idx := range sess.PieceStates {
		sess.setPieceStateLocked(idx, PieceCompleted)
	}
	sess.verifying = false
	sess.verifyDone = nil
	sess.stateDirty = true
	sess.mu.Unlock()
	// The periodic hint flush clears stateDirty, which used to mean a pause more
	// than a tick later never produced a checkpoint at all.
	sess.flushState()
	st.resetSaves()

	sess.Pause()
	if saves := st.savedStates(); len(saves) != 0 {
		t.Fatalf("pause wrote resume state inline: %v", saves)
	}
	select {
	case <-sess.stateFlushCh:
	default:
		t.Fatal("pause did not ask the persistence goroutine to flush")
	}
	sess.flushState()
	if saves := st.savedStates(); len(saves) != 1 || !saves[0] {
		t.Fatalf("deferred flush = %v, want one durable write", saves)
	}

	st.checks = nil
	restarted, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	restarted.verifyResume(restarted.ctx)
	if len(st.checks) != 0 {
		t.Fatalf("paused session rehashed %v", st.checks)
	}
}

// Quitting is the last chance to record a partial download. Without it every piece
// downloaded since the previous checkpoint is rehashed on the next launch.
func TestCloseCheckpointsPartialDownload(t *testing.T) {
	tor, st := resumeSessionFixture(t, 16)
	sess, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	sess.mu.Lock()
	sess.setPieceStateLocked(0, PieceCompleted)
	sess.setPieceStateLocked(1, PieceCompleted)
	sess.verifying = false
	sess.verifyDone = nil
	sess.stateDirty = true
	sess.mu.Unlock()
	sess.flushState()
	st.resetSaves()

	sess.Close()
	saves := st.savedStates()
	if len(saves) == 0 || !saves[len(saves)-1] {
		t.Fatalf("close wrote %v, want a durable checkpoint", saves)
	}

	reopened, err := storage.NewFileStorage(st.BaseDir(), []storage.FileInfo{{Path: "a", Length: 8}, {Path: "b", Length: 8}}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.LoadResumeState(fmt.Sprintf("%x", tor.InfoHash))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, storage.ResumeState{Verified: []int{0, 1}}) {
		t.Fatalf("restored %+v, want both pieces verified", got)
	}
}

// A hint written while a recheck is running must not replace the checkpoint it is
// rechecking against: an interrupted check would otherwise lose the durable status
// of every file the check never touched.
func TestFlushDuringVerificationKeepsCheckpoint(t *testing.T) {
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
	if !sess.IsVerifying() {
		t.Fatal("expected a partial recheck")
	}
	// The periodic flush fires as soon as the first piece verifies; quitting before
	// the check finishes must leave the untouched pieces restorable.
	sess.mu.Lock()
	sess.stateDirty = true
	sess.mu.Unlock()
	sess.flushState()

	st.checks = nil
	next, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	next.verifyResume(next.ctx)
	if !reflect.DeepEqual(st.checks, []int64{2, 3}) {
		t.Fatalf("interrupted check rehashed %v, want only the changed file", st.checks)
	}
}

// A paused torrent shows its recheck progress, but everything derived from that
// display string must still treat it as paused.
func TestPausedRecheckSortsWithPausedTorrents(t *testing.T) {
	tor, st := resumeSessionFixture(t, 16)
	hash := fmt.Sprintf("%x", tor.InfoHash)
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().Add(time.Hour)
	if err := os.Chtimes(filepath.Join(st.BaseDir(), "b"), stamp, stamp); err != nil {
		t.Fatal(err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	sess.Pause()
	if got := sess.Status(); got != "Checking" {
		t.Fatalf("status = %q, want Checking", got)
	}
	if got := sess.GetSortSnapshot().StatusScore; got != 2 {
		t.Fatalf("sort score = %d, want the paused bucket", got)
	}
}

// The forced recheck applies to the torrents restored on this launch, not to every
// torrent added for the rest of the process lifetime.
func TestForcedRecheckAppliesOnlyToRestoredTorrents(t *testing.T) {
	mgr := NewTorrentManager()
	defer mgr.Close()
	mgr.SetVerifyOnStartup(true)

	mgr.mu.Lock()
	mgr.restoring = true
	mgr.mu.Unlock()
	restored, err := mgr.AddMagnet("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mgr.mu.Lock()
	mgr.restoring = false
	mgr.mu.Unlock()
	added, err := mgr.AddMagnet("magnet:?xt=urn:btih:89abcdef0123456789abcdef0123456789abcdef", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !restored.verifyOnStartup {
		t.Fatal("restored torrent was not marked for a forced recheck")
	}
	if added.verifyOnStartup {
		t.Fatal("a torrent added after startup inherited the forced recheck")
	}
}

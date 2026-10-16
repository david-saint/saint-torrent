package downloader

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

type countedVerificationStorage struct {
	*storage.FileStorage
	checks []int64

	mu        sync.Mutex
	saves     []bool
	onDurable func()
	onVerify  func(int64)
}

func (s *countedVerificationStorage) VerifyPiece(idx int64, hash [20]byte) (bool, error) {
	s.checks = append(s.checks, idx)
	if s.onVerify != nil {
		s.onVerify(idx)
	}
	return s.FileStorage.VerifyPiece(idx, hash)
}

func (s *countedVerificationStorage) SaveResumeState(hash string, verified, unverified []int, durable bool) error {
	s.mu.Lock()
	s.saves = append(s.saves, durable)
	observe := s.onDurable
	s.mu.Unlock()
	if durable && observe != nil {
		observe()
	}
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

// A payload that disappears at runtime must not park the torrent in Error: the
// checkpoint degrades that file to untrusted, and a persist failure that later
// resolves has to clear the status it set.
func TestPersistFailureClearsOnceItRecovers(t *testing.T) {
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
	failure := fmt.Errorf("failed to save fast-resume state: disk full")
	sess.statusErr = failure
	sess.lastErr = failure
	sess.statePersistErr = failure
	sess.mu.Unlock()

	if err = os.Remove(filepath.Join(st.BaseDir(), "b")); err != nil {
		t.Fatal(err)
	}
	sess.flushState()
	if got := sess.Status(); got != "Seeding" {
		t.Fatalf("status = %q, want Seeding after a recovered persist", got)
	}
	if got := sess.LastError(); got != nil {
		t.Fatalf("last error = %v, want nil", got)
	}
}

// closeTrackingConn reports whether the session has already dropped the peer.
type closeTrackingConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *closeTrackingConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

// A peer still reading when the shutdown checkpoint runs re-maps a file the
// checkpoint released, and releasing that mapping again at storage close moves the
// timestamps it just recorded, costing a full rehash on the next launch. Close
// stops serving first.
func TestCloseStopsServingBeforeCheckpoint(t *testing.T) {
	tor, st := resumeSessionFixture(t, 16)
	sess, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	defer remote.Close()
	tracked := &closeTrackingConn{Conn: local}
	var serving atomic.Bool
	st.mu.Lock()
	st.onDurable = func() { serving.Store(!tracked.closed.Load()) }
	st.mu.Unlock()

	sess.mu.Lock()
	sess.setPieceStateLocked(0, PieceCompleted)
	sess.verifying = false
	sess.verifyDone = nil
	sess.stateDirty = true
	sess.activePeers["127.0.0.1:6881"] = peer.NewClient(tracked, tor.InfoHash, [20]byte{})
	sess.mu.Unlock()

	sess.Close()
	if saves := st.savedStates(); len(saves) == 0 || !saves[len(saves)-1] {
		t.Fatalf("close wrote %v, want a durable checkpoint", saves)
	}
	if serving.Load() {
		t.Fatal("the shutdown checkpoint ran while a peer could still read through a mapping")
	}
}

func TestFreshlyCreatedStorageSkipsFullScan(t *testing.T) {
	data := []byte{1, 2, 3, 4}
	tor := &torrent.Torrent{
		Name:        "fresh",
		InfoHash:    sha1.Sum([]byte("fresh")),
		PieceLength: 4,
		Files:       []torrent.File{{Path: []string{"a"}, Length: 8}},
		PieceHashes: [][20]byte{sha1.Sum(data), sha1.Sum(data)},
	}
	st, err := storage.NewFileStorage(t.TempDir(), []storage.FileInfo{{Path: "a", Length: 8}}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	counted := &countedVerificationStorage{FileStorage: st}

	sess, err := NewSession(tor, counted, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	if sess.IsVerifying() {
		t.Fatal("freshly created empty storage triggered an opportunistic full scan")
	}
	sess.verifyResume(sess.ctx)
	if len(counted.checks) != 0 {
		t.Fatalf("freshly created storage hashed %v, want no checks", counted.checks)
	}
}

func TestFullScanSkipsAlreadyDownloadedPieces(t *testing.T) {
	tor, st := resumeSessionFixture(t, 16)
	sess, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	st.onVerify = func(idx int64) {
		if idx == 0 {
			sess.mu.Lock()
			sess.setPieceStateLocked(1, PieceCompleted)
			sess.setPieceStateLocked(2, PieceDownloading)
			sess.mu.Unlock()
		}
	}

	sess.verifyResume(sess.ctx)
	if !reflect.DeepEqual(st.checks, []int64{0, 3}) {
		t.Fatalf("full scan checked pieces %v, want only unclaimed [0 3]", st.checks)
	}
	if got := sess.Snapshot().Verification.CheckedBytes; got != 16 {
		t.Fatalf("checked bytes = %d, want 16 including skipped pieces", got)
	}
}

func TestFreshStorageRecognizesZeroPieces(t *testing.T) {
	for _, test := range []struct {
		name   string
		hashes [][20]byte
		states []PieceState
	}{
		{"all zero", [][20]byte{sha1.Sum(make([]byte, 4)), sha1.Sum(make([]byte, 4)), sha1.Sum(make([]byte, 2))}, []PieceState{PieceCompleted, PieceCompleted, PieceCompleted}},
		{"mixed", [][20]byte{sha1.Sum(make([]byte, 4)), sha1.Sum([]byte{1, 2, 3, 4}), sha1.Sum(make([]byte, 2))}, []PieceState{PieceCompleted, PieceEmpty, PieceCompleted}},
	} {
		for _, force := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/force=%v", test.name, force), func(t *testing.T) {
				tor := &torrent.Torrent{
					Name:        "fresh-zero",
					InfoHash:    sha1.Sum([]byte(test.name)),
					PieceLength: 4,
					Files:       []torrent.File{{Path: []string{"a"}, Length: 5}, {Path: []string{"b"}, Length: 5}},
					PieceHashes: test.hashes,
				}
				st, err := storage.NewFileStorage(t.TempDir(), []storage.FileInfo{{Path: "a", Length: 5}, {Path: "b", Length: 5}}, 4)
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				counted := &countedVerificationStorage{FileStorage: st}
				sess, err := newSession(tor, counted, [20]byte{}, 0, st.BaseDir(), force)
				if err != nil {
					t.Fatal(err)
				}
				defer sess.Close()
				sess.verifyResume(sess.ctx)
				if got := sess.GetPieceStates(); !reflect.DeepEqual(got, test.states) {
					t.Fatalf("piece states = %v, want %v", got, test.states)
				}
				if sess.IsVerifying() || len(counted.checks) != 0 {
					t.Fatalf("fresh payload triggered disk verification: %v", counted.checks)
				}
				if got, want := sess.IsCompleted(), test.name == "all zero"; got != want {
					t.Fatalf("completed = %v, want %v", got, want)
				}
			})
		}
	}
}

func TestQuarantineRechecksAllPayloadPieces(t *testing.T) {
	for _, test := range []struct {
		name       string
		verified   []int
		unverified []int
	}{
		{"complete checkpoint", []int{0, 1, 2, 3}, nil},
		{"partial checkpoint", []int{0, 2}, []int{1}},
		{"missing checkpoint", nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			tor, st := resumeSessionFixture(t, 16)
			if len(test.verified) > 0 {
				if err := st.SaveResumeState(fmt.Sprintf("%x", tor.InfoHash), test.verified, test.unverified, true); err != nil {
					t.Fatal(err)
				}
			}
			sess, err := NewSession(tor, st, [20]byte{}, 0, st.BaseDir())
			if err != nil {
				t.Fatal(err)
			}
			defer sess.Close()
			sess.mu.Lock()
			sess.paused = true
			sess.setQuarantinedLocked("previous run crashed")
			sess.mu.Unlock()
			sess.maybeStartVerification()
			if sess.IsCompleted() || !sess.IsVerifying() || len(st.checks) != 0 {
				t.Fatalf("quarantine completed=%v verifying=%v checks=%v", sess.IsCompleted(), sess.IsVerifying(), st.checks)
			}
			for idx, state := range sess.GetPieceStates() {
				if state != PieceUnverified {
					t.Fatalf("quarantined piece %d is %v, want PieceUnverified", idx, state)
				}
			}
			if err := st.WriteBlock(1, 0, []byte{9, 9, 9, 9}); err != nil {
				t.Fatal(err)
			}
			sess.Resume()
			sess.WaitVerified()
			if !reflect.DeepEqual(st.checks, []int64{0, 1, 2, 3}) {
				t.Fatalf("quarantine checked %v, want all pieces", st.checks)
			}
			want := []PieceState{PieceCompleted, PieceEmpty, PieceCompleted, PieceCompleted}
			if got := sess.GetPieceStates(); !reflect.DeepEqual(got, want) {
				t.Fatalf("piece states after recheck = %v, want %v", got, want)
			}
			if got := sess.Snapshot().Verification; got.CheckedBytes != 16 || got.TotalBytes != 16 {
				t.Fatalf("quarantine verification progress = %+v, want 16 bytes checked", got)
			}
		})
	}
}

func TestOnStateChangeSchedulesAsyncManagerSave(t *testing.T) {
	configDir := filepath.Join(t.TempDir(), "config")
	mgr := NewTorrentManager()
	defer mgr.Close()
	if _, err := mgr.EnablePersistence(configDir); err != nil {
		t.Fatal(err)
	}
	data, infoHash := testTorrent(t, "async-save.bin", false)
	torrentPath := filepath.Join(t.TempDir(), "async-save.torrent")
	if err := os.WriteFile(torrentPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	sess, err := mgr.AddTorrentFile(torrentPath, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	hashHex := fmt.Sprintf("%x", infoHash)

	// Hold writeMu to simulate a slow in-flight disk sync; Pause and SetGlobalDownloadLimit
	// must return immediately without blocking the caller (the TUI thread).
	mgr.writeMu.Lock()
	done := make(chan struct{})
	go func() {
		sess.Pause()
		mgr.SetGlobalDownloadLimit(54321)
		mgr.SetGlobalUploadLimit(12345)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		mgr.writeMu.Unlock()
		t.Fatal("Pause or SetGlobalDownloadLimit blocked on writeMu")
	}
	mgr.writeMu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for {
		mgr.writeMu.Lock()
		data, err := os.ReadFile(filepath.Join(configDir, "session.json"))
		mgr.writeMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		var state PersistedState
		if err := json.Unmarshal(data, &state); err != nil {
			t.Fatal(err)
		}
		paused := false
		for _, entry := range state.Torrents {
			if entry.InfoHashHex == hashHex {
				paused = entry.Paused
			}
		}
		if paused && state.GlobalDownloadLimit == 54321 && state.GlobalUploadLimit == 12345 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("async state save did not persist pause and limits: %+v", state)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

package downloader

import (
	"crypto/sha1"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// newPieceTestSession builds a single-file session with numPieces pieces of the
// given length, backed by real storage in a temp dir.
func newPieceTestSession(t testing.TB, pieceLen int64, pieces [][]byte) *Session {
	t.Helper()
	dir := t.TempDir()
	hashes := make([][20]byte, len(pieces))
	var total int64
	for i, p := range pieces {
		hashes[i] = sha1.Sum(p)
		total += int64(len(p))
	}
	tor := &torrent.Torrent{
		Name:        "opt.bin",
		InfoHash:    sha1.Sum([]byte("opt")),
		PieceLength: pieceLen,
		PieceHashes: hashes,
		Files:       []torrent.File{{Length: total, Path: []string{"opt.bin"}}},
	}
	st, err := storage.NewStorage(dir, []storage.FileInfo{{Path: "opt.bin", Length: total}}, pieceLen)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, dir)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	t.Cleanup(func() { sess.Close() })
	return sess
}

// --- #2: async hash/write pool ---

// TestProcessCompletedPieceWritesAndMarks proves the pool writes a verified piece to
// storage and marks it complete (the work that used to run inline on the read loop).
func TestProcessCompletedPieceWritesAndMarks(t *testing.T) {
	data := []byte("a verified piece of exactly some bytes!!")
	sess := newPieceTestSession(t, int64(len(data)), [][]byte{data})

	// Claim the piece as a peer would before handing it to the pool.
	sess.mu.Lock()
	sess.PieceStates[0] = PieceDownloading
	sess.removeNeededLocked(0)
	sess.mu.Unlock()

	sess.processCompletedPiece(pieceWriteJob{index: 0, hash: sha1.Sum(data), data: data})

	if got := sess.GetPieceStates()[0]; got != PieceCompleted {
		t.Fatalf("expected piece 0 completed, got %v", got)
	}
	buf := make([]byte, len(data))
	if _, err := sess.Storage.ReadBlock(0, 0, buf); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(buf) != string(data) {
		t.Fatalf("storage content mismatch: got %q", buf)
	}
}

// TestProcessCompletedPieceBadHashResetsAndDisconnects proves a corrupt piece is
// returned to the pool (PieceEmpty + re-added to the needed set), the error is
// recorded, and the feeding peer's connection is closed to drop it.
func TestProcessCompletedPieceBadHashResetsAndDisconnects(t *testing.T) {
	good := []byte("the real bytes for this piece slot here.")
	sess := newPieceTestSession(t, int64(len(good)), [][]byte{good})

	sess.mu.Lock()
	sess.PieceStates[0] = PieceDownloading
	sess.removeNeededLocked(0)
	sess.mu.Unlock()

	peerConn, ourEnd := net.Pipe()
	defer ourEnd.Close()
	closed := make(chan struct{})
	go func() {
		// A real peer read loop would unblock and exit when its conn is closed.
		buf := make([]byte, 1)
		_, _ = peerConn.Read(buf)
		close(closed)
	}()

	bad := []byte("WRONG bytes that won't hash to the slot.")
	sess.processCompletedPiece(pieceWriteJob{index: 0, hash: sha1.Sum(good), data: bad, conn: peerConn})

	if got := sess.GetPieceStates()[0]; got != PieceEmpty {
		t.Fatalf("expected corrupt piece reset to PieceEmpty, got %v", got)
	}
	sess.mu.RLock()
	_, needed := sess.neededPieces[0]
	sess.mu.RUnlock()
	if !needed {
		t.Fatal("expected corrupt piece returned to the needed set for re-download")
	}
	if err := sess.LastError(); err == nil {
		t.Fatal("expected a hash verification error to be recorded")
	}
	if status := sess.Status(); status != "Downloading" {
		t.Fatalf("expected recoverable hash failure to keep Downloading status, got %q", status)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("feeding peer connection was not closed on hash failure")
	}
}

// TestProcessCompletedPieceSkippedAfterClose proves a late write after Close is
// dropped rather than resurrecting completion/state for a tearing-down session.
func TestProcessCompletedPieceSkippedAfterClose(t *testing.T) {
	data := []byte("bytes for a piece written after close")
	sess := newPieceTestSession(t, int64(len(data)), [][]byte{data})

	sess.mu.Lock()
	sess.PieceStates[0] = PieceDownloading
	sess.mu.Unlock()
	sess.Close()

	sess.processCompletedPiece(pieceWriteJob{index: 0, hash: sha1.Sum(data), data: data})

	if got := sess.GetPieceStates()[0]; got == PieceCompleted {
		t.Fatal("expected a post-close write to be dropped, not marked complete")
	}
}

// --- #6: incremental needed-piece set ---

// TestNeededPieceSetMaintenance exercises the incremental maintenance of the needed
// set across the transitions the picker relies on.
func TestNeededPieceSetMaintenance(t *testing.T) {
	tempDir := t.TempDir()
	// Two 32 KB files, one piece each, so file priorities map cleanly to pieces.
	tor := &torrent.Torrent{
		Name:        "twofile",
		PieceLength: 32768,
		PieceHashes: [][20]byte{sha1.Sum([]byte("p0")), sha1.Sum([]byte("p1"))},
		Files: []torrent.File{
			{Length: 32768, Path: []string{"f0"}},
			{Length: 32768, Path: []string{"f1"}},
		},
	}
	st, err := storage.NewStorage(tempDir, []storage.FileInfo{
		{Path: filepath.Join(tor.Files[0].Path...), Length: 32768},
		{Path: filepath.Join(tor.Files[1].Path...), Length: 32768},
	}, tor.PieceLength)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, tempDir)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	needed := func() map[int]struct{} {
		sess.mu.RLock()
		defer sess.mu.RUnlock()
		cp := make(map[int]struct{}, len(sess.neededPieces))
		for k := range sess.neededPieces {
			cp[k] = struct{}{}
		}
		return cp
	}
	has := func(set map[int]struct{}, idx int) bool { _, ok := set[idx]; return ok }

	// Both pieces start empty and wanted -> both needed.
	n := needed()
	if !has(n, 0) || !has(n, 1) || len(n) != 2 {
		t.Fatalf("expected both pieces needed initially, got %v", n)
	}

	// Completing piece 0 removes it from the needed set.
	sess.markPieceCompleted(0)
	n = needed()
	if has(n, 0) || !has(n, 1) {
		t.Fatalf("expected only piece 1 needed after completing 0, got %v", n)
	}

	// Skipping file 1 removes piece 1 from the needed set (no longer wanted).
	sess.SetFilePriority(1, PrioritySkip)
	n = needed()
	if len(n) != 0 {
		t.Fatalf("expected no pieces needed after skipping the only remaining file, got %v", n)
	}

	// Re-prioritizing file 1 brings piece 1 back (still empty and now wanted).
	sess.SetFilePriority(1, PriorityHigh)
	n = needed()
	if !has(n, 1) || has(n, 0) || len(n) != 1 {
		t.Fatalf("expected piece 1 needed again after re-prioritizing, got %v", n)
	}
}

// TestSelectNeededPieceOrdering proves the picker honors priority (highest first),
// breaks ties by lowest index, and respects the peer's availability — while scanning
// only the needed set.
func TestSelectNeededPieceOrdering(t *testing.T) {
	tempDir := t.TempDir()
	// Three single-piece files so each piece's priority is set independently.
	const pieceLen = 32768
	tor := &torrent.Torrent{
		Name:        "threefile",
		PieceLength: pieceLen,
		PieceHashes: [][20]byte{sha1.Sum([]byte("p0")), sha1.Sum([]byte("p1")), sha1.Sum([]byte("p2"))},
		Files: []torrent.File{
			{Length: pieceLen, Path: []string{"f0"}},
			{Length: pieceLen, Path: []string{"f1"}},
			{Length: pieceLen, Path: []string{"f2"}},
		},
	}
	st, err := storage.NewStorage(tempDir, []storage.FileInfo{
		{Path: "f0", Length: pieceLen},
		{Path: "f1", Length: pieceLen},
		{Path: "f2", Length: pieceLen},
	}, tor.PieceLength)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, tempDir)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	all := func(int64) bool { return true }
	pick := func() int {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.selectNeededPieceLocked(all)
	}

	// All Normal priority, all available -> lowest index wins.
	if got := pick(); got != 0 {
		t.Fatalf("expected lowest-index pick 0, got %d", got)
	}

	// Raise piece 2 to High -> it should win over lower-index Normal pieces.
	sess.SetFilePriority(2, PriorityHigh)
	if got := pick(); got != 2 {
		t.Fatalf("expected high-priority pick 2, got %d", got)
	}

	// Peer that only has pieces 0 and 1 must not be offered the (high) piece 2.
	only01 := func(idx int64) bool { return idx == 0 || idx == 1 }
	sess.mu.Lock()
	got := sess.selectNeededPieceLocked(only01)
	sess.mu.Unlock()
	if got != 0 {
		t.Fatalf("expected pick 0 when peer lacks the high-priority piece, got %d", got)
	}
}

// TestSelectNeededPieceSelfHealsStaleEntries proves a needed-set entry whose piece is
// no longer empty (mutated out-of-band) is skipped and pruned rather than selected.
func TestSelectNeededPieceSelfHealsStaleEntries(t *testing.T) {
	data := []byte("a single-piece torrent payload exactly!!")
	sess := newPieceTestSession(t, int64(len(data)), [][]byte{data})

	// Complete the only piece directly (bypassing the maintenance helpers) so a stale
	// entry remains in the needed set.
	sess.mu.Lock()
	sess.PieceStates[0] = PieceCompleted
	sess.neededPieces[0] = struct{}{} // force the stale entry
	got := sess.selectNeededPieceLocked(func(int64) bool { return true })
	_, stillThere := sess.neededPieces[0]
	sess.mu.Unlock()

	if got != -1 {
		t.Fatalf("expected no selection for a completed piece, got %d", got)
	}
	if stillThere {
		t.Fatal("expected the stale needed entry to be pruned during selection")
	}
}

func TestSelectNeededPieceUsesAvailabilityBuckets(t *testing.T) {
	const numPieces = 256
	pieces := make([][]byte, numPieces)
	for i := range pieces {
		pieces[i] = []byte(fmt.Sprintf("piece-%03d-payload", i))
	}
	sess := newPieceTestSession(t, int64(len(pieces[0])), pieces)

	sess.mu.Lock()
	for i := range sess.PieceStates {
		sess.setPieceAvailabilityLocked(i, 100)
	}
	sess.setPieceAvailabilityLocked(numPieces-1, 1)
	calls := 0
	got := sess.selectNeededPieceLocked(func(int64) bool {
		calls++
		return true
	})
	sess.mu.Unlock()

	if got != numPieces-1 {
		t.Fatalf("expected rarest bucket piece %d, got %d", numPieces-1, got)
	}
	if calls != 1 {
		t.Fatalf("selection checked %d pieces; expected one predicate call from the rarest bucket", calls)
	}

	sess.mu.Lock()
	calls = 0
	got = sess.selectNeededPieceLocked(func(idx int64) bool {
		calls++
		return idx != numPieces-1
	})
	sess.mu.Unlock()

	if got != 0 {
		t.Fatalf("expected fall-through to piece 0 when peer lacks rarest piece, got %d", got)
	}
	if calls != 2 {
		t.Fatalf("fall-through checked %d pieces; expected rarest skip plus next-bucket pick", calls)
	}
}

func TestHasSelectableNeededPieceRepairsBucketDrift(t *testing.T) {
	data := []byte("a single-piece torrent payload exactly!!")
	sess := newPieceTestSession(t, int64(len(data)), [][]byte{data})

	sess.mu.Lock()
	sess.neededBuckets.total = 0 // simulate derived-index drift
	got := sess.hasSelectableNeededPieceLocked(func(int64) bool { return true })
	fresh := sess.neededBucketsFreshLocked()
	sess.mu.Unlock()

	if !got {
		t.Fatal("expected selectable work after repairing bucket drift")
	}
	if !fresh {
		t.Fatal("expected bucket index to be fresh after repair")
	}
}

func BenchmarkSelectNeededPiecePeerCount(b *testing.B) {
	for _, peerCount := range []int{50, 1000} {
		b.Run(fmt.Sprintf("peers_%d", peerCount), func(b *testing.B) {
			sess := newBenchmarkPickerSession(b, 4096, peerCount)
			hasAll := func(int64) bool { return true }
			want := len(sess.PieceStates) - 1

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sess.mu.Lock()
				got := sess.selectNeededPieceLocked(hasAll)
				sess.mu.Unlock()
				if got != want {
					b.Fatalf("pick=%d, want %d", got, want)
				}
			}
		})
	}
}

func newBenchmarkPickerSession(b *testing.B, numPieces, peerCount int) *Session {
	b.Helper()
	const pieceLen = int64(16)
	pieces := make([][]byte, numPieces)
	for i := range pieces {
		pieces[i] = []byte(fmt.Sprintf("%016d", i))
	}
	sess := newPieceTestSession(b, pieceLen, pieces)

	sess.mu.Lock()
	for i := range sess.PieceStates {
		sess.setPieceAvailabilityLocked(i, peerCount+1+(i%64))
	}
	sess.setPieceAvailabilityLocked(len(sess.PieceStates)-1, peerCount)
	sess.mu.Unlock()
	return sess
}

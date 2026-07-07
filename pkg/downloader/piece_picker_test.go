package downloader

import (
	"crypto/sha1"
	"fmt"
	"math/rand"
	"testing"

	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// newTestSessionBuilder constructs a Session for testing piece selection with multi-file setups.
func newTestSessionBuilder(t testing.TB, pieceLen int64, fileLengths []int64, priorities []FilePriority) *Session {
	t.Helper()
	dir := t.TempDir()

	var files []torrent.File
	var fileInfos []storage.FileInfo
	var totalSize int64

	for i, length := range fileLengths {
		name := fmt.Sprintf("file_%d.bin", i)
		files = append(files, torrent.File{
			Length: length,
			Path:   []string{name},
		})
		fileInfos = append(fileInfos, storage.FileInfo{
			Path:   name,
			Length: length,
		})
		totalSize += length
	}

	numPieces := int((totalSize + pieceLen - 1) / pieceLen)
	hashes := make([][20]byte, numPieces)
	for i := range hashes {
		hashes[i] = sha1.Sum([]byte(fmt.Sprintf("piece_%d", i)))
	}

	tor := &torrent.Torrent{
		Name:        "test_torrent",
		InfoHash:    sha1.Sum([]byte("test_torrent_info")),
		PieceLength: pieceLen,
		PieceHashes: hashes,
		Files:       files,
	}

	st, err := storage.NewStorage(dir, fileInfos, pieceLen)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	sess, err := NewSession(tor, st, [20]byte{}, 0, dir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.mu.Lock()
	// Set custom priorities if provided. Because applyFilePrioritiesLocked is an
	// overlay, we pad the priorities vector up to the file count with PrioritySkip
	// so trailing files are zero-filled/skipped rather than defaulting to Normal.
	if len(priorities) > 0 {
		fullPriorities := make([]FilePriority, len(tor.Files))
		for i := range fullPriorities {
			fullPriorities[i] = PrioritySkip
		}
		copy(fullPriorities, priorities)
		sess.applyFilePrioritiesLocked(fullPriorities)
	}
	sess.mu.Unlock()

	t.Cleanup(func() {
		sess.Close()
	})

	return sess
}

// TestPiecePropertiesDifferential verifies that the two-pointer sweep cache matches the slow oracle exactly.
func TestPiecePropertiesDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	pieceSizes := []int64{16384, 32768, 65536, 131072}

	for run := 0; run < 50; run++ {
		pieceLen := pieceSizes[rng.Intn(len(pieceSizes))]
		numFiles := rng.Intn(50) + 1

		fileLengths := make([]int64, numFiles)
		for i := 0; i < numFiles; i++ {
			// Include zero-length files with 10% probability
			if rng.Float64() < 0.1 {
				fileLengths[i] = 0
			} else {
				fileLengths[i] = int64(rng.Intn(1024*1024) + 1)
			}
		}

		// Priorities setup
		var priorities []FilePriority
		prioType := rng.Intn(4)
		if prioType == 1 {
			// Truncated priority vector (fewer priorities than files)
			priorities = make([]FilePriority, rng.Intn(numFiles))
			for i := range priorities {
				priorities[i] = FilePriority(rng.Intn(4)) // skip, normal, high
			}
		} else if prioType == 2 {
			// All skip
			priorities = make([]FilePriority, numFiles)
			for i := range priorities {
				priorities[i] = PrioritySkip
			}
		} else if prioType == 3 {
			// Normal priorities
			priorities = make([]FilePriority, numFiles)
			for i := range priorities {
				priorities[i] = FilePriority(rng.Intn(4))
			}
		} // type 0 is nil (default normal)

		sess := newTestSessionBuilder(t, pieceLen, fileLengths, priorities)

		sess.mu.Lock()
		numPieces := len(sess.PieceStates)
		for p := 0; p < numPieces; p++ {
			idx := int64(p)
			wantedFast := sess.isPieceWanted(idx)
			wantedSlow := sess.isPieceWantedSlow(idx)
			if wantedFast != wantedSlow {
				sess.mu.Unlock()
				t.Fatalf("Run %d: Piece %d wanted mismatch: fast=%t, slow=%t. PieceLength=%d, FileLengths=%v, Priorities=%v",
					run, p, wantedFast, wantedSlow, pieceLen, fileLengths, priorities)
			}

			priFast := sess.piecePriority(idx)
			priSlow := sess.piecePrioritySlow(idx)
			if priFast != priSlow {
				sess.mu.Unlock()
				t.Fatalf("Run %d: Piece %d priority mismatch: fast=%v, slow=%v. PieceLength=%d, FileLengths=%v, Priorities=%v",
					run, p, priFast, priSlow, pieceLen, fileLengths, priorities)
			}
		}
		sess.mu.Unlock()
	}
}

// TestFileStartOffsetTable verifies that the precomputed cumulative offset table is
// built after a recompute, that fileStartOffsetLocked returns the same values as a
// naive prefix sum for every file (including the out-of-range/total case), and that
// the prefix-sum fallback path stays correct when the table is absent.
func TestFileStartOffsetTable(t *testing.T) {
	rng := rand.New(rand.NewSource(7))

	naiveStart := func(files []torrent.File, fileIndex int) int64 {
		var start int64
		for i := 0; i < fileIndex && i < len(files); i++ {
			start += files[i].Length
		}
		return start
	}

	for run := 0; run < 40; run++ {
		pieceLen := int64(1 << (14 + rng.Intn(4))) // 16KiB..128KiB
		numFiles := rng.Intn(40) + 1
		fileLengths := make([]int64, numFiles)
		for i := range fileLengths {
			if rng.Float64() < 0.15 {
				fileLengths[i] = 0 // exercise zero-length files
			} else {
				fileLengths[i] = int64(rng.Intn(1024*1024) + 1)
			}
		}

		sess := newTestSessionBuilder(t, pieceLen, fileLengths, nil)

		sess.mu.Lock()
		// NewSession -> loadResumeState -> recomputeNeededLocked builds the table.
		if len(sess.fileStartOffsets) != numFiles+1 {
			sess.mu.Unlock()
			t.Fatalf("run %d: offset table len=%d, want %d", run, len(sess.fileStartOffsets), numFiles+1)
		}
		// Every table entry and lookup (0..numFiles, plus out of range) must match
		// the naive prefix sum.
		for i := 0; i <= numFiles+2; i++ {
			want := naiveStart(sess.Torrent.Files, i)
			if i < len(sess.fileStartOffsets) && sess.fileStartOffsets[i] != want {
				sess.mu.Unlock()
				t.Fatalf("run %d: table[%d]=%d, want %d", run, i, sess.fileStartOffsets[i], want)
			}
			if got := sess.fileStartOffsetLocked(i); got != want {
				sess.mu.Unlock()
				t.Fatalf("run %d: fileStartOffsetLocked(%d)=%d, want %d", run, i, got, want)
			}
		}

		// Fallback path: with the table cleared, the O(files) prefix sum must yield
		// identical results.
		sess.fileStartOffsets = nil
		for i := 0; i <= numFiles; i++ {
			want := naiveStart(sess.Torrent.Files, i)
			if got := sess.fileStartOffsetLocked(i); got != want {
				sess.mu.Unlock()
				t.Fatalf("run %d: fallback fileStartOffsetLocked(%d)=%d, want %d", run, i, got, want)
			}
		}

		// Rebuilding must repopulate the table and keep the caches consistent.
		sess.recomputeNeededLocked()
		if len(sess.fileStartOffsets) != numFiles+1 {
			sess.mu.Unlock()
			t.Fatalf("run %d: table not rebuilt, len=%d", run, len(sess.fileStartOffsets))
		}
		sess.mu.Unlock()
	}
}

// TestDownloadingIndexInvariant exercises state transition paths and verifies that downloadingPieces map matches PieceStates.
func TestDownloadingIndexInvariant(t *testing.T) {
	fileLengths := []int64{100000, 200000, 300000}
	sess := newTestSessionBuilder(t, 32768, fileLengths, nil)

	sess.mu.Lock()
	defer sess.mu.Unlock()

	assertInvariant := func(step string) {
		t.Helper()
		if err := sess.validateDownloadingIndexLocked(); err != nil {
			t.Fatalf("Invariant failed at step %q: %v", step, err)
		}
	}

	assertInvariant("initial")

	// 1. Peer Claim (Empty -> Downloading)
	sess.setPieceStateLocked(0, PieceDownloading)
	sess.setPieceStateLocked(2, PieceDownloading)
	sess.setPieceStateLocked(4, PieceDownloading)
	assertInvariant("claims")
	if _, ok := sess.downloadingPieces[0]; !ok {
		t.Error("expected piece 0 in downloading map")
	}

	// 2. Peer Release (Downloading -> Empty)
	sess.setPieceStateLocked(0, PieceEmpty)
	assertInvariant("release")
	if _, ok := sess.downloadingPieces[0]; ok {
		t.Error("expected piece 0 removed from downloading map")
	}

	// 3. Mark Complete (Downloading -> Completed)
	sess.setPieceStateLocked(2, PieceCompleted)
	assertInvariant("complete")
	if _, ok := sess.downloadingPieces[2]; ok {
		t.Error("expected piece 2 removed from downloading map")
	}

	// 4. Hash failure (Downloading -> Empty)
	sess.setPieceStateLocked(4, PieceEmpty)
	assertInvariant("hash-failure")

	// 5. Verification Check Success (Empty -> Completed)
	sess.setPieceStateLocked(1, PieceCompleted)
	assertInvariant("verify-success")

	// 6. Verification Check Incorrect Hint (Unverified -> Empty)
	sess.PieceStates[3] = PieceUnverified
	sess.setPieceStateLocked(3, PieceEmpty)
	assertInvariant("verify-hint-fail")

	// 7. Bulk reset (recomputeNeededLocked)
	sess.PieceStates[0] = PieceDownloading
	sess.PieceStates[3] = PieceDownloading
	sess.recomputeNeededLocked()
	assertInvariant("bulk-recompute")
	if len(sess.downloadingPieces) != 2 {
		t.Errorf("expected 2 downloading pieces after bulk recompute, got %d", len(sess.downloadingPieces))
	}
}

// TestEndgameEquivalence verifies that selectEndgamePieceLocked matches selectEndgamePieceSlow under random peers/availabilities.
func TestEndgameEquivalence(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	fileLengths := []int64{100000, 200000, 300000}
	sess := newTestSessionBuilder(t, 32768, fileLengths, nil)

	sess.mu.Lock()
	defer sess.mu.Unlock()

	numPieces := len(sess.PieceStates)

	for run := 0; run < 100; run++ {
		// Randomize PieceStates (some Empty, some Downloading, some Completed)
		for i := range sess.PieceStates {
			stateVal := rng.Intn(3)
			if stateVal == 0 {
				sess.PieceStates[i] = PieceEmpty
			} else if stateVal == 1 {
				sess.PieceStates[i] = PieceDownloading
			} else {
				sess.PieceStates[i] = PieceCompleted
			}
		}

		// Rebuild sets
		sess.recomputeNeededLocked()

		// Randomize piece availabilities
		for i := range sess.pieceAvailability {
			sess.pieceAvailability[i] = rng.Intn(10)
		}

		// Define randomized peer having pieces
		peerHaves := make([]bool, numPieces)
		for i := range peerHaves {
			peerHaves[i] = rng.Float64() < 0.7
		}
		hasPiece := func(idx int64) bool {
			if idx < 0 || idx >= int64(numPieces) {
				return false
			}
			return peerHaves[idx]
		}

		// Define randomized owned set
		owned := make(map[int64]bool)
		for i := 0; i < numPieces; i++ {
			if rng.Float64() < 0.2 {
				owned[int64(i)] = true
			}
		}

		pickFast := sess.selectEndgamePieceLocked(hasPiece, owned)
		pickSlow := sess.selectEndgamePieceSlow(hasPiece, owned)

		if pickFast != pickSlow {
			t.Fatalf("Run %d: endgame pick mismatch: fast=%d, slow=%d", run, pickFast, pickSlow)
		}
	}
}

// recomputeNeededSlow recomputes needed set using slow uncached properties (slow oracle baseline).
func (s *Session) recomputeNeededSlow() {
	if s.neededPieces == nil {
		s.neededPieces = make(map[int]struct{}, len(s.PieceStates))
	} else {
		clear(s.neededPieces)
	}
	s.neededBuckets.reset(len(s.PieceStates))
	if s.Storage == nil || s.Torrent == nil {
		return
	}
	for i, state := range s.PieceStates {
		if state == PieceEmpty && s.isPieceWantedSlow(int64(i)) {
			s.neededPieces[i] = struct{}{}
			s.neededBuckets.add(s.piecePrioritySlow(int64(i)), s.pieceAvailabilityAt(i), i)
		}
	}
}

// isPieceWantedSlow checks if a piece should be downloaded based on file selection (slow path, O(files^2)).
func (s *Session) isPieceWantedSlow(pieceIndex int64) bool {
	if s.Storage == nil || s.Torrent == nil {
		return true
	}
	if len(s.filePriorities) == 0 {
		return true
	}

	pieceStart := pieceIndex * s.Storage.PieceLengthValue()
	pieceEnd := pieceStart + s.Storage.PieceLength(pieceIndex)

	for i, f := range s.Torrent.Files {
		if i >= len(s.filePriorities) {
			return true
		}
		if s.filePriorities[i] == PrioritySkip {
			continue
		}

		var fileStart int64
		for j := 0; j < i; j++ {
			fileStart += s.Torrent.Files[j].Length
		}
		fileEnd := fileStart + f.Length

		if pieceStart < fileEnd && pieceEnd > fileStart {
			return true
		}
	}
	return false
}

// piecePrioritySlow returns the highest priority among files that overlap this piece (slow path, O(files^2)).
func (s *Session) piecePrioritySlow(pieceIndex int64) FilePriority {
	if s.Storage == nil || s.Torrent == nil {
		return PriorityNormal
	}
	if len(s.filePriorities) == 0 {
		return PriorityNormal
	}

	pieceStart := pieceIndex * s.Storage.PieceLengthValue()
	pieceEnd := pieceStart + s.Storage.PieceLength(pieceIndex)

	maxPri := PrioritySkip
	for i, f := range s.Torrent.Files {
		if i >= len(s.filePriorities) {
			break
		}
		var fileStart int64
		for j := 0; j < i; j++ {
			fileStart += s.Torrent.Files[j].Length
		}
		fileEnd := fileStart + f.Length

		if pieceStart < fileEnd && pieceEnd > fileStart {
			if s.filePriorities[i] > maxPri {
				maxPri = s.filePriorities[i]
			}
		}
	}
	return maxPri
}

// selectEndgamePieceSlow is kept as an oracle to compare against in tests.
func (s *Session) selectEndgamePieceSlow(hasPiece func(pieceIndex int64) bool, owned map[int64]bool) int {
	bestIdx := -1
	bestPriority := PrioritySkip
	bestAvail := 0
	for i, state := range s.PieceStates {
		if state != PieceDownloading {
			continue
		}
		idx := int64(i)
		if owned[idx] || !hasPiece(idx) || !s.isPieceWanted(idx) {
			continue
		}
		pri := s.piecePriority(idx)
		avail := s.pieceAvailabilityAt(i)
		if betterPick(bestIdx, i, bestPriority, pri, bestAvail, avail) {
			bestIdx, bestPriority, bestAvail = i, pri, avail
		}
	}
	return bestIdx
}

func BenchmarkRecomputeNeeded(b *testing.B) {
	// Synthetic torrent: ~10k pieces, ~1k files
	numFiles := 1000
	pieceLen := int64(1048576) // 1MB
	fileLengths := make([]int64, numFiles)
	for i := range fileLengths {
		fileLengths[i] = 10485760 // 10MB each
	}

	priorities := make([]FilePriority, numFiles)
	for i := range priorities {
		if i%10 == 0 {
			priorities[i] = PrioritySkip
		} else if i%10 == 1 {
			priorities[i] = PriorityHigh
		} else {
			priorities[i] = PriorityNormal
		}
	}

	sess := newTestSessionBuilder(b, pieceLen, fileLengths, priorities)

	b.Run("Slow-oracle", func(b *testing.B) {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sess.recomputeNeededSlow()
		}
	})

	b.Run("Fast-sweep", func(b *testing.B) {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sess.recomputeNeededLocked()
		}
	})
}

func BenchmarkEndgamePick(b *testing.B) {
	numFiles := 1000
	pieceLen := int64(1048576)
	fileLengths := make([]int64, numFiles)
	for i := range fileLengths {
		fileLengths[i] = 10485760
	}

	sess := newTestSessionBuilder(b, pieceLen, fileLengths, nil)

	sess.mu.Lock()
	// Set up ~10 pieces as PieceDownloading, the rest Completed
	for i := range sess.PieceStates {
		if i%1000 == 0 {
			sess.PieceStates[i] = PieceDownloading
		} else {
			sess.PieceStates[i] = PieceCompleted
		}
	}
	sess.recomputeNeededLocked()
	sess.mu.Unlock()

	hasPiece := func(idx int64) bool { return true }
	owned := make(map[int64]bool)

	b.Run("Slow-scan", func(b *testing.B) {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = sess.selectEndgamePieceSlow(hasPiece, owned)
		}
	})

	b.Run("Fast-indexed", func(b *testing.B) {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = sess.selectEndgamePieceLocked(hasPiece, owned)
		}
	})
}

func (s *Session) validateDownloadingIndexLocked() error {
	expectedCount := 0
	for _, state := range s.PieceStates {
		if state == PieceDownloading {
			expectedCount++
		}
	}
	if len(s.downloadingPieces) != expectedCount {
		return fmt.Errorf("downloadingPieces len mismatch: got %d, expected %d", len(s.downloadingPieces), expectedCount)
	}
	for k := range s.downloadingPieces {
		if k < 0 || k >= len(s.PieceStates) || s.PieceStates[k] != PieceDownloading {
			return fmt.Errorf("piece %d in downloadingPieces but not in PieceDownloading state", k)
		}
	}
	return nil
}

package downloader

import (
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"sainttorrent/pkg/torrent"
)

// statsStubStorageA1 is a storage without data: enough for sessions whose
// completion stats are exercised directly, with any number of files.
type statsStubStorageA1 struct {
	pieceLen, total int64
}

func (st *statsStubStorageA1) BaseDir() string         { return "" }
func (st *statsStubStorageA1) TotalSize() int64        { return st.total }
func (st *statsStubStorageA1) PieceLengthValue() int64 { return st.pieceLen }
func (st *statsStubStorageA1) PieceLength(i int64) int64 {
	start := i * st.pieceLen
	if i < 0 || start >= st.total {
		return 0
	}
	return min(st.pieceLen, st.total-start)
}
func (st *statsStubStorageA1) ReadBlock(int64, int64, []byte) (int, error) {
	return 0, errors.New("stub storage holds no data")
}
func (st *statsStubStorageA1) WriteBlock(int64, int64, []byte) error {
	return errors.New("stub storage holds no data")
}
func (st *statsStubStorageA1) VerifyPiece(int64, [20]byte) (bool, error) { return false, nil }
func (st *statsStubStorageA1) SaveState(string, []int) error             { return nil }
func (st *statsStubStorageA1) LoadState(string) ([]int, error)           { return nil, nil }
func (st *statsStubStorageA1) Close() error                              { return nil }

// newStatsSessionA1 builds a session over numFiles files whose lengths come from
// fileLen, in pieces of pieceLen bytes.
func newStatsSessionA1(tb testing.TB, numFiles int, pieceLen int64, fileLen func(i int) int64) *Session {
	tb.Helper()
	files := make([]torrent.File, numFiles)
	var total int64
	for i := range files {
		files[i] = torrent.File{Length: fileLen(i), Path: []string{"f", string(rune('a' + i%26))}}
		total += files[i].Length
	}
	numPieces := int((total + pieceLen - 1) / pieceLen)
	tor := &torrent.Torrent{
		Name:        "stats",
		PieceLength: pieceLen,
		PieceHashes: make([][20]byte, numPieces),
		Files:       files,
	}
	sess, err := NewSession(tor, &statsStubStorageA1{pieceLen: pieceLen, total: total}, [20]byte{}, 0, tb.TempDir())
	if err != nil {
		tb.Fatalf("session: %v", err)
	}
	tb.Cleanup(sess.Close)
	return sess
}

// referenceOverlapsA1 computes, from first principles, the wanted bytes of every
// piece: the overlap with every file whose priority is not skip.
func referenceOverlapsA1(files []torrent.File, prios []FilePriority, pieceLen int64, numPieces int) []int64 {
	overlaps := make([]int64, numPieces)
	var fileStart int64
	for i, f := range files {
		fileEnd := fileStart + f.Length
		if prios[i] != PrioritySkip {
			for p := int(fileStart / pieceLen); p < numPieces && int64(p)*pieceLen < fileEnd; p++ {
				lo := max(fileStart, int64(p)*pieceLen)
				hi := min(fileEnd, int64(p+1)*pieceLen)
				if hi > lo {
					overlaps[p] += hi - lo
				}
			}
		}
		fileStart = fileEnd
	}
	return overlaps
}

// referenceStatsA1 derives the completion stats from the reference overlaps.
func referenceStatsA1(sess *Session, overlaps []int64, total int64) completionStats {
	var st completionStats
	st.totalBytes = total
	for p, overlap := range overlaps {
		completed := sess.PieceStates[p] == PieceCompleted
		if completed {
			st.completedTotalBytes += sess.Storage.PieceLength(int64(p))
		}
		st.wantedBytes += overlap
		if overlap > 0 {
			st.wantedPieces++
			if completed {
				st.completedWantedBytes += overlap
				st.completedWantedPieces++
			}
		}
	}
	return st
}

// recomputedStatsA1 returns what recomputeStatsLocked computes, leaving the
// session's stats and cached ranges as they were, so a stale cache is not
// repaired behind the incremental path's back. Caller holds sess.mu.
func recomputedStatsA1(sess *Session) completionStats {
	stats, ranges, valid := sess.stats, sess.statsRanges, sess.statsRangesValid
	sess.recomputeStatsLocked()
	recomputed := sess.stats
	sess.stats, sess.statsRanges, sess.statsRangesValid = stats, ranges, valid
	return recomputed
}

// The per-piece stats update now binary-searches cached wanted ranges and the
// recompute merges pieces and ranges in one pass. Completing pieces in random
// order over a 2000-file torrent with random skipped files, the incremental
// stats must match both a first-principles reference and a full recompute after
// every step, including after priority changes.
func TestCompletionStatsIncrementalMatchesRecompute(t *testing.T) {
	const (
		numFiles = 2000
		pieceLen = 1024
	)
	rng := rand.New(rand.NewPCG(1, 2))
	lengths := make([]int64, numFiles)
	for i := range lengths {
		switch rng.IntN(10) {
		case 0:
			lengths[i] = 0 // empty files own no bytes
		case 1:
			lengths[i] = int64(rng.IntN(8 * pieceLen)) // spans several pieces
		default:
			lengths[i] = int64(1 + rng.IntN(pieceLen))
		}
	}
	sess := newStatsSessionA1(t, numFiles, pieceLen, func(i int) int64 { return lengths[i] })

	sess.mu.Lock()
	defer sess.mu.Unlock()
	files := sess.Torrent.Files
	numPieces := len(sess.PieceStates)
	total := sess.Storage.TotalSize()

	prios := make([]FilePriority, numFiles)
	for i := range prios {
		prios[i] = PriorityNormal
		if rng.IntN(3) == 0 {
			prios[i] = PrioritySkip
		}
	}
	sess.applyFilePrioritiesLocked(prios)
	overlaps := referenceOverlapsA1(files, prios, pieceLen, numPieces)

	check := func(step string) {
		t.Helper()
		if want := referenceStatsA1(sess, overlaps, total); sess.stats != want {
			t.Fatalf("%s: stats %+v, reference %+v", step, sess.stats, want)
		}
		if want := recomputedStatsA1(sess); sess.stats != want {
			t.Fatalf("%s: stats %+v, recomputed %+v", step, sess.stats, want)
		}
	}
	check("initial")

	order := rng.Perm(numPieces)
	for k, p := range order {
		sess.setPieceStateLocked(p, PieceCompleted)
		check("after completing a piece")

		if k == numPieces/3 || k == 2*numPieces/3 {
			// Flip a batch of files, through both setters.
			for n := 0; n < 50; n++ {
				i := rng.IntN(numFiles)
				if prios[i] == PrioritySkip {
					prios[i] = PriorityHigh
				} else {
					prios[i] = PrioritySkip
				}
				if n%2 == 0 {
					sess.setFilePriorityLocked(i, prios[i])
				} else {
					sess.applyFilePrioritiesLocked(prios)
				}
			}
			overlaps = referenceOverlapsA1(files, prios, pieceLen, numPieces)
			check("after priority changes")
			if !slices.Equal(sess.statsRangesLocked(), sess.wantedStatsRangesLocked()) {
				t.Fatal("cached wanted ranges differ from the current file priorities")
			}
		}
	}
}

// Completing a piece takes the session write lock, so its stats update must not
// allocate: the old one rebuilt a range for every file per piece.
func TestPieceCompleteStatsAllocatesNothing(t *testing.T) {
	sess := newStatsSessionA1(t, 5000, 16*1024, func(i int) int64 { return int64(1000 + i%3000) })
	sess.mu.Lock()
	defer sess.mu.Unlock()
	numPieces := len(sess.PieceStates)
	i := 0
	if allocs := testing.AllocsPerRun(1000, func() {
		sess.updateStatsOnPieceCompleteLocked(i % numPieces)
		i++
	}); allocs != 0 {
		t.Fatalf("a piece completion's stats update made %.1f allocations, want 0", allocs)
	}
}

// BenchmarkPieceCompleteStatsManyFiles measures the stats update run under the
// session write lock for every completed piece of a 200k-file torrent.
func BenchmarkPieceCompleteStatsManyFiles(b *testing.B) {
	const pieceLen = 256 * 1024
	sess := newStatsSessionA1(b, 200_000, pieceLen, func(i int) int64 { return int64(4096 + (i*7919)%28672) })
	sess.mu.Lock()
	defer sess.mu.Unlock()
	numPieces := len(sess.PieceStates)
	_ = sess.statsRangesLocked()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess.updateStatsOnPieceCompleteLocked(i % numPieces)
	}
}

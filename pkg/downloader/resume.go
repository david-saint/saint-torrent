package downloader

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/storage"
	"sync"
	"time"
)

// verifyGate bounds how many piece hash checks run concurrently across all sessions,
// so background verification saturates available cores without thrashing the disk.
var verifyGate = make(chan struct{}, max(1, runtime.GOMAXPROCS(0)))

func (s *Session) logSessionEvent(level logging.Level, event string, fields ...logging.Field) {
	if !logging.EnabledFor(level) {
		return
	}
	s.mu.RLock()
	infoHash, name := s.logIdentityLocked()
	s.mu.RUnlock()
	base := []logging.Field{
		logging.String("info_hash", infoHash),
		logging.String("name", name),
	}
	base = append(base, fields...)
	logging.Log(level, event, base...)
}

// pieceWriteJob is a completed piece handed off to the async hash/write pool.
type pieceWriteJob struct {
	index int64
	hash  [20]byte
	data  []byte
	// conn is the connection of the peer that supplied the piece. If the assembled
	// data fails the SHA-1 check the worker closes it, dropping the misbehaving peer
	// (its read loop unblocks and exits) — the decoupled equivalent of the old inline
	// disconnect-on-corruption.
	conn                    net.Conn
	result                  chan<- pieceWriteResult
	recoverableStorageError bool
}

type pieceWriteStatus int

const (
	pieceWriteSkipped pieceWriteStatus = iota
	pieceWriteCompleted
	pieceWriteHashFailed
	pieceWriteStorageFailed
)

type pieceWriteResult struct {
	status pieceWriteStatus
	err    error
}

func (job pieceWriteJob) sendResult(status pieceWriteStatus, err error) {
	if job.result == nil {
		return
	}
	select {
	case job.result <- pieceWriteResult{status: status, err: err}:
	default:
	}
}

// pieceWriteQueueDepth bounds how many completed-piece buffers can be queued for the
// write pool. Each entry holds a full piece, so this caps the pool's memory; once
// full, submitting applies backpressure to the peer goroutine, which is the intended
// bound (a peer can't outrun the disk without limit).
const pieceWriteQueueDepth = 8

// ensurePieceWritePool lazily starts the background hash/write workers. Idempotent.
func (s *Session) ensurePieceWritePool() {
	s.pieceWriteOnce.Do(func() {
		s.pieceWriteCh = make(chan pieceWriteJob, pieceWriteQueueDepth)
		workers := max(1, runtime.GOMAXPROCS(0))
		for range workers {
			go s.pieceWriteWorker()
		}
	})
}

func (s *Session) pieceWriteWorker() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case job := <-s.pieceWriteCh:
			s.processCompletedPiece(job)
		}
	}
}

// processCompletedPiece verifies, writes, and records a completed piece off the peer
// read loop. On a hash mismatch it returns the piece to the pool and disconnects the
// feeding peer; on a storage repair it resets progress; otherwise it marks the piece
// complete (which persists fast-resume state and advertises Have).
func (s *Session) processCompletedPiece(job pieceWriteJob) {
	// Drop the write if the session is shutting down: the piece will be re-fetched on
	// the next run, and a late state persist must not resurrect a .state file a remove
	// is deleting. Also drop a piece already completed by another peer (#8 endgame
	// produces redundant copies) so we don't re-write storage or re-announce Have.
	s.mu.RLock()
	closed := s.closed
	alreadyDone := job.index >= 0 && job.index < int64(len(s.PieceStates)) &&
		s.PieceStates[job.index] == PieceCompleted
	s.mu.RUnlock()
	if closed || alreadyDone {
		job.sendResult(pieceWriteSkipped, nil)
		return
	}

	if sha1.Sum(job.data) != job.hash {
		verifyErr := fmt.Errorf("piece %d failed hash verification", job.index)
		s.mu.Lock()
		s.lastErr = verifyErr
		if job.index >= 0 && job.index < int64(len(s.PieceStates)) && s.PieceStates[job.index] == PieceDownloading {
			s.setPieceStateLocked(int(job.index), PieceEmpty)
		}
		s.mu.Unlock()
		if job.conn != nil {
			_ = job.conn.Close()
		}
		s.logSessionEvent(logging.LevelWarn, "piece_verification_failed",
			logging.Int64("piece", job.index),
			logging.Err(verifyErr),
		)
		job.sendResult(pieceWriteHashFailed, verifyErr)
		return
	}

	err := s.Storage.WriteBlock(job.index, 0, job.data)
	switch {
	case err == nil:
		s.markPieceCompleted(job.index)
		s.logSessionEvent(logging.LevelDebug, "piece_verified",
			logging.Int64("piece", job.index),
			logging.Int("bytes", len(job.data)),
		)
		job.sendResult(pieceWriteCompleted, nil)
	case errors.Is(err, storage.ErrFileRepaired):
		s.mu.Lock()
		s.lastErr = fmt.Errorf("download file was missing or resized; recreated target file")
		s.mu.Unlock()
		s.resetProgressAfterStorageRepair(job.index)
		s.logSessionEvent(logging.LevelWarn, "piece_storage_repaired",
			logging.Int64("piece", job.index),
			logging.Err(err),
		)
		job.sendResult(pieceWriteCompleted, nil)
	case errors.Is(err, storage.ErrStorageClosed):
		// Session is tearing down; nothing to record.
		job.sendResult(pieceWriteSkipped, err)
		return
	default:
		s.mu.Lock()
		s.lastErr = err
		if !job.recoverableStorageError {
			s.statusErr = err
		}
		if job.index >= 0 && job.index < int64(len(s.PieceStates)) && s.PieceStates[job.index] == PieceDownloading {
			s.setPieceStateLocked(int(job.index), PieceEmpty)
		}
		s.broadcastPieceWaitersLocked()
		s.mu.Unlock()
		s.logSessionEvent(logging.LevelError, "piece_storage_failed",
			logging.Int64("piece", job.index),
			logging.Err(err),
		)
		job.sendResult(pieceWriteStorageFailed, err)
	}
}

// loadResumeState reads the fast-resume hint and marks the pieces it claims complete
// as PieceUnverified (or schedules a full scan when there is no hint). It does no disk
// hashing, so it is cheap enough for the startup path; the actual hash verification is
// deferred to verifyResume, which Start() launches in the background.
//
// Invariant preserved from the original synchronous verifier ("never trust resume data
// blindly"): a PieceUnverified piece is never advertised in a bitfield, served to a
// peer, or counted toward seeding/completion until it has been hash-verified here.
func (s *Session) loadResumeState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Storage == nil || len(s.Torrent.PieceHashes) == 0 {
		return
	}
	s.PieceStates = make([]PieceState, len(s.Torrent.PieceHashes))

	infoHashHex := fmt.Sprintf("%x", s.Torrent.InfoHash)
	completedIndices, err := s.Storage.LoadState(infoHashHex)
	if err == nil {
		for _, idx := range completedIndices {
			if idx >= 0 && idx < len(s.PieceStates) {
				s.PieceStates[idx] = PieceUnverified
			}
		}
	} else {
		// No valid hint: leave pieces empty (a fresh torrent downloads immediately) and
		// let the background pass scan for any already-present data on disk.
		s.verifyFullScan = true
	}
	s.recomputeNeededLocked()
	s.recomputeStatsLocked()
	s.verifying = true
	s.verifyDone = make(chan struct{})
}

// maybeStartVerification launches the background verification goroutine exactly once.
// Idempotent; safe to call from Start() for both restored and freshly added torrents.
func (s *Session) maybeStartVerification() {
	s.mu.Lock()
	if !s.verifying || s.verifyStarted || s.closed {
		s.mu.Unlock()
		return
	}
	s.verifyStarted = true
	ctx := s.ctx
	s.mu.Unlock()

	// Deliberately NOT tracked by s.wg: a VerifyPiece read can block on slow I/O, and
	// Close()/RemoveSession() must never wait on it. The goroutine stops promptly on ctx
	// cancel (checked between pieces) and never mutates a closed session's state.
	go s.verifyResume(ctx)
}

// verifyResume hash-checks the pieces flagged by loadResumeState. On success a piece
// becomes PieceCompleted (now advertisable and seedable); on failure it returns to
// PieceEmpty so the downloader re-fetches it.
func (s *Session) verifyResume(ctx context.Context) {
	if s.runVerification(ctx) {
		s.finishVerify()
	}
	// If cancelled mid-way the session is shutting down; WaitVerified wakes on ctx.
}

func (s *Session) runVerification(ctx context.Context) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	if s.Storage == nil || !s.verifying {
		s.mu.Unlock()
		return true
	}
	st := s.Storage
	pieceHashes := append([][20]byte(nil), s.Torrent.PieceHashes...)
	fullScan := s.verifyFullScan
	toCheck := make([]int, 0, len(s.PieceStates))
	for i, state := range s.PieceStates {
		if fullScan {
			if state == PieceEmpty {
				toCheck = append(toCheck, i)
			}
		} else if state == PieceUnverified {
			toCheck = append(toCheck, i)
		}
	}
	s.mu.Unlock()
	if len(toCheck) == 0 {
		return true
	}
	s.logSessionEvent(logging.LevelInfo, "resume_verification_started",
		logging.Int("pieces", len(toCheck)),
		logging.Bool("full_scan", fullScan),
	)
	verified := 0
	failed := 0

	// Take one verification slot for this session, bounding concurrent hashing across all
	// sessions. Acquisition is cancellable, and the slot is reclaimed by Close() even if a
	// VerifyPiece read is wedged on slow I/O — so a closed session can never permanently
	// consume verification capacity.
	select {
	case verifyGate <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	var relOnce sync.Once
	release := func() { relOnce.Do(func() { <-verifyGate }) }
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		release()
		return false
	}
	s.verifyGateRelease = release
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.verifyGateRelease = nil
		s.mu.Unlock()
		release()
	}()

	for _, idx := range toCheck {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		ok, verifyErr := st.VerifyPiece(int64(idx), pieceHashes[idx])

		s.mu.Lock()
		if s.closed || s.Storage != st || idx >= len(s.PieceStates) {
			s.mu.Unlock()
			return false
		}
		var nowCompleted bool
		if fullScan {
			// Adopt only positively-verified pieces, and only if the downloader has not
			// already claimed the slot in the meantime.
			if verifyErr == nil && ok && s.PieceStates[idx] == PieceEmpty {
				s.setPieceStateLocked(idx, PieceCompleted)
				nowCompleted = true
			}
		} else if s.PieceStates[idx] == PieceUnverified {
			if verifyErr == nil && ok {
				s.setPieceStateLocked(idx, PieceCompleted)
				nowCompleted = true
			} else {
				// Resume data was wrong: return the piece to the pool for re-download.
				s.setPieceStateLocked(idx, PieceEmpty)
				failed++
			}
		}
		if nowCompleted {
			s.signalPieceWaitersLocked(int64(idx))
			verified++
		}
		s.mu.Unlock()

		if nowCompleted {
			s.broadcastHave(uint32(idx))
		} else if !fullScan && (verifyErr != nil || !ok) {
			s.logSessionEvent(logging.LevelWarn, "resume_piece_verification_failed",
				logging.Int("piece", idx),
				logging.Bool("hash_ok", ok),
				logging.Err(verifyErr),
			)
		}
	}
	s.logSessionEvent(logging.LevelInfo, "resume_verification_finished",
		logging.Int("pieces", len(toCheck)),
		logging.Int("verified", verified),
		logging.Int("failed", failed),
		logging.Bool("full_scan", fullScan),
	)
	return true
}

// finishVerify clears the verifying flag, persists the verified piece set, fires the
// completion announce if the torrent is now done, and wakes WaitVerified callers.
func (s *Session) finishVerify() {
	s.mu.Lock()
	if !s.verifying {
		s.mu.Unlock()
		return
	}
	s.verifying = false
	s.verifyFullScan = false
	// Skip the state write if the session is closing so a late finish can't resurrect a
	// .state file that RemoveSession is deleting.
	if !s.closed {
		s.stateDirty = true
	}

	var completedNow bool
	if !s.completedAnnounced && !s.metadataMode && s.Storage != nil {
		stats := s.completionStatsLocked()
		if stats.completedTotalBytes == stats.totalBytes && stats.totalBytes > 0 {
			s.completedAnnounced = true
			s.queueTrackerEventLocked("completed")
			completedNow = true
		}
	}
	done := s.verifyDone
	s.verifyDone = nil
	s.mu.Unlock()

	s.flushState()

	if completedNow {
		select {
		case s.resumeCh <- struct{}{}:
		default:
		}
	}
	if done != nil {
		close(done)
	}
}

// IsVerifying reports whether background fast-resume verification is still pending.
func (s *Session) IsVerifying() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.verifying
}

// WaitVerified blocks until background verification finishes or the session is closed.
// Intended for tests and the headless benchmark path.
func (s *Session) WaitVerified() {
	s.mu.RLock()
	done := s.verifyDone
	verifying := s.verifying
	ctx := s.ctx
	s.mu.RUnlock()
	if !verifying || done == nil {
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// statePersistLoop runs in a dedicated background goroutine, checking for dirty state
// and flushing it to disk periodically.
func (s *Session) statePersistLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			s.flushState()
			return
		case <-ticker.C:
			s.flushState()
		}
	}
}

// flushState snapshots the current fast-resume completed pieces set under s.mu and
// calls s.Storage.SaveState off-lock to serialize and write to disk, ensuring that
// heavy JSON marshaling and slow filesystem I/O do not block peer loops or reads.
func (s *Session) flushState() {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.Lock()
	if s.Storage == nil || !s.stateDirty {
		s.mu.Unlock()
		return
	}
	infoHashHex := fmt.Sprintf("%x", s.Torrent.InfoHash)
	var completed []int
	for i, state := range s.PieceStates {
		// Include PieceUnverified so a quit mid-verification keeps the resume hint;
		// those pieces are re-checked (not re-downloaded) on the next start.
		if state == PieceCompleted || state == PieceUnverified {
			completed = append(completed, i)
		}
	}
	s.stateDirty = false
	closed := s.closed
	st := s.Storage
	s.mu.Unlock()

	if err := st.SaveState(infoHashHex, completed); err != nil {
		if err == storage.ErrStorageClosed && closed {
			return
		}
		s.mu.Lock()
		s.stateDirty = true // Retry on next tick
		stateErr := fmt.Errorf("failed to save fast-resume state: %w", err)
		s.lastErr = stateErr
		s.statusErr = stateErr
		// Wake any blocked readers so they observe the new statusErr rather than
		// sleeping until the next unrelated state change.
		s.broadcastPieceWaitersLocked()
		s.mu.Unlock()
	}
}

type byteRange struct {
	start int64
	end   int64
}

type completionStats struct {
	totalBytes            int64
	completedTotalBytes   int64
	wantedBytes           int64
	completedWantedBytes  int64
	wantedPieces          int
	completedWantedPieces int
}

func (s *Session) completionStatsLocked() completionStats {
	return s.stats
}

// recomputeStatsLocked fully recalculates completion stats from scratch and caches them.
func (s *Session) recomputeStatsLocked() {
	if s.Storage == nil || s.Torrent == nil {
		s.stats = completionStats{}
		return
	}
	var stats completionStats
	stats.totalBytes = s.Storage.TotalSize()
	ranges := s.wantedStatsRangesLocked()

	for _, r := range ranges {
		if r.end > r.start {
			stats.wantedBytes += r.end - r.start
		}
	}

	for i, state := range s.PieceStates {
		pieceLen := s.Storage.PieceLength(int64(i))
		if state == PieceCompleted {
			stats.completedTotalBytes += pieceLen
		}

		wantedOverlap := s.pieceWantedOverlapLocked(i, ranges)
		if wantedOverlap > 0 {
			stats.wantedPieces++
			if state == PieceCompleted {
				stats.completedWantedBytes += wantedOverlap
				stats.completedWantedPieces++
			}
		}
	}
	s.stats = stats
}

// wantedStatsRangesLocked returns the wanted byte ranges used for completion stats,
// synthesizing a whole-storage range for legacy single-file torrents whose metadata
// carries no file list. Caller holds s.mu.
func (s *Session) wantedStatsRangesLocked() []byteRange {
	if s.Storage == nil || s.Torrent == nil {
		return nil
	}
	ranges := s.wantedFileRangesLocked()
	if len(ranges) == 0 && len(s.Torrent.Files) == 0 && s.Storage.TotalSize() > 0 {
		ranges = []byteRange{{start: 0, end: s.Storage.TotalSize()}}
	}
	return ranges
}

// pieceWantedOverlapLocked returns the number of bytes the given piece overlaps with the
// wanted ranges. Ranges are passed in (see wantedStatsRangesLocked) so bulk recomputes
// build them once instead of per piece. Caller holds s.mu.
func (s *Session) pieceWantedOverlapLocked(idx int, ranges []byteRange) int64 {
	pieceStart := int64(idx) * s.Storage.PieceLengthValue()
	pieceEnd := pieceStart + s.Storage.PieceLength(int64(idx))
	wantedOverlap := int64(0)
	for _, r := range ranges {
		overlapStart := maxInt64(pieceStart, r.start)
		overlapEnd := minInt64(pieceEnd, r.end)
		if overlapEnd > overlapStart {
			wantedOverlap += overlapEnd - overlapStart
		}
	}
	return wantedOverlap
}

// updateStatsOnPieceCompleteLocked incrementally updates the cached completion stats when
// a single piece transitions to PieceCompleted. Caller holds s.mu.
func (s *Session) updateStatsOnPieceCompleteLocked(idx int) {
	if s.Storage == nil {
		return
	}
	pieceLen := s.Storage.PieceLength(int64(idx))
	s.stats.completedTotalBytes += pieceLen
	wantedOverlap := s.pieceWantedOverlapLocked(idx, s.wantedStatsRangesLocked())
	if wantedOverlap > 0 {
		s.stats.completedWantedBytes += wantedOverlap
		s.stats.completedWantedPieces++
	}
}

func (s *Session) wantedFileRangesLocked() []byteRange {
	if s.Torrent == nil {
		return nil
	}

	ranges := make([]byteRange, 0, len(s.Torrent.Files))
	var fileStart int64
	for i, f := range s.Torrent.Files {
		fileEnd := fileStart + f.Length
		priority := PriorityNormal
		if len(s.FilePriorities) > 0 && i < len(s.FilePriorities) {
			priority = s.FilePriorities[i]
		}
		if priority != PrioritySkip && fileEnd > fileStart {
			ranges = append(ranges, byteRange{start: fileStart, end: fileEnd})
		}
		fileStart = fileEnd
	}
	return ranges
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (s *Session) markPieceCompleted(index int64) {
	s.mu.Lock()
	if s.PieceStates[index] == PieceCompleted {
		s.mu.Unlock()
		return
	}
	s.setPieceStateLocked(int(index), PieceCompleted)
	s.lastErr = nil
	s.statusErr = nil
	s.signalPieceWaitersLocked(index)
	// Skip the resume persist if the session is closing so a late piece write (the
	// async pool is not awaited by Close) cannot recreate a .state file a remove is
	// deleting — mirroring finishVerify.
	if !s.closed {
		s.stateDirty = true
	}

	var completedNow bool
	if !s.completedAnnounced && !s.metadataMode && s.Storage != nil {
		stats := s.completionStatsLocked()
		if stats.completedTotalBytes == stats.totalBytes && stats.totalBytes > 0 {
			s.completedAnnounced = true
			s.queueTrackerEventLocked("completed")
			completedNow = true
		}
	}
	s.mu.Unlock()

	// Notify active peers
	s.broadcastHave(uint32(index))

	if completedNow {
		select {
		case s.resumeCh <- struct{}{}:
		default:
		}
	}
}

func (s *Session) resetProgressAfterStorageRepair(index int64) {
	s.mu.Lock()
	for i := range s.PieceStates {
		s.PieceStates[i] = PieceEmpty
	}
	s.PieceStates[index] = PieceCompleted
	s.recomputeNeededLocked()
	s.recomputeStatsLocked()
	s.lastErr = nil
	s.statusErr = nil
	s.signalPieceWaitersLocked(index)
	if !s.closed {
		s.stateDirty = true
	}

	var completedNow bool
	if !s.completedAnnounced && !s.metadataMode && s.Storage != nil {
		stats := s.completionStatsLocked()
		if stats.completedTotalBytes == stats.totalBytes && stats.totalBytes > 0 {
			s.completedAnnounced = true
			s.queueTrackerEventLocked("completed")
			completedNow = true
		}
	}
	s.mu.Unlock()

	s.flushState()

	s.broadcastHave(uint32(index))

	if completedNow {
		select {
		case s.resumeCh <- struct{}{}:
		default:
		}
	}
}

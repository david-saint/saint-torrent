package downloader

// recomputeNeededLocked rebuilds the needed-piece set from scratch. Used after bulk
// state changes (resume load, metadata arrival, storage repair, priority changes).
// Caller holds s.mu.
func (s *Session) recomputeNeededLocked() {
	if s.neededPieces == nil {
		s.neededPieces = make(map[int]struct{}, len(s.PieceStates))
	} else {
		clear(s.neededPieces)
	}
	for i, state := range s.PieceStates {
		if state == PieceEmpty && s.isPieceWanted(int64(i)) {
			s.neededPieces[i] = struct{}{}
		}
	}
}

// addNeededLocked records a piece as needed when it (re)enters PieceEmpty, if wanted.
// Caller holds s.mu.
func (s *Session) addNeededLocked(idx int) {
	if idx < 0 || idx >= len(s.PieceStates) {
		return
	}
	if !s.isPieceWanted(int64(idx)) {
		return
	}
	if s.neededPieces == nil {
		s.neededPieces = make(map[int]struct{}, len(s.PieceStates))
	}
	s.neededPieces[idx] = struct{}{}
}

// removeNeededLocked drops a piece from the needed set when it leaves PieceEmpty.
// Caller holds s.mu.
func (s *Session) removeNeededLocked(idx int) {
	delete(s.neededPieces, idx)
}

// selectNeededPieceLocked returns the index of the piece to fetch next: the
// highest-priority, then rarest (lowest swarm availability), then lowest-index
// PieceEmpty wanted piece for which hasPiece reports the peer has it, or -1 if none.
// It scans only the incrementally-maintained needed set (empty & wanted) rather than
// every piece, so cost is O(remaining-needed) — shrinking toward zero as the download
// finishes — instead of O(total) per pick. The set is a hint: entries that are no
// longer empty (e.g. completed out-of-band) are pruned in-line, and membership is
// re-verified against PieceStates, so a stale entry is never mis-selected. Caller
// holds s.mu.
func (s *Session) selectNeededPieceLocked(hasPiece func(pieceIndex int64) bool) int {
	bestIdx := -1
	bestPriority := PrioritySkip
	bestAvail := 0
	for i := range s.neededPieces {
		if i < 0 || i >= len(s.PieceStates) || s.PieceStates[i] != PieceEmpty {
			delete(s.neededPieces, i)
			continue
		}
		idx := int64(i)
		if !hasPiece(idx) || !s.isPieceWanted(idx) {
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

// betterPick reports whether candidate piece cand (priority cp, availability ca)
// should beat the current best (priority bp, availability ba). Ordering: highest
// priority, then rarest (lowest availability), then lowest index. best == -1 means
// no candidate chosen yet.
func betterPick(best, cand int, bp, cp FilePriority, ba, ca int) bool {
	if best == -1 {
		return true
	}
	if cp != bp {
		return cp > bp
	}
	if ca != ba {
		return ca < ba
	}
	return cand < best
}

// pieceAvailabilityAt returns how many connected peers advertise piece i (0 if out
// of range). Caller holds s.mu.
func (s *Session) pieceAvailabilityAt(i int) int {
	if i >= 0 && i < len(s.pieceAvailability) {
		return s.pieceAvailability[i]
	}
	return 0
}

// bitfieldHas reports whether bit i is set in a BitTorrent bitfield (MSB-first
// within each byte).
func bitfieldHas(bf []byte, i int) bool {
	byteIdx := i / 8
	if byteIdx < 0 || byteIdx >= len(bf) {
		return false
	}
	return bf[byteIdx]&(1<<(7-uint(i%8))) != 0
}

// addPieceAvailability records that a peer now advertises piece idx (a Have).
func (s *Session) addPieceAvailability(idx int) {
	s.mu.Lock()
	if idx >= 0 && idx < len(s.pieceAvailability) {
		s.pieceAvailability[idx]++
	}
	s.mu.Unlock()
}

// applyBitfieldAvailability folds the delta between a peer's previous and new
// advertised bitfield into the swarm availability counts. This handles a peer that
// re-sends or extends its bitfield without double-counting.
func (s *Session) applyBitfieldAvailability(oldBF, newBF []byte) {
	s.mu.Lock()
	for i := range s.pieceAvailability {
		old := bitfieldHas(oldBF, i)
		now := bitfieldHas(newBF, i)
		switch {
		case now && !old:
			s.pieceAvailability[i]++
		case old && !now && s.pieceAvailability[i] > 0:
			s.pieceAvailability[i]--
		}
	}
	s.mu.Unlock()
}

// removePeerAvailability drops a disconnecting peer's contribution to the counts,
// using the bitfield it had accumulated (Haves set bits incrementally, so this is
// the exact set it added). Caller does not hold s.mu.
func (s *Session) removePeerAvailability(bf []byte) {
	if bf == nil {
		return
	}
	s.mu.Lock()
	for i := range s.pieceAvailability {
		if bitfieldHas(bf, i) && s.pieceAvailability[i] > 0 {
			s.pieceAvailability[i]--
		}
	}
	s.mu.Unlock()
}

// --- #8: endgame mode ---
//
// Endgame begins once every empty wanted piece has been claimed (the needed set is
// empty) while pieces are still in flight. Past that point a peer with nothing fresh
// to fetch would idle while the final pieces trickle in from the slowest peers, so we
// let it redundantly fetch a piece another peer already holds open. The first copy to
// complete wins; the losers cancel their outstanding block requests (see
// dropCompletedElsewhere in the peer loop).

// endgameActiveLocked reports whether the download has reached the completion tail:
// no empty wanted pieces are left to hand out, so remaining wanted pieces (if any) are
// all in flight. O(1). Caller holds s.mu.
func (s *Session) endgameActiveLocked() bool {
	return len(s.neededPieces) == 0
}

// selectEndgamePieceLocked picks the highest-priority, rarest, lowest-index in-progress
// (PieceDownloading) wanted piece the peer has that it is not already downloading, or
// -1. Used only in endgame to fetch a piece redundantly. Caller holds s.mu.
func (s *Session) selectEndgamePieceLocked(hasPiece func(pieceIndex int64) bool, owned map[int64]bool) int {
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

func (s *Session) blocksInPiece(pieceIndex int64) int64 {
	if s.Storage == nil {
		return 0
	}
	length := s.Storage.PieceLength(pieceIndex)
	return (length + BlockSize - 1) / BlockSize
}

// isPieceWanted checks if a piece should be downloaded based on file selection.
func (s *Session) isPieceWanted(pieceIndex int64) bool {
	// If no file priorities set, all pieces are wanted
	if len(s.FilePriorities) == 0 {
		return true
	}

	pieceStart := pieceIndex * s.Storage.PieceLengthValue()
	pieceEnd := pieceStart + s.Storage.PieceLength(pieceIndex)

	// Check if any file overlapping this piece has non-skip priority
	for i, f := range s.Torrent.Files {
		if i >= len(s.FilePriorities) {
			return true // default to wanted
		}
		if s.FilePriorities[i] == PrioritySkip {
			continue
		}

		// Calculate file boundaries in the torrent
		var fileStart int64
		for j := 0; j < i; j++ {
			fileStart += s.Torrent.Files[j].Length
		}
		fileEnd := fileStart + f.Length

		// Check overlap
		if pieceStart < fileEnd && pieceEnd > fileStart {
			return true
		}
	}
	return false
}

// piecePriority returns the highest priority among files that overlap this piece.
func (s *Session) piecePriority(pieceIndex int64) FilePriority {
	if len(s.FilePriorities) == 0 {
		return PriorityNormal
	}

	pieceStart := pieceIndex * s.Storage.PieceLengthValue()
	pieceEnd := pieceStart + s.Storage.PieceLength(pieceIndex)

	maxPri := PrioritySkip
	for i, f := range s.Torrent.Files {
		if i >= len(s.FilePriorities) {
			break
		}
		var fileStart int64
		for j := 0; j < i; j++ {
			fileStart += s.Torrent.Files[j].Length
		}
		fileEnd := fileStart + f.Length

		if pieceStart < fileEnd && pieceEnd > fileStart {
			if s.FilePriorities[i] > maxPri {
				maxPri = s.FilePriorities[i]
			}
		}
	}
	return maxPri
}

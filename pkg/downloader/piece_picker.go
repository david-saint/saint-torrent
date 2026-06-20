package downloader

import (
	"container/heap"
	"sort"
)

const neededBucketPriorities = int(PriorityHigh) + 1

// DefaultSequentialReadaheadPieces is the number of pieces prioritized ahead of a
// sequential read cursor when callers opt in without specifying a larger window.
const DefaultSequentialReadaheadPieces = 8

type intMinHeap []int

func (h intMinHeap) Len() int { return len(h) }

func (h intMinHeap) Less(i, j int) bool { return h[i] < h[j] }

func (h intMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *intMinHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *intMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type sortedIntSet []int

func (s *sortedIntSet) add(v int) {
	values := *s
	pos := sort.SearchInts(values, v)
	if pos < len(values) && values[pos] == v {
		return
	}
	values = append(values, 0)
	copy(values[pos+1:], values[pos:])
	values[pos] = v
	*s = values
}

type neededBucketMatch int

const (
	neededBucketSkip neededBucketMatch = iota
	neededBucketPick
	neededBucketDrop
)

type neededPieceBucket struct {
	members map[int]struct{}
	heap    intMinHeap
}

func newNeededPieceBucket() *neededPieceBucket {
	return &neededPieceBucket{members: make(map[int]struct{})}
}

func (b *neededPieceBucket) add(idx int) bool {
	if _, exists := b.members[idx]; exists {
		return false
	}
	b.members[idx] = struct{}{}
	heap.Push(&b.heap, idx)
	return true
}

func (b *neededPieceBucket) remove(idx int) bool {
	if _, exists := b.members[idx]; !exists {
		return false
	}
	delete(b.members, idx)
	b.compactIfSparse()
	return true
}

func (b *neededPieceBucket) compactIfSparse() {
	if len(b.heap) <= len(b.members)*2+64 {
		return
	}
	b.heap = b.heap[:0]
	for idx := range b.members {
		b.heap = append(b.heap, idx)
	}
	heap.Init(&b.heap)
}

func (b *neededPieceBucket) firstMatch(match func(int) neededBucketMatch) (int, []int) {
	var skipped []int
	var dropped []int
	defer func() {
		for _, idx := range skipped {
			heap.Push(&b.heap, idx)
		}
	}()

	for b.heap.Len() > 0 {
		idx := b.heap[0]
		if _, exists := b.members[idx]; !exists {
			heap.Pop(&b.heap)
			continue
		}
		switch match(idx) {
		case neededBucketPick:
			return idx, dropped
		case neededBucketDrop:
			heap.Pop(&b.heap)
			delete(b.members, idx)
			dropped = append(dropped, idx)
		case neededBucketSkip:
			skipped = append(skipped, heap.Pop(&b.heap).(int))
		}
	}
	return -1, dropped
}

type neededPieceBucketRef struct {
	priority     FilePriority
	availability int
	present      bool
}

type neededPieceBuckets struct {
	numPieces  int
	total      int
	refs       []neededPieceBucketRef
	byPriority [neededBucketPriorities]map[int]*neededPieceBucket
	levels     [neededBucketPriorities]sortedIntSet
}

func (b *neededPieceBuckets) reset(numPieces int) {
	*b = neededPieceBuckets{
		numPieces: numPieces,
		refs:      make([]neededPieceBucketRef, numPieces),
	}
}

func validNeededPriority(priority FilePriority) bool {
	return priority > PrioritySkip && int(priority) < neededBucketPriorities
}

func (b *neededPieceBuckets) bucketFor(priority FilePriority, availability int) *neededPieceBucket {
	if !validNeededPriority(priority) {
		return nil
	}
	if availability < 0 {
		availability = 0
	}
	priorityIdx := int(priority)
	if b.byPriority[priorityIdx] == nil {
		b.byPriority[priorityIdx] = make(map[int]*neededPieceBucket)
	}
	bucket := b.byPriority[priorityIdx][availability]
	if bucket == nil {
		bucket = newNeededPieceBucket()
		b.byPriority[priorityIdx][availability] = bucket
		b.levels[priorityIdx].add(availability)
	}
	return bucket
}

func (b *neededPieceBuckets) bucket(priority FilePriority, availability int) *neededPieceBucket {
	if !validNeededPriority(priority) || availability < 0 {
		return nil
	}
	buckets := b.byPriority[int(priority)]
	if buckets == nil {
		return nil
	}
	return buckets[availability]
}

func (b *neededPieceBuckets) add(priority FilePriority, availability, idx int) bool {
	if idx < 0 || idx >= len(b.refs) || !validNeededPriority(priority) {
		return false
	}
	if b.refs[idx].present {
		return false
	}
	bucket := b.bucketFor(priority, availability)
	if bucket == nil {
		return false
	}
	if bucket.add(idx) {
		if availability < 0 {
			availability = 0
		}
		b.refs[idx] = neededPieceBucketRef{
			priority:     priority,
			availability: availability,
			present:      true,
		}
		b.total++
		return true
	}
	return false
}

func (b *neededPieceBuckets) remove(idx int) bool {
	if idx < 0 || idx >= len(b.refs) {
		return false
	}
	ref := b.refs[idx]
	if !ref.present {
		return false
	}
	bucket := b.bucket(ref.priority, ref.availability)
	if bucket != nil {
		bucket.remove(idx)
	}
	b.refs[idx] = neededPieceBucketRef{}
	b.total--
	return true
}

func (b *neededPieceBuckets) move(idx int, priority FilePriority, availability int) {
	if idx < 0 || idx >= len(b.refs) || !validNeededPriority(priority) {
		return
	}
	if availability < 0 {
		availability = 0
	}
	ref := b.refs[idx]
	if !ref.present {
		b.add(priority, availability, idx)
		return
	}
	if ref.priority == priority && ref.availability == availability {
		return
	}
	oldBucket := b.bucket(ref.priority, ref.availability)
	newBucket := b.bucketFor(priority, availability)
	if oldBucket == nil || newBucket == nil {
		return
	}
	if !oldBucket.remove(idx) {
		return
	}
	newBucket.add(idx)
	b.refs[idx] = neededPieceBucketRef{
		priority:     priority,
		availability: availability,
		present:      true,
	}
}

func (b *neededPieceBuckets) firstMatch(match func(int) neededBucketMatch) (int, []int) {
	var allDropped []int
	for priorityIdx := int(PriorityHigh); priorityIdx > int(PrioritySkip); priorityIdx-- {
		for _, availability := range b.levels[priorityIdx] {
			bucket := b.byPriority[priorityIdx][availability]
			if bucket == nil || len(bucket.members) == 0 {
				continue
			}
			idx, dropped := bucket.firstMatch(match)
			if len(dropped) > 0 {
				b.total -= len(dropped)
				for _, dropIdx := range dropped {
					if dropIdx >= 0 && dropIdx < len(b.refs) {
						b.refs[dropIdx] = neededPieceBucketRef{}
					}
				}
				allDropped = append(allDropped, dropped...)
			}
			if idx != -1 {
				return idx, allDropped
			}
		}
	}
	return -1, allDropped
}

// recomputeNeededLocked rebuilds the needed-piece set from scratch. Used after bulk
// state changes (resume load, metadata arrival, storage repair, priority changes).
// Caller holds s.mu.
func (s *Session) recomputeNeededLocked() {
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
		if state == PieceEmpty && s.isPieceWanted(int64(i)) {
			s.neededPieces[i] = struct{}{}
			s.neededBuckets.add(s.piecePriority(int64(i)), s.pieceAvailabilityAt(i), i)
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
	if s.neededBuckets.numPieces != len(s.PieceStates) {
		s.recomputeNeededLocked()
		return
	}
	if _, exists := s.neededPieces[idx]; exists {
		return
	}
	s.neededPieces[idx] = struct{}{}
	s.neededBuckets.add(s.piecePriority(int64(idx)), s.pieceAvailabilityAt(idx), idx)
}

// removeNeededLocked drops a piece from the needed set when it leaves PieceEmpty.
// Caller holds s.mu.
func (s *Session) removeNeededLocked(idx int) {
	if s.neededPieces == nil {
		return
	}
	if _, exists := s.neededPieces[idx]; !exists {
		return
	}
	delete(s.neededPieces, idx)
	s.neededBuckets.remove(idx)
}

func (s *Session) neededBucketsFreshLocked() bool {
	return s.neededPieces != nil &&
		s.neededBuckets.numPieces == len(s.PieceStates) &&
		len(s.neededBuckets.refs) == len(s.PieceStates) &&
		s.neededBuckets.total == len(s.neededPieces)
}

func (s *Session) ensureNeededBucketsLocked() {
	if !s.neededBucketsFreshLocked() {
		s.recomputeNeededLocked()
	}
}

func (s *Session) neededCandidateLocked(idx int) bool {
	return idx >= 0 &&
		idx < len(s.PieceStates) &&
		s.PieceStates[idx] == PieceEmpty &&
		s.isPieceWanted(int64(idx))
}

// selectNeededPieceLocked returns the index of the piece to fetch next: the
// highest-priority, then rarest (lowest swarm availability), then lowest-index
// PieceEmpty wanted piece for which hasPiece reports the peer has it, or -1 if none.
// It walks priority/availability buckets instead of scanning every needed piece, so
// the common case checks the rarest bucket's lowest-index candidates rather than the
// whole remaining-needed set. Stale entries encountered during the walk are pruned
// in-line and never selected. Caller holds s.mu.
func (s *Session) selectNeededPieceLocked(hasPiece func(pieceIndex int64) bool) int {
	s.ensureNeededBucketsLocked()
	if bestIdx := s.selectSequentialNeededPieceLocked(hasPiece); bestIdx != -1 {
		return bestIdx
	}
	bestIdx, dropped := s.neededBuckets.firstMatch(func(i int) neededBucketMatch {
		if !s.neededCandidateLocked(i) {
			return neededBucketDrop
		}
		if !hasPiece(int64(i)) {
			return neededBucketSkip
		}
		return neededBucketPick
	})
	for _, idx := range dropped {
		delete(s.neededPieces, idx)
	}
	return bestIdx
}

func normalizeSequentialReadaheadPieces(n int) int {
	if n <= 0 {
		return DefaultSequentialReadaheadPieces
	}
	return n
}

// SetSequentialMode enables or disables sequential piece selection. When enabled,
// the picker first tries PieceEmpty wanted pieces in [startPiece,
// startPiece+readaheadPieces), ordered by piece index. If no candidate in that
// window is available from a peer, selection falls back to the normal priority +
// rarest-first strategy so bandwidth is not left idle.
func (s *Session) SetSequentialMode(enabled bool, startPiece int64, readaheadPieces int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequentialMode = enabled
	if startPiece < 0 {
		startPiece = 0
	}
	s.sequentialStartPiece = startPiece
	s.sequentialReadaheadPieces = normalizeSequentialReadaheadPieces(readaheadPieces)
}

// SequentialMode returns the current sequential picker configuration.
func (s *Session) SequentialMode() (enabled bool, startPiece int64, readaheadPieces int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sequentialMode, s.sequentialStartPiece, s.sequentialReadaheadPieces
}

// prioritizeSequentialReadLocked points sequential mode at a byte range that a
// stream reader is about to need. Caller holds s.mu.
func (s *Session) prioritizeSequentialReadLocked(offset, length int64, readaheadPieces int) {
	if s.Storage == nil || len(s.PieceStates) == 0 || offset < 0 {
		return
	}
	pieceLength := s.Storage.PieceLengthValue()
	if pieceLength <= 0 {
		return
	}
	startPiece := offset / pieceLength
	if startPiece >= int64(len(s.PieceStates)) {
		startPiece = int64(len(s.PieceStates) - 1)
	}
	window := normalizeSequentialReadaheadPieces(readaheadPieces)
	if length > 0 {
		lastByte := offset + length - 1
		if lastByte < offset {
			lastByte = offset
		}
		endPiece := lastByte / pieceLength
		span := int(endPiece - startPiece + 1)
		if span > window {
			window = span
		}
	}
	s.sequentialMode = true
	s.sequentialStartPiece = startPiece
	s.sequentialReadaheadPieces = window
}

func (s *Session) selectSequentialNeededPieceLocked(hasPiece func(pieceIndex int64) bool) int {
	if !s.sequentialMode || s.Storage == nil || len(s.PieceStates) == 0 {
		return -1
	}
	start := int(s.sequentialStartPiece)
	if start < 0 {
		start = 0
	}
	if start >= len(s.PieceStates) {
		return -1
	}

	for start < len(s.PieceStates) {
		state := s.PieceStates[start]
		if state != PieceCompleted && s.isPieceWanted(int64(start)) {
			break
		}
		start++
	}
	s.sequentialStartPiece = int64(start)
	if start >= len(s.PieceStates) {
		return -1
	}

	end := start + normalizeSequentialReadaheadPieces(s.sequentialReadaheadPieces)
	if end > len(s.PieceStates) {
		end = len(s.PieceStates)
	}
	for i := start; i < end; i++ {
		if _, needed := s.neededPieces[i]; !needed {
			continue
		}
		if !s.neededCandidateLocked(i) {
			s.removeNeededLocked(i)
			continue
		}
		if hasPiece(int64(i)) {
			return i
		}
	}
	return -1
}

func (s *Session) hasSelectableNeededPieceLocked(hasPiece func(pieceIndex int64) bool) bool {
	s.ensureNeededBucketsLocked()
	bestIdx, dropped := s.neededBuckets.firstMatch(func(i int) neededBucketMatch {
		if !s.neededCandidateLocked(i) {
			return neededBucketDrop
		}
		if !hasPiece(int64(i)) {
			return neededBucketSkip
		}
		return neededBucketPick
	})
	for _, idx := range dropped {
		delete(s.neededPieces, idx)
	}
	return bestIdx != -1
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

func (s *Session) setPieceAvailabilityLocked(idx, availability int) {
	if idx < 0 || idx >= len(s.pieceAvailability) {
		return
	}
	if availability < 0 {
		availability = 0
	}
	old := s.pieceAvailability[idx]
	if old == availability {
		return
	}
	if _, needed := s.neededPieces[idx]; needed {
		s.neededBuckets.move(idx, s.piecePriority(int64(idx)), availability)
	}
	s.pieceAvailability[idx] = availability
}

func (s *Session) changePieceAvailabilityLocked(idx, delta int) {
	if idx < 0 || idx >= len(s.pieceAvailability) {
		return
	}
	s.setPieceAvailabilityLocked(idx, s.pieceAvailability[idx]+delta)
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

// setBit sets bit i in a BitTorrent bitfield (MSB-first within each byte). It is
// the write-side counterpart to bitfieldHas; both encode the single on-the-wire
// bit ordering convention so callers never hand-roll the shift.
func setBit(bf []byte, i int) {
	byteIdx := i / 8
	if byteIdx < 0 || byteIdx >= len(bf) {
		return
	}
	bf[byteIdx] |= 1 << (7 - uint(i%8))
}

// addPieceAvailability records that a peer now advertises piece idx (a Have).
func (s *Session) addPieceAvailability(idx int) {
	s.mu.Lock()
	s.changePieceAvailabilityLocked(idx, 1)
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
			s.changePieceAvailabilityLocked(i, 1)
		case old && !now:
			s.changePieceAvailabilityLocked(i, -1)
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
		if bitfieldHas(bf, i) {
			s.changePieceAvailabilityLocked(i, -1)
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

package downloader

import (
	"container/heap"
	"sort"
)

const neededBucketPriorities = int(PriorityHigh) + 1

// DefaultSequentialReadaheadPieces is the number of pieces prioritized ahead of a
// sequential read cursor when callers opt in without specifying a larger window.
const DefaultSequentialReadaheadPieces = 8

type sequentialReadWindow struct {
	startPiece int64
	// endPiece bounds reader-owned windows. Manual sequential mode leaves it zero
	// so the historical sliding window behavior is preserved.
	endPiece        int64
	readaheadPieces int
}

type sequentialReadWindowRef struct {
	readerID int64
	manual   bool
	window   sequentialReadWindow
}

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

	s.rebuildPieceCachesLocked()
	s.recomputeDownloadingLocked()

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

// SetSequentialMode enables or disables the session-wide manual sequential picker
// window. Reader-private windows are tracked separately and are not cleared by
// disabling this manual mode. When enabled, the picker first tries PieceEmpty
// wanted pieces in [startPiece, startPiece+readaheadPieces), ordered by piece
// index. If no candidate in that window is available from a peer, selection falls
// back to the normal priority + rarest-first strategy so bandwidth is not left
// idle.
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

// SequentialMode returns the manual sequential picker configuration. It does not
// report reader-private windows registered by TorrentReader.
func (s *Session) SequentialMode() (enabled bool, startPiece int64, readaheadPieces int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sequentialMode, s.sequentialStartPiece, s.sequentialReadaheadPieces
}

func (s *Session) sequentialWindowForRangeLocked(offset, length, limitOffset int64, readaheadPieces int) (sequentialReadWindow, bool) {
	if s.Storage == nil || len(s.PieceStates) == 0 || offset < 0 {
		return sequentialReadWindow{}, false
	}
	pieceLength := s.Storage.PieceLengthValue()
	if pieceLength <= 0 {
		return sequentialReadWindow{}, false
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
	endPiece := startPiece + int64(window)
	if limitOffset > offset {
		limitEndPiece := (limitOffset-1)/pieceLength + 1
		if limitEndPiece < endPiece {
			endPiece = limitEndPiece
		}
	}
	if maxPiece := int64(len(s.PieceStates)); endPiece > maxPiece {
		endPiece = maxPiece
	}
	if endPiece <= startPiece {
		return sequentialReadWindow{}, false
	}
	return sequentialReadWindow{startPiece: startPiece, endPiece: endPiece, readaheadPieces: window}, true
}

// prioritizeSequentialReadLocked records the byte range that a stream reader is
// about to need without overwriting other active readers' windows. Caller holds
// s.mu.
func (s *Session) prioritizeSequentialReadLocked(readerID int64, offset, length, limitOffset int64, readaheadPieces int) {
	window, ok := s.sequentialWindowForRangeLocked(offset, length, limitOffset, readaheadPieces)
	if !ok {
		return
	}
	if s.sequentialReaderWindows == nil {
		s.sequentialReaderWindows = make(map[int64]sequentialReadWindow)
	}
	s.sequentialReaderWindows[readerID] = window
}

func (s *Session) selectSequentialNeededPieceLocked(hasPiece func(pieceIndex int64) bool) int {
	if (!s.sequentialMode && len(s.sequentialReaderWindows) == 0) ||
		s.Storage == nil ||
		len(s.PieceStates) == 0 {
		return -1
	}

	readerWindows := len(s.sequentialReaderWindows)
	if readerWindows == 0 {
		return s.selectSequentialWindowRefLocked(sequentialReadWindowRef{
			readerID: -1,
			manual:   true,
			window: sequentialReadWindow{
				startPiece:      s.sequentialStartPiece,
				readaheadPieces: s.sequentialReadaheadPieces,
			},
		}, hasPiece)
	}
	if readerWindows == 1 {
		for readerID, window := range s.sequentialReaderWindows {
			readerRef := sequentialReadWindowRef{readerID: readerID, window: window}
			if !s.sequentialMode {
				return s.selectSequentialWindowRefLocked(readerRef, hasPiece)
			}
			manualRef := sequentialReadWindowRef{
				readerID: -1,
				manual:   true,
				window: sequentialReadWindow{
					startPiece:      s.sequentialStartPiece,
					readaheadPieces: s.sequentialReadaheadPieces,
				},
			}
			firstRef, secondRef := manualRef, readerRef
			if sequentialWindowRefLess(readerRef, manualRef) {
				firstRef, secondRef = readerRef, manualRef
			}
			if piece := s.selectSequentialWindowRefLocked(firstRef, hasPiece); piece != -1 {
				return piece
			}
			return s.selectSequentialWindowRefLocked(secondRef, hasPiece)
		}
	}

	windows := make([]sequentialReadWindowRef, 0, 1+len(s.sequentialReaderWindows))
	if s.sequentialMode {
		windows = append(windows, sequentialReadWindowRef{
			readerID: -1,
			manual:   true,
			window: sequentialReadWindow{
				startPiece:      s.sequentialStartPiece,
				readaheadPieces: s.sequentialReadaheadPieces,
			},
		})
	}
	for readerID, window := range s.sequentialReaderWindows {
		windows = append(windows, sequentialReadWindowRef{
			readerID: readerID,
			window:   window,
		})
	}
	sort.Slice(windows, func(i, j int) bool { return sequentialWindowRefLess(windows[i], windows[j]) })

	for _, ref := range windows {
		if piece := s.selectSequentialWindowRefLocked(ref, hasPiece); piece != -1 {
			return piece
		}
	}
	return -1
}

func sequentialWindowRefLess(a, b sequentialReadWindowRef) bool {
	if a.window.startPiece != b.window.startPiece {
		return a.window.startPiece < b.window.startPiece
	}
	return a.readerID < b.readerID
}

func (s *Session) selectSequentialWindowRefLocked(ref sequentialReadWindowRef, hasPiece func(pieceIndex int64) bool) int {
	piece, adjusted := s.selectFromSequentialWindowLocked(ref.window, hasPiece)
	if ref.manual {
		s.sequentialStartPiece = adjusted.startPiece
		s.sequentialReadaheadPieces = adjusted.readaheadPieces
	} else if s.sequentialReaderWindows != nil {
		s.sequentialReaderWindows[ref.readerID] = adjusted
	}
	return piece
}

func (s *Session) selectFromSequentialWindowLocked(window sequentialReadWindow, hasPiece func(pieceIndex int64) bool) (int, sequentialReadWindow) {
	start := int(window.startPiece)
	if start < 0 {
		start = 0
	}
	if start >= len(s.PieceStates) {
		window.startPiece = int64(start)
		return -1, window
	}

	end := len(s.PieceStates)
	if window.endPiece > 0 && window.endPiece < int64(end) {
		end = int(window.endPiece)
	}
	if start >= end {
		window.startPiece = int64(start)
		return -1, window
	}

	for start < end {
		state := s.PieceStates[start]
		if state != PieceCompleted && s.isPieceWanted(int64(start)) {
			break
		}
		start++
	}
	window.startPiece = int64(start)
	if start >= end {
		return -1, window
	}

	if window.endPiece <= 0 {
		end = start + normalizeSequentialReadaheadPieces(window.readaheadPieces)
		if end > len(s.PieceStates) {
			end = len(s.PieceStates)
		}
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
			return i, window
		}
	}
	return -1, window
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
	for i := range s.downloadingPieces {
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

// fileStartOffsetLocked returns the byte offset of file fileIndex within the
// concatenated torrent. Caller holds s.mu (read or write).
func (s *Session) fileStartOffsetLocked(fileIndex int) int64 {
	var start int64
	for i := 0; i < fileIndex && i < len(s.Torrent.Files); i++ {
		start += s.Torrent.Files[i].Length
	}
	return start
}

// isPieceWanted checks if a piece should be downloaded based on file selection (cached, O(1)).
func (s *Session) isPieceWanted(pieceIndex int64) bool {
	if pieceIndex < 0 || pieceIndex >= int64(len(s.pieceWantedCache)) {
		return true // default to wanted
	}
	return s.pieceWantedCache[pieceIndex]
}

// piecePriority returns the highest priority among files that overlap this piece (cached, O(1)).
func (s *Session) piecePriority(pieceIndex int64) FilePriority {
	if pieceIndex < 0 || pieceIndex >= int64(len(s.piecePriorityCache)) {
		return PriorityNormal // default to Normal
	}
	return s.piecePriorityCache[pieceIndex]
}

// rebuildPieceCachesLocked rebuilds the pieceWantedCache and piecePriorityCache
// using an O(pieces + files) two-pointer sweep over pieces and files.
func (s *Session) rebuildPieceCachesLocked() {
	if s.Storage == nil || s.Torrent == nil {
		s.pieceWantedCache = nil
		s.piecePriorityCache = nil
		return
	}

	numFiles := len(s.Torrent.Files)
	numPieces := len(s.PieceStates)

	if cap(s.pieceWantedCache) >= numPieces {
		s.pieceWantedCache = s.pieceWantedCache[:numPieces]
	} else {
		s.pieceWantedCache = make([]bool, numPieces)
	}

	if cap(s.piecePriorityCache) >= numPieces {
		s.piecePriorityCache = s.piecePriorityCache[:numPieces]
	} else {
		s.piecePriorityCache = make([]FilePriority, numPieces)
	}

	// Precompute file start offsets to avoid repeated O(files) calculations
	fileOffsets := make([]int64, numFiles+1)
	var offset int64
	for i, f := range s.Torrent.Files {
		fileOffsets[i] = offset
		offset += f.Length
	}
	fileOffsets[numFiles] = offset

	pieceLengthVal := s.Storage.PieceLengthValue()
	fileIdx := 0

	hasUnprioritized := len(s.FilePriorities) < numFiles

	for p := 0; p < numPieces; p++ {
		pieceStart := int64(p) * pieceLengthVal
		pieceEnd := pieceStart + s.Storage.PieceLength(int64(p))

		maxPri := PrioritySkip
		if len(s.FilePriorities) == 0 {
			maxPri = PriorityNormal
		} else {
			// Sweep only files below cutoff: i < len(s.FilePriorities)
			for fileIdx < len(s.FilePriorities) && fileOffsets[fileIdx]+s.Torrent.Files[fileIdx].Length <= pieceStart {
				fileIdx++
			}
			for i := fileIdx; i < len(s.FilePriorities) && fileOffsets[i] < pieceEnd; i++ {
				fileStart := fileOffsets[i]
				fileEnd := fileStart + s.Torrent.Files[i].Length
				if pieceStart < fileEnd && pieceEnd > fileStart {
					if s.FilePriorities[i] > maxPri {
						maxPri = s.FilePriorities[i]
					}
				}
			}
		}
		s.piecePriorityCache[p] = maxPri
		s.pieceWantedCache[p] = len(s.FilePriorities) == 0 || hasUnprioritized || maxPri != PrioritySkip
	}
}

func (s *Session) recomputeDownloadingLocked() {
	if s.downloadingPieces == nil {
		s.downloadingPieces = make(map[int]struct{})
	} else {
		clear(s.downloadingPieces)
	}
	for i, state := range s.PieceStates {
		if state == PieceDownloading {
			s.downloadingPieces[i] = struct{}{}
		}
	}
}

// setPieceStateLocked updates a piece state and maintains needed/downloading sets and stats.
// Write s.PieceStates[idx] first, then maintain sets (neededPieces, downloadingPieces, stats).
// Deliberate bulk bypass exceptions: loadResumeState, resetProgressAfterStorageRepair.
func (s *Session) setPieceStateLocked(idx int, state PieceState) {
	if idx < 0 || idx >= len(s.PieceStates) {
		return
	}
	oldState := s.PieceStates[idx]
	if oldState == state {
		return
	}
	s.PieceStates[idx] = state

	if oldState != PieceCompleted && state == PieceCompleted {
		s.updateStatsOnPieceCompleteLocked(idx)
	}

	if state == PieceEmpty {
		s.addNeededLocked(idx)
	} else {
		s.removeNeededLocked(idx)
	}

	if state == PieceDownloading {
		s.downloadingPieces[idx] = struct{}{}
	} else {
		delete(s.downloadingPieces, idx)
	}
}

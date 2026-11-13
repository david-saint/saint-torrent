package storage

import (
	"encoding/json"
	"strconv"
	"sync"
)

// mtUpdate is one change to a file's recorded modification time, queued under mtMu
// for the next hint write to fold into its own copy.
type mtUpdate struct {
	idx   int
	mtime int64
}

// resumeHint is the state saveResumeHint keeps between ticks so that a tick costs
// work proportional to what changed rather than to the number of files, and so
// that mtMu is held only for work proportional to the dirty set.
//
// Everything in it belongs to the single hint writer: mu serializes hint writes
// and guards every field. The only fields shared with the block path are the
// FileStorage ones the hint reads under mtMu (dirty, checkpoint, mtDelta).
type resumeHint struct {
	mu sync.Mutex

	// layout key: the file table this cache was built for. Paths and lengths of a
	// layout never change, so a different table (or length) rebuilds the cache.
	layoutFirst *fileLayout
	layoutLen   int
	// frags holds, for every file, the serialized `{"path":...,"size":...,"mtime":`
	// prefix of its table entry; fragEnd[i] is where file i's fragment ends in it.
	frags   []byte
	fragEnd []int

	// mtimes mirrors stateFileMt by file index, maintained from mtDelta. synced
	// reports that it has been seeded; a storage not seeded by its constructor is
	// seeded from stateFileMt on the first tick.
	mtimes []int64
	synced bool

	// idents is the serialized, quoted identity of every file of the checkpoint
	// whose identity slice starts at identKey, so a new checkpoint (which always
	// allocates fresh slices) is the only thing that rebuilds it.
	identKey *string
	idents   []byte
	identEnd []int

	// Scratch reused across ticks.
	syncMark  []bool
	syncIdx   []int
	spareDelt []mtUpdate
	lastSize  int
}

// setStateMtLocked records a file's modification time and queues the change for the
// hint writer. The caller holds mtMu. If nobody is consuming the queue (state
// persisted only through SaveState), it is dropped in favor of one full resync
// rather than growing without bound.
func (s *FileStorage) setStateMtLocked(file *fileLayout, mtime int64) {
	s.stateFileMt[file.path] = mtime
	if s.mtResync {
		return
	}
	if len(s.mtDelta) > 2*len(s.files)+64 {
		s.mtDelta = nil
		s.mtResync = true
		return
	}
	s.mtDelta = append(s.mtDelta, mtUpdate{file.idx, mtime})
}

// hintSnapshot is what saveResumeHint takes from the shared bookkeeping under mtMu.
type hintSnapshot struct {
	checkpoint resumeCheckpoint
	delta      []mtUpdate
	resync     bool
}

// snapshotHintLocked takes the hint's view of the shared bookkeeping. Under mtMu it
// only refreshes the files written since the last persist, copies the checkpoint
// header and the indices of the files still owing a flush (the dirty set), and
// swaps out the queue of changed mtimes. Everything proportional to the number of
// files is done by the caller after the lock is released. The caller holds mtMu
// and h.mu.
func (s *FileStorage) snapshotHintLocked(h *resumeHint) hintSnapshot {
	s.refreshDirtyLocked()
	snap := hintSnapshot{checkpoint: s.checkpoint}
	h.syncIdx = h.syncIdx[:0]
	for file, flags := range s.dirty {
		if flags&dirtySync != 0 {
			h.syncIdx = append(h.syncIdx, file.idx)
		}
	}
	if !h.synced || s.mtResync {
		// Seeding or repairing the mirror needs every file's mtime.
		snap.resync = true
		h.mtimes = growInt64(h.mtimes, len(s.files))
		for i, file := range s.files {
			h.mtimes[i] = s.stateFileMt[file.path]
		}
		h.synced = true
		s.mtResync = false
		s.mtDelta = h.spareDelt[:0]
		h.spareDelt = nil
		return snap
	}
	snap.delta = s.mtDelta
	s.mtDelta = h.spareDelt[:0]
	h.spareDelt = nil
	return snap
}

func growInt64(buf []int64, n int) []int64 {
	if cap(buf) >= n {
		return buf[:n]
	}
	return make([]int64, n)
}

// prepareLayout (re)builds the per-file fragments when the file table is new.
func (h *resumeHint) prepareLayout(files []*fileLayout) {
	var first *fileLayout
	if len(files) > 0 {
		first = files[0]
	}
	if h.frags != nil && h.layoutFirst == first && h.layoutLen == len(files) {
		return
	}
	// A table swapped under a built cache invalidates the mtime mirror as well; a
	// first build keeps whatever the constructor seeded.
	if h.frags != nil || len(h.mtimes) != len(files) {
		h.synced = false
	}
	h.layoutFirst, h.layoutLen = first, len(files)
	h.frags = make([]byte, 0, len(files)*64)
	h.fragEnd = make([]int, len(files))
	h.syncMark = make([]bool, len(files))
	h.identKey = nil
	for i, file := range files {
		path, err := json.Marshal(file.path)
		if err != nil {
			// A string always marshals; fall back to a quoted empty path rather
			// than failing the hint, which the loader would reject on mismatch.
			path = []byte(`""`)
		}
		h.frags = append(h.frags, `{"path":`...)
		h.frags = append(h.frags, path...)
		h.frags = append(h.frags, `,"size":`...)
		h.frags = strconv.AppendInt(h.frags, file.length, 10)
		h.frags = append(h.frags, `,"mtime":`...)
		h.fragEnd[i] = len(h.frags)
	}
}

// prepareIdentities serializes the checkpoint's identities once per checkpoint.
func (h *resumeHint) prepareIdentities(identities []string) {
	if len(identities) == 0 {
		h.identKey, h.idents, h.identEnd = nil, nil, nil
		return
	}
	if h.identKey == &identities[0] && len(h.identEnd) == len(identities) {
		return
	}
	h.identKey = &identities[0]
	h.idents = h.idents[:0]
	h.identEnd = make([]int, len(identities))
	for i, identity := range identities {
		quoted, err := json.Marshal(identity)
		if err != nil {
			quoted = []byte(`""`)
		}
		h.idents = append(h.idents, quoted...)
		h.identEnd[i] = len(h.idents)
	}
}

func appendJSONString(dst []byte, s string) []byte {
	quoted, err := json.Marshal(s)
	if err != nil {
		return append(dst, `""`...)
	}
	return append(dst, quoted...)
}

func appendIntArray(dst []byte, values []int) []byte {
	dst = append(dst, '[')
	for i, v := range values {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = strconv.AppendInt(dst, int64(v), 10)
	}
	return append(dst, ']')
}

// appendHintJSON emits exactly what json.Marshal produces for the FastResumeState
// a hint describes (same fields, order, omitempty and null handling), assembled
// from the cached fragments. identities and mtimes are per file; a file in the
// dirty set (marked in h.syncMark) gets an empty identity and its live mtime, any
// other file replays the checkpoint when one is valid.
func (s *FileStorage) appendHintJSON(h *resumeHint, dst []byte, infoHash string, checkpoint resumeCheckpoint, verified, completed []int) []byte {
	n := len(s.files)
	dst = append(dst, `{"version":2`...)
	if s.pieceLength != 0 {
		dst = append(dst, `,"piece_length":`...)
		dst = strconv.AppendInt(dst, s.pieceLength, 10)
	}
	if len(verified) > 0 {
		dst = append(dst, `,"verified_pieces":`...)
		dst = appendIntArray(dst, verified)
	}
	if n > 0 {
		dst = append(dst, `,"identities":[`...)
		for i := 0; i < n; i++ {
			if i > 0 {
				dst = append(dst, ',')
			}
			if checkpoint.valid && !h.syncMark[i] {
				start := 0
				if i > 0 {
					start = h.identEnd[i-1]
				}
				dst = append(dst, h.idents[start:h.identEnd[i]]...)
			} else {
				dst = append(dst, `""`...)
			}
		}
		dst = append(dst, ']')
	}
	if resumeIdentityMode != "" {
		dst = append(dst, `,"identity_mode":`...)
		dst = appendJSONString(dst, resumeIdentityMode)
	}
	dst = append(dst, `,"info_hash_hex":`...)
	dst = appendJSONString(dst, infoHash)
	dst = append(dst, `,"files":`...)
	if n == 0 {
		dst = append(dst, `null`...)
	} else {
		dst = append(dst, '[')
		start := 0
		for i := 0; i < n; i++ {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = append(dst, h.frags[start:h.fragEnd[i]]...)
			start = h.fragEnd[i]
			mtime := h.mtimes[i]
			if checkpoint.valid && !h.syncMark[i] {
				mtime = checkpoint.mtimes[i]
			}
			dst = strconv.AppendInt(dst, mtime, 10)
			dst = append(dst, '}')
		}
		dst = append(dst, ']')
	}
	dst = append(dst, `,"completed_pieces":`...)
	if completed == nil {
		dst = append(dst, `null`...)
	} else {
		dst = appendIntArray(dst, completed)
	}
	return append(dst, '}')
}

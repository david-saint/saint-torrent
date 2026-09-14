package storage

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// resumeIdentityMode names the per-file identity recorded in a checkpoint. The
// change timestamp is what makes an in-place edit detectable even when a tool
// restores the modification time, so a checkpoint written with any other mode is
// rejected rather than trusted.
const resumeIdentityMode = "change_time"

// ResumeState distinguishes durable verified pieces from hints that still need hashing.
// Recheck may also contain pieces overlapping a changed file that were not in the hint.
type ResumeState struct {
	Verified []int
	Recheck  []int
}

// ResumeStorage is optional: other backends continue to use conservative LoadState hints.
type ResumeStorage interface {
	LoadResumeState(string) (ResumeState, error)
	SaveResumeState(infoHash string, verified, unverified []int, durable bool) error
}

// resumeCheckpoint remembers the last durable checkpoint this process wrote or
// loaded. Cheap hint writes replay it for files nothing has written since, so an
// interrupted recheck cannot demote durably verified pieces back to guesses.
type resumeCheckpoint struct {
	identities []string
	mtimes     []int64
	verified   []int
	completed  []int
	valid      bool
	// complete reports that every file was trusted, so an unchanged piece set can
	// reuse the checkpoint verbatim instead of sweeping the files again.
	complete bool
}

// LoadResumeState validates every file independently. Older checkpoints remain hints;
// a changed file invalidates only its overlapping pieces, including boundary pieces.
// Pieces the checkpoint never claimed stay absent from the result so they remain
// immediately downloadable instead of queueing behind a hash of never-written bytes.
func (s *FileStorage) LoadResumeState(infoHash string) (ResumeState, error) {
	var result ResumeState
	if s.closed.Load() {
		return result, ErrStorageClosed
	}
	name := "." + infoHash + ".state"
	if _, err := s.resolver.ResolveAndValidate(name); err != nil {
		return result, err
	}
	f, info, err := rootOpenNoFollowInfo(s.downloadRoot, name, resumeMetadataReadFlags, 0)
	if err != nil {
		return result, err
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return result, fmt.Errorf("resume state is not a readable regular file")
	}
	data, err := io.ReadAll(f)
	closeErr := f.Close()
	if err != nil {
		return result, err
	}
	if closeErr != nil {
		return result, closeErr
	}
	var state FastResumeState
	if err := json.Unmarshal(data, &state); err != nil {
		return result, err
	}
	if state.InfoHashHex != infoHash || len(state.Files) != len(s.files) || state.Version < 0 || state.Version > 2 {
		return result, fmt.Errorf("incompatible resume state")
	}
	if state.Version == 2 && (state.IdentityMode != resumeIdentityMode || state.PieceLength != s.pieceLength || len(state.Identities) != len(s.files)) {
		return result, fmt.Errorf("incompatible resume layout")
	}
	count := s.pieceCount()
	claimed := make([]bool, count)
	verified := make([]bool, count)
	recheck := make([]bool, count)
	for _, idx := range state.CompletedPieces {
		if idx < 0 || idx >= count || claimed[idx] {
			return result, fmt.Errorf("invalid resume piece index")
		}
		claimed[idx] = true
	}
	for _, idx := range state.VerifiedPieces {
		if idx < 0 || idx >= count || !claimed[idx] || verified[idx] {
			return result, fmt.Errorf("invalid verified piece index")
		}
		verified[idx] = state.Version == 2
	}
	for i, file := range s.files {
		saved := state.Files[i]
		if saved.Path != file.path || saved.Size != file.length {
			return result, fmt.Errorf("incompatible resume file layout")
		}
		matches := false
		if fi, h, ok := s.openPayloadMetadata(file, resumeMetadataReadFlags); ok {
			matches = fi.Size() == saved.Size && fi.ModTime().UnixNano() == saved.Mtime
			if state.Version == 2 {
				matches = matches && state.Identities[i] != "" && fileIdentity(h, fi) == state.Identities[i]
			}
			_ = h.Close()
		}
		if !matches && file.length > 0 {
			for piece := file.startOffset / s.pieceLength; piece <= (file.endOffset-1)/s.pieceLength; piece++ {
				recheck[piece] = true
			}
		}
	}
	for i := range claimed {
		if !claimed[i] {
			continue
		}
		if recheck[i] || !verified[i] {
			result.Recheck = append(result.Recheck, i)
		} else {
			result.Verified = append(result.Verified, i)
		}
	}
	if state.Version == 2 {
		s.adoptCheckpoint(state)
	}
	return result, nil
}

// openPayloadMetadata opens a payload file for metadata inspection, refusing to
// open anything that is not a regular file so a FIFO or device swapped in for a
// payload cannot block or side-effect the startup path.
func (s *FileStorage) openPayloadMetadata(file *fileLayout, flag int) (os.FileInfo, *os.File, bool) {
	entry, err := s.downloadRoot.Lstat(file.path)
	if err != nil || !entry.Mode().IsRegular() {
		return nil, nil, false
	}
	h, info, err := rootOpenNoFollowInfo(s.downloadRoot, file.path, flag, 0)
	if err != nil {
		return nil, nil, false
	}
	if !info.Mode().IsRegular() {
		_ = h.Close()
		return nil, nil, false
	}
	return info, h, true
}

// adoptCheckpoint records a loaded checkpoint so later hint writes replay its
// verified pieces instead of downgrading them to unhashed completion guesses.
func (s *FileStorage) adoptCheckpoint(state FastResumeState) {
	checkpoint := resumeCheckpoint{
		identities: append([]string(nil), state.Identities...),
		mtimes:     make([]int64, len(state.Files)),
		verified:   append([]int(nil), state.VerifiedPieces...),
		completed:  append([]int(nil), state.CompletedPieces...),
		valid:      true,
	}
	for i, saved := range state.Files {
		checkpoint.mtimes[i] = saved.Mtime
	}
	s.mtMu.Lock()
	s.checkpoint = checkpoint
	s.mtMu.Unlock()
}

func (s *FileStorage) pieceCount() int {
	if s.totalSize <= 0 {
		return 0
	}
	return int((s.totalSize-1)/s.pieceLength + 1)
}

// SaveResumeState emits a durable checkpoint only when the session is quiescent.
// Active downloads retain cheap hints, avoiding data flushes on the transfer path.
// Sync and serialization run outside the session/storage/dirty-set locks.
func (s *FileStorage) SaveResumeState(infoHash string, verified, unverified []int, durable bool) error {
	if s.closed.Load() {
		return ErrStorageClosed
	}
	name := "." + infoHash + ".state"
	if _, err := s.resolver.ResolveAndValidate(name); err != nil {
		return err
	}
	completed := append(append([]int(nil), verified...), unverified...)
	if !durable {
		return s.saveResumeHint(name, infoHash, verified, completed)
	}
	return s.saveResumeCheckpoint(name, infoHash, verified, completed)
}

// saveResumeHint writes the cheap periodic state. It replays the identities of the
// last durable checkpoint for every file nothing has written since, so a hint
// written mid-recheck (or at shutdown) preserves the pieces that checkpoint proved
// rather than replacing it with a version-less guess that forces a full rehash.
func (s *FileStorage) saveResumeHint(name, infoHash string, verified, completed []int) error {
	state := FastResumeState{IdentityMode: resumeIdentityMode, Version: 2, PieceLength: s.pieceLength, InfoHashHex: infoHash, CompletedPieces: completed}
	// Only pieces the caller still holds as verified may keep their durable status:
	// one it has queued for rechecking must not be blessed back into place.
	trusted := make([]bool, s.pieceCount())
	for _, idx := range verified {
		if idx >= 0 && idx < len(trusted) {
			trusted[idx] = true
		}
	}
	s.mtMu.Lock()
	s.refreshDirtyLocked()
	checkpoint := s.checkpoint
	for i, file := range s.files {
		identity := ""
		mtime := s.stateFileMt[file.path]
		if checkpoint.valid && s.dirty[file]&dirtySync == 0 {
			identity = checkpoint.identities[i]
			mtime = checkpoint.mtimes[i]
		}
		state.Identities = append(state.Identities, identity)
		state.Files = append(state.Files, resumeFileEntry{file.path, file.length, mtime})
	}
	s.mtMu.Unlock()
	for _, idx := range checkpoint.verified {
		if idx >= 0 && idx < len(trusted) && trusted[idx] {
			state.VerifiedPieces = append(state.VerifiedPieces, idx)
		}
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.writeResumeAtomically(name, data, false)
}

// saveResumeCheckpoint flushes the files written since the last checkpoint and
// records the metadata that lets the next launch restore them without hashing. A
// payload that cannot be opened, stat-ed or flushed only loses its own identity —
// its pieces are rechecked next launch — instead of failing the whole checkpoint
// and leaving the caller retrying a full sweep every second.
func (s *FileStorage) saveResumeCheckpoint(name, infoHash string, verified, completed []int) error {
	s.mtMu.Lock()
	dirty := s.dirty
	if s.checkpointCurrentLocked(verified, completed, dirty) {
		s.mtMu.Unlock()
		return nil
	}
	s.dirty = make(map[*fileLayout]dirtyState, len(dirty))
	expected := make(map[string]os.FileInfo, len(s.stateFileInfo))
	for path, info := range s.stateFileInfo {
		expected[path] = info
	}
	initial := make(map[string]os.FileInfo, len(s.initialInfo))
	for path, info := range s.initialInfo {
		initial[path] = info
	}
	s.mtMu.Unlock()
	saved := false
	pending := make(map[*fileLayout]dirtyState, len(dirty))
	defer func() {
		s.mtMu.Lock()
		if !saved {
			pending = dirty
		}
		for file, flags := range pending {
			if flags != 0 {
				s.dirty[file] |= flags
			}
		}
		s.mtMu.Unlock()
	}()

	state := FastResumeState{IdentityMode: resumeIdentityMode, Version: 2, PieceLength: s.pieceLength, InfoHashHex: infoHash, VerifiedPieces: verified, CompletedPieces: completed}
	checkpoint := resumeCheckpoint{identities: make([]string, len(s.files)), mtimes: make([]int64, len(s.files)), valid: true, complete: true}
	current := make(map[string]os.FileInfo, len(s.files))
	for i, file := range s.files {
		flags := dirty[file]
		info, identity := s.checkpointFile(file, flags, expected[file.path], initial[file.path])
		if identity == "" {
			// Keep the file queued for the next checkpoint: its data may still be
			// unflushed and its recorded identity is deliberately untrusted.
			pending[file] = flags
			checkpoint.complete = false
		}
		mtime := int64(0)
		if info != nil {
			mtime = info.ModTime().UnixNano()
			current[file.path] = info
		}
		checkpoint.identities[i] = identity
		checkpoint.mtimes[i] = mtime
		state.Identities = append(state.Identities, identity)
		state.Files = append(state.Files, resumeFileEntry{file.path, file.length, mtime})
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err = s.writeResumeAtomically(name, data, true); err != nil {
		return err
	}
	saved = true
	checkpoint.verified = append([]int(nil), verified...)
	checkpoint.completed = append([]int(nil), completed...)
	s.mtMu.Lock()
	for path, info := range current {
		s.stateFileMt[path] = info.ModTime().UnixNano()
		s.stateFileInfo[path] = info
	}
	s.checkpoint = checkpoint
	s.mtMu.Unlock()
	return nil
}

// checkpointCurrentLocked reports whether the on-disk checkpoint already describes
// this exact piece set with no file written since, so quitting or pausing an idle
// torrent costs no sweep at all. The caller holds mtMu.
func (s *FileStorage) checkpointCurrentLocked(verified, completed []int, dirty map[*fileLayout]dirtyState) bool {
	if !s.checkpoint.valid || !s.checkpoint.complete || !sameIndices(s.checkpoint.verified, verified) || !sameIndices(s.checkpoint.completed, completed) {
		return false
	}
	// Any pending bookkeeping means a file's recorded metadata is stale: it either
	// still owes a flush or its timestamps have moved since the snapshot.
	for _, flags := range dirty {
		if flags != 0 {
			return false
		}
	}
	return true
}

func sameIndices(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// checkpointFile flushes one payload file if it has unsynced writes and returns the
// metadata to record. An empty identity marks the file untrusted, which forces its
// pieces to be rechecked on the next launch.
func (s *FileStorage) checkpointFile(file *fileLayout, flags dirtyState, expected, initial os.FileInfo) (os.FileInfo, string) {
	openFlags := resumeMetadataReadFlags
	if flags&dirtySync != 0 {
		openFlags = resumeSyncOpenFlags
	}
	before, f, ok := s.openPayloadMetadata(file, openFlags)
	if !ok {
		return nil, ""
	}
	repaired := file.repaired.Load()
	// A file written since the last metadata snapshot cannot be compared against it:
	// our own writes moved its timestamps. See the fast-resume trust model in README.
	trustworthy := !repaired && before.Size() == file.length && initial != nil && os.SameFile(before, initial) &&
		(flags&dirtyMeta != 0 || sameFileVersion(before, expected))
	identity := fileIdentity(f, before)
	if flags&dirtySync != 0 {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, ""
		}
	}
	after, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, ""
	}
	if err = f.Close(); err != nil {
		return nil, ""
	}
	// A concurrent external modification between the two stats invalidates the file.
	trustworthy = trustworthy && identity == fileIdentity(f, after) && before.ModTime().Equal(after.ModTime()) && before.Size() == after.Size()
	if repaired {
		// The repair is now recorded as untrusted, so re-anchor the file: a session
		// that keeps running (the downloader re-fetches every piece after a repair)
		// can be trusted again by the next checkpoint instead of rehashing forever.
		s.mtMu.Lock()
		s.initialInfo[file.path] = after
		s.mtMu.Unlock()
		file.repaired.Store(false)
	}
	if !trustworthy {
		return after, ""
	}
	return after, identity
}

// Keep temporary files in the anchored directory; interrupted writes leave the old
// checkpoint intact. Random O_EXCL names avoid following a pre-created symlink.
func (s *FileStorage) writeResumeAtomically(name string, data []byte, durable bool) error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	temporary := fmt.Sprintf("%s.%x.tmp", name, nonce)
	f, err := rootOpenNoFollow(s.downloadRoot, temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = s.downloadRoot.Remove(temporary) }()
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if durable {
		if err = f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err = f.Close(); err != nil {
		return err
	}
	return replaceResumeFile(s.downloadRoot, temporary, name, durable)
}

package storage

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

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

// LoadResumeState validates every file independently. Older checkpoints remain hints;
// a changed file invalidates only its overlapping pieces, including boundary pieces.
func (s *FileStorage) LoadResumeState(infoHash string) (ResumeState, error) {
	var result ResumeState
	if s.closed.Load() {
		return result, ErrStorageClosed
	}
	name := "." + infoHash + ".state"
	if _, err := s.resolver.ResolveAndValidate(name); err != nil {
		return result, err
	}
	f, err := rootOpenNoFollow(s.downloadRoot, name, resumeMetadataReadFlags, 0)
	if err != nil {
		return result, err
	}
	info, statErr := f.Stat()
	if statErr != nil || !info.Mode().IsRegular() {
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
	if state.Version == 2 && ((state.IdentityMode != "change_time" && state.IdentityMode != "file") || state.PieceLength != s.pieceLength || len(state.Identities) != len(s.files)) {
		return result, fmt.Errorf("incompatible resume layout")
	}
	count := 0
	if s.totalSize > 0 {
		count = int((s.totalSize-1)/s.pieceLength + 1)
	}
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
		// Avoid opening FIFOs/devices while restoring metadata on the startup path.
		var h *os.File
		info, openErr := s.downloadRoot.Lstat(file.path)
		if openErr == nil && info.Mode().IsRegular() {
			h, openErr = rootOpenNoFollow(s.downloadRoot, file.path, resumeMetadataReadFlags, 0)
		}
		matches := false
		if openErr == nil && h != nil {
			fi, statErr := h.Stat()
			if statErr == nil {
				matches = fi.Mode().IsRegular() && fi.Size() == saved.Size && fi.ModTime().UnixNano() == saved.Mtime
				if state.Version == 2 {
					matches = matches && state.Identities[i] != "" && resumeFileIdentity(h, fi, state.IdentityMode) == state.Identities[i]
				}
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
		if recheck[i] || (claimed[i] && !verified[i]) {
			result.Recheck = append(result.Recheck, i)
		} else if claimed[i] {
			result.Verified = append(result.Verified, i)
		}
	}
	return result, nil
}

// SaveResumeState emits a durable checkpoint only when the session is quiescent.
// Active downloads retain cheap hints, avoiding data flushes on the transfer path.
// Sync and serialization run outside the session/storage/dirty-set locks.
func (s *FileStorage) SaveResumeState(infoHash string, verified, unverified []int, durable bool) error {
	if !durable {
		return s.SaveState(infoHash, append(append([]int(nil), verified...), unverified...))
	}
	if s.closed.Load() {
		return ErrStorageClosed
	}
	name := "." + infoHash + ".state"
	if _, err := s.resolver.ResolveAndValidate(name); err != nil {
		return err
	}
	s.mtMu.Lock()
	dirty := s.dirty
	s.dirty = make(map[*fileLayout]struct{})
	expected := make(map[string]os.FileInfo, len(s.stateFileInfo))
	for path, info := range s.stateFileInfo {
		expected[path] = info
	}
	s.mtMu.Unlock()
	saved := false
	defer func() {
		if !saved {
			s.mtMu.Lock()
			for file := range dirty {
				s.dirty[file] = struct{}{}
			}
			s.mtMu.Unlock()
		}
	}()
	mode := s.resumeIdentityMode
	if mode == "" {
		mode = "change_time"
	}
	state := FastResumeState{IdentityMode: mode, Version: 2, PieceLength: s.pieceLength, InfoHashHex: infoHash, VerifiedPieces: verified, CompletedPieces: append(append([]int(nil), verified...), unverified...)}
	for _, file := range s.files {
		f, err := rootOpenNoFollow(s.downloadRoot, file.path, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		before, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}
		identity := fileIdentity(f, before)
		checkpointIdentity := resumeFileIdentity(f, before, mode)
		_, written := dirty[file]
		trustworthy := !file.repaired.Load() && before.Mode().IsRegular() && before.Size() == file.length && os.SameFile(before, file.initialInfo) && (written || sameFileVersion(before, expected[file.path]))
		if err = f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		after, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}
		trustworthy = trustworthy && identity == fileIdentity(f, after) && before.ModTime().Equal(after.ModTime()) && before.Size() == after.Size()
		if err = f.Close(); err != nil {
			return err
		}
		if !trustworthy {
			checkpointIdentity = ""
		}
		state.Identities = append(state.Identities, checkpointIdentity)
		state.Files = append(state.Files, struct {
			Path  string `json:"path"`
			Size  int64  `json:"size"`
			Mtime int64  `json:"mtime"`
		}{file.path, file.length, after.ModTime().UnixNano()})
		s.mtMu.Lock()
		if trustworthy {
			s.stateFileMt[file.path] = after.ModTime().UnixNano()
			s.stateFileInfo[file.path] = after
		}
		s.mtMu.Unlock()
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	err = s.writeResumeAtomically(name, data, true)
	saved = err == nil
	return err
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

func resumeFileIdentity(f *os.File, info os.FileInfo, mode string) string {
	if mode == "file" {
		return fileObjectIdentity(f, info)
	}
	return fileIdentity(f, info)
}

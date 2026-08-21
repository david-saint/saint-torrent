//go:build !windows

package storage

import (
	"crypto/sha1"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

type mappedFile struct {
	layout *fileLayout
	data   []byte
}

// MMapStorage serves torrent content from shared file-backed memory mappings.
type MMapStorage struct {
	*FileStorage
	maps  []*mappedFile
	dirty map[*fileLayout]struct{}
}

var _ Storage = (*MMapStorage)(nil)

// NewMMapStorage creates a memory-mapped file storage backend.
func NewMMapStorage(baseDir string, files []FileInfo, pieceLength int64) (*MMapStorage, error) {
	fs, err := NewFileStorage(baseDir, files, pieceLength)
	if err != nil {
		return nil, err
	}

	st := &MMapStorage{
		FileStorage: fs,
		dirty:       make(map[*fileLayout]struct{}),
	}
	for _, layout := range fs.files {
		st.maps = append(st.maps, &mappedFile{layout: layout})
	}
	return st, nil
}

// ReadBlock reads a block of data from mapped files.
func (s *MMapStorage) ReadBlock(pieceIndex int64, offset int64, buf []byte) (int, error) {
	if pieceIndex < 0 {
		return 0, fmt.Errorf("negative piece index: %d", pieceIndex)
	}
	if offset < 0 {
		return 0, fmt.Errorf("negative offset: %d", offset)
	}

	pieceLen := s.PieceLength(pieceIndex)
	if pieceLen == 0 {
		return 0, fmt.Errorf("piece index out of bounds: %d", pieceIndex)
	}
	if offset+int64(len(buf)) > pieceLen {
		return 0, fmt.Errorf("block exceeds piece boundaries: pieceLen=%d, offset=%d, readLen=%d", pieceLen, offset, len(buf))
	}

	globalStart := pieceIndex*s.pieceLength + offset
	globalEnd := globalStart + int64(len(buf))
	if err := s.ensureMappedRange(globalStart, globalEnd); err != nil {
		return 0, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return 0, ErrStorageClosed
	}

	for _, mapped := range s.maps {
		file := mapped.layout
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)
			fileOffset := overlapStart - file.startOffset
			bufOffset := overlapStart - globalStart
			nBytes := overlapEnd - overlapStart
			copy(buf[bufOffset:bufOffset+nBytes], mapped.data[fileOffset:fileOffset+nBytes])
		}
	}
	return len(buf), nil
}

// WriteBlock writes a block of data into mapped files.
func (s *MMapStorage) WriteBlock(pieceIndex int64, offset int64, data []byte) error {
	if pieceIndex < 0 {
		return fmt.Errorf("negative piece index: %d", pieceIndex)
	}
	if offset < 0 {
		return fmt.Errorf("negative offset: %d", offset)
	}

	pieceLen := s.PieceLength(pieceIndex)
	if pieceLen == 0 {
		return fmt.Errorf("piece index out of bounds: %d", pieceIndex)
	}
	if offset+int64(len(data)) > pieceLen {
		return fmt.Errorf("block exceeds piece boundaries: pieceLen=%d, offset=%d, writeLen=%d", pieceLen, offset, len(data))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrStorageClosed
	}

	repaired := false
	globalStart := pieceIndex*s.pieceLength + offset
	globalEnd := globalStart + int64(len(data))
	for _, mapped := range s.maps {
		file := mapped.layout
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			wasRepaired, err := s.ensureMappedFileLocked(mapped, true)
			if err != nil {
				return err
			}
			repaired = repaired || wasRepaired

			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)
			fileOffset := overlapStart - file.startOffset
			bufOffset := overlapStart - globalStart
			nBytes := overlapEnd - overlapStart
			copy(mapped.data[fileOffset:fileOffset+nBytes], data[bufOffset:bufOffset+nBytes])
			s.dirty[file] = struct{}{}
		}
	}
	if repaired {
		return ErrFileRepaired
	}
	return nil
}

// VerifyPiece computes the SHA-1 hash of a piece from mapped files.
func (s *MMapStorage) VerifyPiece(pieceIndex int64, expectedHash [20]byte) (bool, error) {
	pieceLen := s.PieceLength(pieceIndex)
	if pieceLen == 0 {
		return false, fmt.Errorf("invalid piece index: %d", pieceIndex)
	}

	globalStart := pieceIndex * s.pieceLength
	globalEnd := globalStart + pieceLen
	if err := s.ensureMappedRange(globalStart, globalEnd); err != nil {
		return false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return false, ErrStorageClosed
	}

	h := sha1.New()
	for _, mapped := range s.maps {
		file := mapped.layout
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)
			fileOffset := overlapStart - file.startOffset
			nBytes := overlapEnd - overlapStart
			if _, err := h.Write(mapped.data[fileOffset : fileOffset+nBytes]); err != nil {
				return false, err
			}
		}
	}
	var actualHash [20]byte
	copy(actualHash[:], h.Sum(nil))
	return actualHash == expectedHash, nil
}

// Close flushes and releases mappings. It is idempotent.
func (s *MMapStorage) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	for _, mapped := range s.maps {
		if len(mapped.data) > 0 {
			if err := unix.Msync(mapped.data, unix.MS_SYNC); err != nil && firstErr == nil {
				firstErr = err
			}
			if err := unix.Munmap(mapped.data); err != nil && firstErr == nil {
				firstErr = err
			}
			mapped.data = nil
		}
	}
	for _, file := range s.files {
		file.tryInvalidateReader()
		file.tryInvalidateWriter()
	}
	if s.downloadRoot != nil {
		if err := s.downloadRoot.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *MMapStorage) ensureMappedRange(globalStart, globalEnd int64) error {
	// Fast path: in steady state every file overlapping the range is already mapped,
	// so a shared read lock suffices and reads/verifies run in parallel instead of
	// serializing through the exclusive mapping lock. mapped.data is only ever
	// assigned under the exclusive lock, so reading it under the read lock is safe.
	s.mu.RLock()
	if s.closed.Load() {
		s.mu.RUnlock()
		return ErrStorageClosed
	}
	allMapped := true
	for _, mapped := range s.maps {
		file := mapped.layout
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			if file.length != 0 && len(mapped.data) == 0 {
				allMapped = false
				break
			}
		}
	}
	s.mu.RUnlock()
	if allMapped {
		return nil
	}

	// Slow path: at least one overlapping file still needs its initial mapping.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrStorageClosed
	}
	for _, mapped := range s.maps {
		file := mapped.layout
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			if _, err := s.ensureMappedFileLocked(mapped, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *MMapStorage) ensureMappedFileLocked(mapped *mappedFile, repair bool) (bool, error) {
	layout := mapped.layout
	if layout.length == 0 {
		return false, nil
	}
	wasStale := false
	if len(mapped.data) > 0 {
		if !repair {
			return false, nil
		}
		if s.isMappedFileValidLocked(mapped) {
			return false, nil
		}
		// File on disk was unlinked, replaced, or resized; drop the stale mapping.
		_ = unix.Munmap(mapped.data)
		mapped.data = nil
		layout.invalidateWriter()
		layout.invalidateReader()
		wasStale = true
	}

	data, repaired, err := mapOrRepairFile(layout, repair)
	if err != nil {
		return false, err
	}
	mapped.data = data
	return repaired || wasStale, nil
}

func (s *MMapStorage) isMappedFileValidLocked(mapped *mappedFile) bool {
	layout := mapped.layout
	if err := layout.volumeGuard.validate(); err != nil {
		return false
	}
	pathInfo, err := layout.downloadRoot.Lstat(layout.path)
	if err != nil || pathInfo.Mode()&os.ModeSymlink != 0 || pathInfo.Size() != layout.length {
		return false
	}
	return true
}

func mapOrRepairFile(layout *fileLayout, repair bool) ([]byte, bool, error) {
	if err := layout.volumeGuard.validate(); err != nil {
		return nil, false, err
	}
	f, err := rootOpenNoFollow(layout.downloadRoot, layout.path, os.O_RDWR, 0644)
	repaired := false
	if os.IsNotExist(err) {
		if !repair {
			return nil, false, fmt.Errorf("failed to open file %s for mmap: %w", layout.path, err)
		}
		if err := mkdirAllInRoot(layout.downloadRoot, filepath.Dir(layout.path), 0755); err != nil {
			return nil, false, fmt.Errorf("failed to recreate directories for file %s: %w", layout.path, err)
		}
		f, err = rootOpenNoFollow(layout.downloadRoot, layout.path, os.O_CREATE|os.O_RDWR, 0644)
		repaired = true
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to open file %s for mmap: %w", layout.path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, false, fmt.Errorf("failed to stat file %s for mmap: %w", layout.path, err)
	}
	if fi.Size() != layout.length {
		if !repair {
			_ = f.Close()
			return nil, false, fmt.Errorf("file %s size mismatch for mmap: got %d, want %d", layout.path, fi.Size(), layout.length)
		}
		if err := f.Truncate(layout.length); err != nil {
			_ = f.Close()
			return nil, false, fmt.Errorf("failed to repair size for file %s: %w", layout.path, err)
		}
		repaired = true
	}
	if layout.length == 0 {
		if err := f.Close(); err != nil {
			return nil, false, fmt.Errorf("failed to close file %s after mmap repair: %w", layout.path, err)
		}
		return nil, repaired, nil
	}
	if layout.length > int64(int(^uint(0)>>1)) {
		_ = f.Close()
		return nil, false, fmt.Errorf("file %s is too large to mmap on this platform", layout.path)
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(layout.length), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, false, fmt.Errorf("failed to mmap file %s: %w", layout.path, err)
	}
	if err := f.Close(); err != nil {
		_ = unix.Munmap(data)
		return nil, false, fmt.Errorf("failed to close file %s after mmap: %w", layout.path, err)
	}
	return data, repaired, nil
}

// SaveState refreshes mmap-backed file mtimes once per resume persist rather
// than on every block write, then reuses FileStorage's state serialization.
func (s *MMapStorage) SaveState(infoHashHex string, completedPieces []int) error {
	if s.closed.Load() {
		return ErrStorageClosed
	}

	s.mu.Lock()
	closed := s.closed.Load()
	dirty := s.dirty
	if !closed {
		s.dirty = make(map[*fileLayout]struct{}, len(dirty))
	}
	s.mu.Unlock()

	if closed {
		return ErrStorageClosed
	}

	mtimes := make(map[*fileLayout]int64, len(dirty))
	for file := range dirty {
		mtime, err := touchMappedFile(file)
		if err != nil {
			s.mu.Lock()
			for pending := range dirty {
				s.dirty[pending] = struct{}{}
			}
			s.mu.Unlock()
			return err
		}
		mtimes[file] = mtime
	}
	s.mtMu.Lock()
	for file, mtime := range mtimes {
		s.stateFileMt[file.path] = mtime
	}
	s.mtMu.Unlock()

	return s.FileStorage.SaveState(infoHashHex, completedPieces)
}

func touchMappedFile(file *fileLayout) (int64, error) {
	now := time.Now()
	h, err := rootOpenNoFollow(file.downloadRoot, file.path, os.O_RDONLY, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %s for mtime refresh: %w", file.path, err)
	}
	tv := unix.NsecToTimeval(now.UnixNano())
	touchErr := unix.Futimes(int(h.Fd()), []unix.Timeval{tv, tv})
	fi, statErr := h.Stat()
	closeErr := h.Close()
	if statErr != nil {
		if touchErr != nil {
			return 0, fmt.Errorf("failed to refresh mtime for file %s: futimes: %v; stat: %w", file.path, touchErr, statErr)
		}
		return 0, fmt.Errorf("failed to stat file %s after mtime refresh: %w", file.path, statErr)
	}
	if closeErr != nil {
		return 0, fmt.Errorf("failed to close file %s after mtime refresh: %w", file.path, closeErr)
	}
	return fi.ModTime().UnixNano(), nil
}

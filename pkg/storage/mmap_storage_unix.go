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
	}
	return firstErr
}

func (s *MMapStorage) ensureMappedRange(globalStart, globalEnd int64) error {
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
	if layout.length == 0 || len(mapped.data) > 0 {
		return false, nil
	}

	data, repaired, err := mapOrRepairFile(layout, repair)
	if err != nil {
		return false, err
	}
	mapped.data = data
	return repaired, nil
}

func mapOrRepairFile(layout *fileLayout, repair bool) ([]byte, bool, error) {
	f, err := openNoFollow(layout.absPath, os.O_RDWR, 0644)
	repaired := false
	if os.IsNotExist(err) {
		if !repair {
			return nil, false, fmt.Errorf("failed to open file %s for mmap: %w", layout.path, err)
		}
		if err := os.MkdirAll(filepath.Dir(layout.absPath), 0755); err != nil {
			return nil, false, fmt.Errorf("failed to recreate directories for file %s: %w", layout.path, err)
		}
		f, err = openNoFollow(layout.absPath, os.O_CREATE|os.O_RDWR, 0644)
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
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrStorageClosed
	}
	if err := s.refreshDirtyStateLocked(); err != nil {
		return err
	}
	return s.saveStateLocked(infoHashHex, completedPieces)
}

func (s *MMapStorage) refreshDirtyStateLocked() error {
	for file := range s.dirty {
		if err := s.touchMappedFileLocked(file); err != nil {
			return err
		}
		delete(s.dirty, file)
	}
	return nil
}

func (s *MMapStorage) touchMappedFileLocked(file *fileLayout) error {
	now := time.Now()
	if err := os.Chtimes(file.absPath, now, now); err != nil {
		fi, statErr := os.Stat(file.absPath)
		if statErr != nil {
			return fmt.Errorf("failed to refresh mtime for file %s: chtimes: %v; stat: %w", file.path, err, statErr)
		}
		s.stateFileMt[file.path] = fi.ModTime().UnixNano()
		return nil
	}
	if fi, err := os.Stat(file.absPath); err == nil {
		s.stateFileMt[file.path] = fi.ModTime().UnixNano()
		return nil
	} else {
		return fmt.Errorf("failed to stat file %s after mtime refresh: %w", file.path, err)
	}
}

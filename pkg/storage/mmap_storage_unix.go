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
	file   *os.File
	data   []byte
}

// MMapStorage serves torrent content from shared file-backed memory mappings.
type MMapStorage struct {
	*FileStorage
	maps []*mappedFile
}

var _ Storage = (*MMapStorage)(nil)

// NewMMapStorage creates a memory-mapped file storage backend.
func NewMMapStorage(baseDir string, files []FileInfo, pieceLength int64) (*MMapStorage, error) {
	fs, err := NewFileStorage(baseDir, files, pieceLength)
	if err != nil {
		return nil, err
	}

	st := &MMapStorage{FileStorage: fs}
	for _, layout := range fs.files {
		mapped, err := openMappedFile(layout)
		if err != nil {
			_ = st.Close()
			return nil, err
		}
		st.maps = append(st.maps, mapped)
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

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return 0, ErrStorageClosed
	}

	globalStart := pieceIndex*s.pieceLength + offset
	globalEnd := globalStart + int64(len(buf))
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
			wasRepaired, err := s.ensureMappedFileLocked(mapped)
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
			if err := flushMappedRange(mapped.data); err != nil {
				return fmt.Errorf("failed to flush mapped file %s: %w", file.path, err)
			}
			s.touchMappedFileLocked(file)
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

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return false, ErrStorageClosed
	}

	h := sha1.New()
	globalStart := pieceIndex * s.pieceLength
	globalEnd := globalStart + pieceLen
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

	var firstErr error
	for _, mapped := range s.maps {
		if len(mapped.data) > 0 {
			if err := unix.Msync(mapped.data, unix.MS_SYNC); err != nil && firstErr == nil {
				firstErr = err
			}
			if err := unix.Munmap(mapped.data); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if mapped.file != nil {
			if err := mapped.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	for _, file := range s.files {
		file.tryInvalidateReader()
	}
	return firstErr
}

func openMappedFile(layout *fileLayout) (*mappedFile, error) {
	f, err := openNoFollow(layout.absPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s for mmap: %w", layout.path, err)
	}
	mapped := &mappedFile{layout: layout, file: f}
	if layout.length == 0 {
		return mapped, nil
	}
	if layout.length > int64(int(^uint(0)>>1)) {
		_ = f.Close()
		return nil, fmt.Errorf("file %s is too large to mmap on this platform", layout.path)
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(layout.length), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to mmap file %s: %w", layout.path, err)
	}
	mapped.data = data
	return mapped, nil
}

func (s *MMapStorage) ensureMappedFileLocked(mapped *mappedFile) (bool, error) {
	layout := mapped.layout
	if layout.length == 0 {
		return false, nil
	}

	fi, err := os.Stat(layout.absPath)
	if err == nil && fi.Size() == layout.length {
		return false, nil
	}
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("failed to stat file %s for mmap write: %w", layout.path, err)
	}

	if len(mapped.data) > 0 {
		_ = unix.Munmap(mapped.data)
		mapped.data = nil
	}
	if mapped.file != nil {
		_ = mapped.file.Close()
		mapped.file = nil
	}

	if err := os.MkdirAll(filepath.Dir(layout.absPath), 0755); err != nil {
		return false, fmt.Errorf("failed to recreate directories for file %s: %w", layout.path, err)
	}
	f, err := openNoFollow(layout.absPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return false, fmt.Errorf("failed to open/create file %s for mmap repair: %w", layout.path, err)
	}
	if err := f.Truncate(layout.length); err != nil {
		_ = f.Close()
		return false, fmt.Errorf("failed to repair size for file %s: %w", layout.path, err)
	}
	mapped.file = f
	data, err := unix.Mmap(int(f.Fd()), 0, int(layout.length), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		mapped.file = nil
		return false, fmt.Errorf("failed to remap file %s after repair: %w", layout.path, err)
	}
	mapped.data = data
	return true, nil
}

func flushMappedRange(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return unix.Msync(data, unix.MS_SYNC)
}

func (s *MMapStorage) touchMappedFileLocked(file *fileLayout) {
	now := time.Now()
	if err := os.Chtimes(file.absPath, now, now); err != nil {
		s.stateFileMt[file.path] = now.UnixNano()
		return
	}
	if fi, err := os.Stat(file.absPath); err == nil {
		s.stateFileMt[file.path] = fi.ModTime().UnixNano()
		return
	}
	s.stateFileMt[file.path] = now.UnixNano()
}

package storage

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MemStorage keeps torrent content and fast-resume state in memory.
type MemStorage struct {
	mu          sync.RWMutex
	baseDir     string
	files       []*fileLayout
	pieceLength int64
	totalSize   int64
	data        []byte
	stateFileMt map[string]int64
	states      map[string][]byte
	closed      atomic.Bool
}

var _ Storage = (*MemStorage)(nil)

// NewMemStorage creates an in-memory storage backend.
func NewMemStorage(baseDir string, files []FileInfo, pieceLength int64) (*MemStorage, error) {
	if pieceLength <= 0 {
		return nil, fmt.Errorf("piece length must be positive, got %d", pieceLength)
	}

	resolvedBase := baseDir
	if resolvedBase != "" {
		abs, err := filepath.Abs(resolvedBase)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path of base directory: %w", err)
		}
		resolvedBase = filepath.Clean(abs)
	}

	var layouts []*fileLayout
	var currentOffset int64
	seenPaths := make(map[string]bool)
	stateFileMt := make(map[string]int64, len(files))
	now := time.Now().UnixNano()

	for _, file := range files {
		if file.Length < 0 {
			return nil, fmt.Errorf("file length cannot be negative: %s has length %d", file.Path, file.Length)
		}
		if file.Path == "" {
			return nil, fmt.Errorf("file path cannot be empty")
		}

		cleanPath := filepath.Clean(file.Path)
		if filepath.IsAbs(cleanPath) || cleanPath == ".." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("unsafe file path detected (directory traversal attempt): %s", file.Path)
		}

		topComponent := cleanPath
		if i := strings.IndexRune(topComponent, filepath.Separator); i >= 0 {
			topComponent = topComponent[:i]
		}
		if isReservedStorageName(topComponent) {
			return nil, fmt.Errorf("file path uses reserved internal name %q: %s", topComponent, file.Path)
		}
		if currentOffset > math.MaxInt64-file.Length {
			return nil, fmt.Errorf("total file length overflows int64")
		}

		lowerPath := strings.ToLower(cleanPath)
		if seenPaths[lowerPath] {
			return nil, fmt.Errorf("duplicate file path detected: %s", file.Path)
		}
		seenPaths[lowerPath] = true

		layout := &fileLayout{
			path:        file.Path,
			length:      file.Length,
			startOffset: currentOffset,
			endOffset:   currentOffset + file.Length,
		}
		layouts = append(layouts, layout)
		currentOffset += file.Length
		stateFileMt[file.Path] = now
	}

	if currentOffset > int64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("total file length overflows addressable memory")
	}

	return &MemStorage{
		baseDir:     resolvedBase,
		files:       layouts,
		pieceLength: pieceLength,
		totalSize:   currentOffset,
		data:        make([]byte, int(currentOffset)),
		stateFileMt: stateFileMt,
		states:      make(map[string][]byte),
	}, nil
}

// BaseDir returns the configured base directory.
func (s *MemStorage) BaseDir() string {
	return s.baseDir
}

// TotalSize returns the total size of all files in the torrent.
func (s *MemStorage) TotalSize() int64 {
	return s.totalSize
}

// PieceLengthValue returns the standard piece length.
func (s *MemStorage) PieceLengthValue() int64 {
	return s.pieceLength
}

// PieceLength returns the length of a piece at pieceIndex.
func (s *MemStorage) PieceLength(pieceIndex int64) int64 {
	if pieceIndex < 0 {
		return 0
	}
	pieceStart := pieceIndex * s.pieceLength
	if pieceStart >= s.totalSize {
		return 0
	}
	pieceEnd := pieceStart + s.pieceLength
	if pieceEnd > s.totalSize {
		pieceEnd = s.totalSize
	}
	return pieceEnd - pieceStart
}

// ReadBlock reads a block of data from memory.
func (s *MemStorage) ReadBlock(pieceIndex int64, offset int64, buf []byte) (int, error) {
	start, end, err := s.blockRange(pieceIndex, offset, len(buf))
	if err != nil {
		return 0, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return 0, ErrStorageClosed
	}
	copy(buf, s.data[start:end])
	return len(buf), nil
}

// WriteBlock writes a block of data to memory.
func (s *MemStorage) WriteBlock(pieceIndex int64, offset int64, data []byte) error {
	start, end, err := s.blockRange(pieceIndex, offset, len(data))
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrStorageClosed
	}
	copy(s.data[start:end], data)
	s.touchOverlappingLocked(start, end)
	return nil
}

// VerifyPiece computes the SHA-1 hash of the piece and compares it with expectedHash.
func (s *MemStorage) VerifyPiece(pieceIndex int64, expectedHash [20]byte) (bool, error) {
	start, end, err := s.blockRange(pieceIndex, 0, int(s.PieceLength(pieceIndex)))
	if err != nil {
		return false, fmt.Errorf("invalid piece index: %d", pieceIndex)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return false, ErrStorageClosed
	}
	actualHash := sha1.Sum(s.data[start:end])
	return actualHash == expectedHash, nil
}

// Close marks storage closed. It is idempotent.
func (s *MemStorage) Close() error {
	s.closed.Store(true)
	return nil
}

// SaveState stores the completed pieces and file metadata in memory.
func (s *MemStorage) SaveState(infoHashHex string, completedPieces []int) error {
	if s.closed.Load() {
		return ErrStorageClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrStorageClosed
	}

	state := FastResumeState{
		InfoHashHex:     infoHashHex,
		CompletedPieces: append([]int(nil), completedPieces...),
	}
	for _, f := range s.files {
		state.Files = append(state.Files, struct {
			Path  string `json:"path"`
			Size  int64  `json:"size"`
			Mtime int64  `json:"mtime"`
		}{
			Path:  f.path,
			Size:  f.length,
			Mtime: s.stateMtimeLocked(f.path, 0),
		})
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	s.states[infoHashHex] = append([]byte(nil), data...)
	return nil
}

func (s *MemStorage) stateMtimeLocked(path string, fallback int64) int64 {
	if mt, ok := s.stateFileMt[path]; ok {
		return mt
	}
	return fallback
}

// LoadState reads and validates an in-memory fast-resume state.
func (s *MemStorage) LoadState(infoHashHex string) ([]int, error) {
	if s.closed.Load() {
		return nil, ErrStorageClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, ErrStorageClosed
	}

	data, ok := s.states[infoHashHex]
	if !ok {
		return nil, fmt.Errorf("state not found: %w", os.ErrNotExist)
	}

	var state FastResumeState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.InfoHashHex != infoHashHex {
		return nil, fmt.Errorf("info hash mismatch")
	}
	if len(state.Files) != len(s.files) {
		return nil, fmt.Errorf("file count mismatch")
	}
	for i, f := range s.files {
		savedFile := state.Files[i]
		if savedFile.Path != f.path {
			return nil, fmt.Errorf("file path mismatch at index %d", i)
		}
		if savedFile.Size != f.length || savedFile.Mtime != s.stateMtimeLocked(f.path, 0) {
			return nil, fmt.Errorf("file modification mismatch for %s", f.path)
		}
	}
	return append([]int(nil), state.CompletedPieces...), nil
}

func (s *MemStorage) blockRange(pieceIndex int64, offset int64, length int) (int64, int64, error) {
	if pieceIndex < 0 {
		return 0, 0, fmt.Errorf("negative piece index: %d", pieceIndex)
	}
	if offset < 0 {
		return 0, 0, fmt.Errorf("negative offset: %d", offset)
	}

	pieceLen := s.PieceLength(pieceIndex)
	if pieceLen == 0 {
		return 0, 0, fmt.Errorf("piece index out of bounds: %d", pieceIndex)
	}
	if int64(length) > pieceLen-offset {
		return 0, 0, fmt.Errorf("block exceeds piece boundaries: pieceLen=%d, offset=%d, length=%d", pieceLen, offset, length)
	}
	start := pieceIndex*s.pieceLength + offset
	end := start + int64(length)
	return start, end, nil
}

func (s *MemStorage) touchOverlappingLocked(globalStart, globalEnd int64) {
	now := time.Now().UnixNano()
	for _, file := range s.files {
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			s.stateFileMt[file.path] = now
		}
	}
}

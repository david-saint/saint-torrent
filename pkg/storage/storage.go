package storage

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// FileInfo represents a file in the torrent.
type FileInfo struct {
	Path   string // Relative path from the base directory
	Length int64  // Size of the file in bytes
}

type fileLayout struct {
	path        string
	length      int64
	startOffset int64
	endOffset   int64
}

// Storage manages the files on disk for a torrent and provides thread-safe
// block read/write and piece verification.
type Storage struct {
	mu          sync.Mutex
	baseDir     string
	files       []fileLayout
	pieceLength int64
	totalSize   int64
	stateFileMt map[string]int64
}

// NewStorage creates the target directories and pre-allocates files to their
// respective sizes.
func NewStorage(baseDir string, files []FileInfo, pieceLength int64) (*Storage, error) {
	if pieceLength <= 0 {
		return nil, fmt.Errorf("piece length must be positive, got %d", pieceLength)
	}

	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	var layouts []fileLayout
	var currentOffset int64
	seenPaths := make(map[string]bool)
	stateFileMt := make(map[string]int64, len(files))

	for _, file := range files {
		if file.Length < 0 {
			return nil, fmt.Errorf("file length cannot be negative: %s has length %d", file.Path, file.Length)
		}
		if file.Path == "" {
			return nil, fmt.Errorf("file path cannot be empty")
		}
		if currentOffset > math.MaxInt64-file.Length {
			return nil, fmt.Errorf("total file length overflows int64")
		}

		lowerPath := strings.ToLower(filepath.Clean(file.Path))
		if seenPaths[lowerPath] {
			return nil, fmt.Errorf("duplicate file path detected: %s", file.Path)
		}
		seenPaths[lowerPath] = true

		layout := fileLayout{
			path:        file.Path,
			length:      file.Length,
			startOffset: currentOffset,
			endOffset:   currentOffset + file.Length,
		}
		layouts = append(layouts, layout)
		currentOffset += file.Length

		// Construct absolute path and verify containment / no symlinks
		absPath, err := ResolveAndValidatePath(baseDir, file.Path)
		if err != nil {
			return nil, err
		}

		parentDir := filepath.Dir(absPath)
		if err := os.MkdirAll(parentDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directories for file %s: %w", file.Path, err)
		}

		// Open/Create the file and set its size without following a final symlink.
		f, err := openNoFollow(absPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open/create file %s: %w", file.Path, err)
		}

		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to stat file %s: %w", file.Path, err)
		}
		if fi.Size() != file.Length {
			if err := f.Truncate(file.Length); err != nil {
				f.Close()
				return nil, fmt.Errorf("failed to pre-allocate size for file %s: %w", file.Path, err)
			}
			fi, err = f.Stat()
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("failed to stat file %s after resize: %w", file.Path, err)
			}
		}
		stateFileMt[file.Path] = fi.ModTime().UnixNano()
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("failed to close file %s: %w", file.Path, err)
		}
	}

	return &Storage{
		baseDir:     baseDir,
		files:       layouts,
		pieceLength: pieceLength,
		totalSize:   currentOffset,
		stateFileMt: stateFileMt,
	}, nil
}

// BaseDir returns the base directory of the storage.
func (s *Storage) BaseDir() string {
	return s.baseDir
}

// TotalSize returns the total size of all files in the torrent.
func (s *Storage) TotalSize() int64 {
	return s.totalSize
}

// PieceLengthValue returns the standard piece length (not the actual length of a specific piece).
func (s *Storage) PieceLengthValue() int64 {
	return s.pieceLength
}

// PieceLength returns the length of a piece at pieceIndex.
// All pieces except the last are equal to the standard pieceLength.
func (s *Storage) PieceLength(pieceIndex int64) int64 {
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

// ReadBlock reads a block of data from the storage.
// It returns the number of bytes read, or an error.
func (s *Storage) ReadBlock(pieceIndex int64, offset int64, buf []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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

	for _, file := range s.files {
		// Check overlap between [globalStart, globalEnd) and [file.startOffset, file.endOffset)
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)

			fileOffset := overlapStart - file.startOffset
			bufOffset := overlapStart - globalStart
			nBytes := overlapEnd - overlapStart

			absPath, err := ResolveAndValidatePath(s.baseDir, file.path)
			if err != nil {
				return 0, err
			}
			f, err := openNoFollow(absPath, os.O_RDONLY, 0)
			if err != nil {
				return 0, fmt.Errorf("failed to open file %s for reading: %w", file.path, err)
			}

			n, err := f.ReadAt(buf[bufOffset:bufOffset+nBytes], fileOffset)
			f.Close()
			if err != nil && err != io.EOF {
				return 0, fmt.Errorf("read error on file %s: %w", file.path, err)
			}
			if int64(n) != nBytes {
				return 0, fmt.Errorf("short read on file %s: expected %d bytes, got %d", file.path, nBytes, n)
			}
		}
	}

	return len(buf), nil
}

// WriteBlock writes a block of data to the storage, spanning across files if necessary.
func (s *Storage) WriteBlock(pieceIndex int64, offset int64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

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

	globalStart := pieceIndex*s.pieceLength + offset
	globalEnd := globalStart + int64(len(data))

	for _, file := range s.files {
		// Check overlap between [globalStart, globalEnd) and [file.startOffset, file.endOffset)
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)

			fileOffset := overlapStart - file.startOffset
			bufOffset := overlapStart - globalStart
			nBytes := overlapEnd - overlapStart

			absPath, err := ResolveAndValidatePath(s.baseDir, file.path)
			if err != nil {
				return err
			}
			f, err := openNoFollow(absPath, os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("failed to open file %s for writing: %w", file.path, err)
			}

			n, err := f.WriteAt(data[bufOffset:bufOffset+nBytes], fileOffset)
			f.Close()
			if err != nil {
				return fmt.Errorf("write error on file %s: %w", file.path, err)
			}
			if int64(n) != nBytes {
				return fmt.Errorf("short write on file %s: expected %d bytes, got %d", file.path, nBytes, n)
			}
			if fi, err := os.Stat(absPath); err == nil {
				s.stateFileMt[file.path] = fi.ModTime().UnixNano()
			}
		}
	}

	return nil
}

// VerifyPiece computes the SHA-1 hash of the piece and compares it with expectedHash.
func (s *Storage) VerifyPiece(pieceIndex int64, expectedHash [20]byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pieceLen := s.PieceLength(pieceIndex)
	if pieceLen == 0 {
		return false, fmt.Errorf("invalid piece index: %d", pieceIndex)
	}

	buf := make([]byte, pieceLen)
	globalStart := pieceIndex * s.pieceLength
	globalEnd := globalStart + pieceLen

	for _, file := range s.files {
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)

			fileOffset := overlapStart - file.startOffset
			bufOffset := overlapStart - globalStart
			nBytes := overlapEnd - overlapStart

			absPath, err := ResolveAndValidatePath(s.baseDir, file.path)
			if err != nil {
				return false, err
			}
			f, err := openNoFollow(absPath, os.O_RDONLY, 0)
			if err != nil {
				return false, fmt.Errorf("failed to open file %s for reading: %w", file.path, err)
			}

			n, err := f.ReadAt(buf[bufOffset:bufOffset+nBytes], fileOffset)
			f.Close()
			if err != nil && err != io.EOF {
				return false, fmt.Errorf("read error on file %s: %w", file.path, err)
			}
			if int64(n) != nBytes {
				return false, fmt.Errorf("short read on file %s: expected %d bytes, got %d", file.path, nBytes, n)
			}
		}
	}

	actualHash := sha1.Sum(buf)
	return actualHash == expectedHash, nil
}

// Close closes the storage. For this implementation it does not maintain open file handles,
// but it is provided to satisfy any lifecycle interface.
func (s *Storage) Close() error {
	return nil
}

// FastResumeState defines the structure for saving completed pieces metadata
type FastResumeState struct {
	InfoHashHex string `json:"info_hash_hex"`
	Files       []struct {
		Path  string `json:"path"`
		Size  int64  `json:"size"`
		Mtime int64  `json:"mtime"`
	} `json:"files"`
	CompletedPieces []int `json:"completed_pieces"`
}

// SaveState writes the completed pieces and file metadata to a fast-resume state file.
func (s *Storage) SaveState(infoHashHex string, completedPieces []int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	statePath, err := ResolveAndValidatePath(s.baseDir, "."+infoHashHex+".state")
	if err != nil {
		return err
	}
	state := FastResumeState{
		InfoHashHex:     infoHashHex,
		CompletedPieces: completedPieces,
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

	return os.WriteFile(statePath, data, 0644)
}

func (s *Storage) stateMtimeLocked(path string, fallback int64) int64 {
	if mt, ok := s.stateFileMt[path]; ok {
		return mt
	}
	return fallback
}

// LoadState reads and validates the fast-resume state file, returning completed piece indices if valid.
func (s *Storage) LoadState(infoHashHex string) ([]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	statePath, err := ResolveAndValidatePath(s.baseDir, "."+infoHashHex+".state")
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(statePath)
	if err != nil {
		return nil, err
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

		absPath, err := ResolveAndValidatePath(s.baseDir, f.path)
		if err != nil {
			return nil, err
		}
		fi, err := os.Stat(absPath)
		if err != nil {
			return nil, fmt.Errorf("file stat error: %w", err)
		}

		if fi.Size() != savedFile.Size || fi.ModTime().UnixNano() != savedFile.Mtime {
			return nil, fmt.Errorf("file modification mismatch for %s", f.path)
		}
	}
	for i, f := range s.files {
		s.stateFileMt[f.path] = state.Files[i].Mtime
	}

	return state.CompletedPieces, nil
}

// ResolveAndValidatePath canonicalizes the base directory and validates the relative
// path component by component to ensure it is contained and does not traverse any symlinks.
func ResolveAndValidatePath(baseDir, relPath string) (string, error) {
	canonicalBase, err := filepath.EvalSymlinks(baseDir)
	if err != nil {
		canonicalBase, err = filepath.Abs(baseDir)
		if err != nil {
			return "", fmt.Errorf("failed to get absolute path of base directory: %w", err)
		}
	}
	canonicalBase = filepath.Clean(canonicalBase)

	absPath := filepath.Clean(filepath.Join(canonicalBase, relPath))
	rel, err := filepath.Rel(canonicalBase, absPath)
	if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("unsafe file path detected (directory traversal attempt): %s", relPath)
	}

	// Verify that no component of the path is a symlink
	current := canonicalBase
	components := strings.Split(rel, string(filepath.Separator))
	for _, comp := range components {
		if comp == "" || comp == "." || comp == ".." {
			continue
		}
		current = filepath.Join(current, comp)
		fi, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				// Path component doesn't exist, which is fine (we will create it), but we must ensure we don't follow any symlinks.
				continue
			}
			return "", err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("symlink detected in path component: %s", current)
		}
	}

	return absPath, nil
}

func openNoFollow(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag|syscall.O_NOFOLLOW, perm)
}

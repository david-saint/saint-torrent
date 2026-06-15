// Package storage handles thread-safe read/write operations and verification
// of torrent files on disk.
package storage

import (
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// ErrFileRepaired is returned when a write had to recreate or resize a target
// file that disappeared or changed after the session started. The write itself
// has succeeded, but callers should re-verify completed pieces.
var ErrFileRepaired = errors.New("storage file repaired")

// ErrStorageClosed is returned by block operations after Close has released the
// storage's cached file handles.
var ErrStorageClosed = errors.New("storage is closed")

// FileInfo represents a file in the torrent.
type FileInfo struct {
	Path   string // Relative path from the base directory
	Length int64  // Size of the file in bytes
}

// fileLayout holds a file's byte range within the torrent plus a lazily-opened,
// cached read handle. The absolute path is validated once at construction (see
// NewStorage) so the hot read path never re-walks/EvalSymlinks the path.
type fileLayout struct {
	path        string // relative path (torrent-declared)
	absPath     string // validated absolute path, resolved once at construction
	length      int64
	startOffset int64
	endOffset   int64

	// readHandle is an O_RDONLY handle opened on first read and reused for every
	// subsequent block read of this file — eliminating the open/close syscall pair
	// per 16 KB block on the seed path. Guarded by rmu. Invalidated when a write
	// recreates/resizes the file so a stale handle to an orphaned inode is dropped.
	rmu        sync.Mutex
	readHandle *os.File
}

// reader returns the cached O_RDONLY handle, opening it on first use. Callers must
// hold the Storage read lock so the handle cannot be closed underneath them.
func (f *fileLayout) reader() (*os.File, error) {
	f.rmu.Lock()
	defer f.rmu.Unlock()
	if f.readHandle != nil {
		return f.readHandle, nil
	}
	h, err := openNoFollow(f.absPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	f.readHandle = h
	return h, nil
}

// invalidateReader closes and drops the cached read handle. Called under the
// Storage write lock (repair/close), so no concurrent reader is using it.
func (f *fileLayout) invalidateReader() {
	f.rmu.Lock()
	if f.readHandle != nil {
		_ = f.readHandle.Close()
		f.readHandle = nil
	}
	f.rmu.Unlock()
}

// Storage manages the files on disk for a torrent and provides thread-safe
// block read/write and piece verification.
//
// Concurrency model: mu is an RWMutex. Block reads (ReadBlock/VerifyPiece) take
// the read lock and run concurrently — positional ReadAt on a cached handle is
// safe for parallel use, so the seed path is no longer serialized through one
// mutex. Writes, repair, fast-resume state, and Close take the write lock; they
// are infrequent (per piece, not per block) and run on the background write pool,
// so they never block peer goroutines.
type Storage struct {
	mu          sync.RWMutex
	resolver    *PathResolver
	baseDir     string
	files       []*fileLayout
	pieceLength int64
	totalSize   int64
	stateFileMt map[string]int64
	closed      bool
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

	// Resolve the base directory once; every per-file path is validated against
	// this single canonical base instead of re-running EvalSymlinks per file.
	resolver, err := NewPathResolver(baseDir)
	if err != nil {
		return nil, err
	}

	var layouts []*fileLayout
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
		// Reject torrent-declared paths whose top-level component collides with an
		// internal file we keep alongside the content in the download dir: the DHT
		// routing table (.dht_nodes) and the per-torrent fast-resume state
		// (.<infohash>.state). Without this, a single-file torrent named ".dht_nodes"
		// could overwrite the routing table with attacker-chosen bytes (DHT poisoning
		// on next launch), or a ".<hash>.state" file could clobber resume data. Nested
		// uses (e.g. "movie/.dht_nodes") never reach the root, so they stay allowed.
		topComponent := filepath.Clean(file.Path)
		if i := strings.IndexRune(topComponent, filepath.Separator); i >= 0 {
			topComponent = topComponent[:i]
		}
		if isReservedStorageName(topComponent) {
			return nil, fmt.Errorf("file path uses reserved internal name %q: %s", topComponent, file.Path)
		}
		if currentOffset > math.MaxInt64-file.Length {
			return nil, fmt.Errorf("total file length overflows int64")
		}

		lowerPath := strings.ToLower(filepath.Clean(file.Path))
		if seenPaths[lowerPath] {
			return nil, fmt.Errorf("duplicate file path detected: %s", file.Path)
		}
		seenPaths[lowerPath] = true

		// Construct absolute path and verify containment / no symlinks. Validated
		// here once and cached on the layout for the lifetime of the storage.
		absPath, err := resolver.ResolveAndValidate(file.Path)
		if err != nil {
			return nil, err
		}

		layout := &fileLayout{
			path:        file.Path,
			absPath:     absPath,
			length:      file.Length,
			startOffset: currentOffset,
			endOffset:   currentOffset + file.Length,
		}
		layouts = append(layouts, layout)
		currentOffset += file.Length

		parentDir := filepath.Dir(absPath)
		if err := os.MkdirAll(parentDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directories for file %s: %w", file.Path, err)
		}

		// Open/Create the file and set its size without following a final symlink.
		// We do not retain this handle: read handles are cached lazily on first read
		// (see fileLayout.reader) and writes open on demand, which keeps construction
		// cheap and lets tests that swap a file for a FIFO still block on first access.
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
		resolver:    resolver,
		baseDir:     resolver.BaseDir(),
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

	// Read lock only: positional ReadAt on the cached handles is concurrency-safe,
	// so many block reads (the hot seed path) run in parallel without serializing.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, ErrStorageClosed
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

			f, err := file.reader()
			if err != nil {
				return 0, fmt.Errorf("failed to open file %s for reading: %w", file.path, err)
			}

			n, err := f.ReadAt(buf[bufOffset:bufOffset+nBytes], fileOffset)
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

	// Writes take the exclusive lock. They happen once per completed piece (on the
	// background write pool, never on a peer's read loop), so serializing them — and
	// briefly excluding concurrent reads — does not throttle the per-block seed path.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStorageClosed
	}
	repaired := false

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

			absPath := file.absPath
			f, err := openNoFollow(absPath, os.O_WRONLY, 0644)
			if os.IsNotExist(err) {
				if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
					return fmt.Errorf("failed to recreate directories for file %s: %w", file.path, err)
				}
				f, err = openNoFollow(absPath, os.O_CREATE|os.O_RDWR, 0644)
				if err == nil {
					repaired = true
					// The file was recreated as a fresh inode; any cached read handle
					// now points at the orphaned old inode and must be dropped.
					file.invalidateReader()
				}
			}
			if err != nil {
				return fmt.Errorf("failed to open file %s for writing: %w", file.path, err)
			}
			if fi, err := f.Stat(); err != nil {
				f.Close()
				return fmt.Errorf("failed to stat file %s for writing: %w", file.path, err)
			} else if fi.Size() != file.length {
				if err := f.Truncate(file.length); err != nil {
					f.Close()
					return fmt.Errorf("failed to repair size for file %s: %w", file.path, err)
				}
				repaired = true
				file.invalidateReader()
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

	if repaired {
		return ErrFileRepaired
	}
	return nil
}

// VerifyPiece computes the SHA-1 hash of the piece and compares it with expectedHash.
func (s *Storage) VerifyPiece(pieceIndex int64, expectedHash [20]byte) (bool, error) {
	pieceLen := s.PieceLength(pieceIndex)
	if pieceLen == 0 {
		return false, fmt.Errorf("invalid piece index: %d", pieceIndex)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, ErrStorageClosed
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

			f, err := file.reader()
			if err != nil {
				return false, fmt.Errorf("failed to open file %s for reading: %w", file.path, err)
			}

			n, err := f.ReadAt(buf[bufOffset:bufOffset+nBytes], fileOffset)
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

// Close releases the storage's cached file handles. It is idempotent. After Close,
// block operations return ErrStorageClosed.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for _, file := range s.files {
		file.invalidateReader()
	}
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

	statePath, err := s.resolver.ResolveAndValidate("." + infoHashHex + ".state")
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

	statePath, err := s.resolver.ResolveAndValidate("." + infoHashHex + ".state")
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

		fi, err := os.Stat(f.absPath)
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

// PathResolver validates many paths against one canonical base directory without
// repeating the base-directory filesystem lookup for every path.
type PathResolver struct {
	canonicalBase string
}

// NewPathResolver canonicalizes baseDir for repeated path validation.
func NewPathResolver(baseDir string) (*PathResolver, error) {
	canonicalBase, err := filepath.EvalSymlinks(baseDir)
	if err != nil {
		canonicalBase, err = filepath.Abs(baseDir)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path of base directory: %w", err)
		}
	}
	return &PathResolver{canonicalBase: filepath.Clean(canonicalBase)}, nil
}

// BaseDir returns the canonical base directory used by the resolver.
func (r *PathResolver) BaseDir() string {
	return r.canonicalBase
}

// ResolveAndValidate validates relPath component by component to ensure it is
// contained by the base directory and does not traverse any symlinks.
func (r *PathResolver) ResolveAndValidate(relPath string) (string, error) {
	absPath := filepath.Clean(filepath.Join(r.canonicalBase, relPath))
	rel, err := filepath.Rel(r.canonicalBase, absPath)
	if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("unsafe file path detected (directory traversal attempt): %s", relPath)
	}

	// Verify that no component of the path is a symlink
	current := r.canonicalBase
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

// ResolveAndValidatePath canonicalizes the base directory and validates the relative
// path component by component to ensure it is contained and does not traverse any symlinks.
func ResolveAndValidatePath(baseDir, relPath string) (string, error) {
	resolver, err := NewPathResolver(baseDir)
	if err != nil {
		return "", err
	}
	return resolver.ResolveAndValidate(relPath)
}

func openNoFollow(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag|syscall.O_NOFOLLOW, perm)
}

// isReservedStorageName reports whether a top-level path component would collide
// with one of the internal files saintTorrent stores in the download directory.
// These names are produced only by our own code (dht.saveNodes writes ".dht_nodes";
// SaveState writes ".<infohash>.state"), so torrent content must never be allowed
// to claim them. The check mirrors those literal names rather than importing them,
// to avoid a storage -> dht import cycle.
func isReservedStorageName(name string) bool {
	if name == ".dht_nodes" {
		return true
	}
	// Per-torrent fast-resume files: a leading dot plus a ".state" suffix.
	return strings.HasPrefix(name, ".") && strings.HasSuffix(name, ".state")
}

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
	"sync/atomic"
)

// ErrFileRepaired is returned when a write had to recreate or resize a target
// file that disappeared or changed after the session started. The write itself
// has succeeded, but callers should re-verify completed pieces.
var ErrFileRepaired = errors.New("storage file repaired")

// ErrStorageClosed is returned by block operations after Close has marked the
// storage unavailable and released cached file handles.
var ErrStorageClosed = errors.New("storage is closed")

// FileInfo represents a file in the torrent.
type FileInfo struct {
	Path   string // Relative path from the base directory
	Length int64  // Size of the file in bytes
}

// Storage is the pluggable torrent storage contract used by the downloader.
type Storage interface {
	BaseDir() string
	TotalSize() int64
	PieceLengthValue() int64
	PieceLength(pieceIndex int64) int64
	ReadBlock(pieceIndex int64, offset int64, buf []byte) (int, error)
	WriteBlock(pieceIndex int64, offset int64, data []byte) error
	VerifyPiece(pieceIndex int64, expectedHash [20]byte) (bool, error)
	SaveState(infoHashHex string, completedPieces []int) error
	LoadState(infoHashHex string) ([]int, error)
	Close() error
}

// Factory creates a Storage backend for a torrent.
type Factory func(baseDir string, files []FileInfo, pieceLength int64) (Storage, error)

// Backend identifies the built-in storage implementations.
type Backend string

const (
	BackendFile   Backend = "file"
	BackendMMap   Backend = "mmap"
	BackendMemory Backend = "mem"
)

// ParseBackend parses a storage backend name.
func ParseBackend(name string) (Backend, error) {
	switch Backend(strings.ToLower(strings.TrimSpace(name))) {
	case "", BackendFile:
		return BackendFile, nil
	case BackendMMap:
		return BackendMMap, nil
	case BackendMemory, "memory":
		return BackendMemory, nil
	default:
		return "", fmt.Errorf("unknown storage backend %q", name)
	}
}

// FactoryForBackend returns a constructor for a built-in storage backend.
func FactoryForBackend(backend Backend) (Factory, error) {
	switch backend {
	case BackendFile:
		return func(baseDir string, files []FileInfo, pieceLength int64) (Storage, error) {
			return NewFileStorage(baseDir, files, pieceLength)
		}, nil
	case BackendMMap:
		return mmapFactory()
	case BackendMemory:
		return func(baseDir string, files []FileInfo, pieceLength int64) (Storage, error) {
			return NewMemStorage(baseDir, files, pieceLength)
		}, nil
	default:
		return nil, fmt.Errorf("unknown storage backend %q", backend)
	}
}

// NewStorageWithBackend creates storage using one of the built-in backends.
func NewStorageWithBackend(backend Backend, baseDir string, files []FileInfo, pieceLength int64) (Storage, error) {
	factory, err := FactoryForBackend(backend)
	if err != nil {
		return nil, err
	}
	return factory(baseDir, files, pieceLength)
}

// NewStorage creates the default file-backed storage.
func NewStorage(baseDir string, files []FileInfo, pieceLength int64) (Storage, error) {
	return NewFileStorage(baseDir, files, pieceLength)
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

	// writeHandle is an O_RDWR handle opened (and, if the file vanished or was
	// resized, repaired) on first write and reused for every subsequent block write
	// of this file — the write-side analogue of readHandle, eliminating the
	// open/stat/close syscall churn per completed piece on the download path. Guarded
	// by wmu. Invalidated when a repair recreates/resizes the file or on Close.
	wmu         sync.Mutex
	writeHandle *os.File
}

// reader returns the cached O_RDONLY handle, opening it on first use.
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

// invalidateReader closes and drops the cached read handle.
func (f *fileLayout) invalidateReader() {
	f.rmu.Lock()
	f.invalidateReaderLocked()
	f.rmu.Unlock()
}

func (f *fileLayout) tryInvalidateReader() {
	if !f.rmu.TryLock() {
		return
	}
	f.invalidateReaderLocked()
	f.rmu.Unlock()
}

func (f *fileLayout) invalidateReaderLocked() {
	if f.readHandle != nil {
		_ = f.readHandle.Close()
		f.readHandle = nil
	}
}

// writer returns the cached O_RDWR handle, opening it on first use. The open
// doubles as the repair check: if the file vanished it is recreated, and if its
// size drifted it is truncated back to the declared length — either case reports
// repaired=true and drops the now-stale read handle. Once cached, subsequent
// writes reuse the handle, so the open/stat/close syscall churn is paid once per
// file rather than once per completed piece. Guarded by wmu.
func (f *fileLayout) writer() (h *os.File, repaired bool, err error) {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	if f.writeHandle != nil {
		return f.writeHandle, false, nil
	}

	h, err = openNoFollow(f.absPath, os.O_RDWR, 0644)
	if os.IsNotExist(err) {
		if mkErr := os.MkdirAll(filepath.Dir(f.absPath), 0755); mkErr != nil {
			return nil, false, mkErr
		}
		h, err = openNoFollow(f.absPath, os.O_CREATE|os.O_RDWR, 0644)
		if err == nil {
			repaired = true
			// The file was recreated as a fresh inode; any cached read handle now
			// points at the orphaned old inode and must be dropped.
			f.invalidateReader()
		}
	}
	if err != nil {
		return nil, false, err
	}

	fi, statErr := h.Stat()
	if statErr != nil {
		_ = h.Close()
		return nil, false, statErr
	}
	if fi.Size() != f.length {
		if err := h.Truncate(f.length); err != nil {
			_ = h.Close()
			return nil, false, err
		}
		repaired = true
		f.invalidateReader()
	}

	f.writeHandle = h
	return h, repaired, nil
}

// invalidateWriter closes and drops the cached write handle.
func (f *fileLayout) invalidateWriter() {
	f.wmu.Lock()
	f.invalidateWriterLocked()
	f.wmu.Unlock()
}

func (f *fileLayout) tryInvalidateWriter() {
	if !f.wmu.TryLock() {
		return
	}
	f.invalidateWriterLocked()
	f.wmu.Unlock()
}

func (f *fileLayout) invalidateWriterLocked() {
	if f.writeHandle != nil {
		_ = f.writeHandle.Close()
		f.writeHandle = nil
	}
}

// FileStorage manages the files on disk for a torrent and provides thread-safe
// block read/write and piece verification.
//
// Concurrency model: mu is an RWMutex. Block reads AND writes (ReadBlock,
// VerifyPiece, WriteBlock) take the read lock and run concurrently — positional
// ReadAt/WriteAt on the cached per-file handles is safe for parallel use, so
// neither the seed path nor a multi-MB piece write serializes behind one mutex or
// stalls peer goroutines. The exclusive write lock is reserved for fast-resume
// state (LoadState), which is infrequent and off the block path. The only shared
// write-path bookkeeping — the cached handles and the deferred-mtime dirty set —
// is guarded by finer-grained locks (fileLayout.wmu/rmu and mtMu). Close marks the
// storage closed atomically before invalidating cached handles so session teardown
// cannot wedge behind a blocked VerifyPiece/read lock.
type FileStorage struct {
	mu          sync.RWMutex
	resolver    *PathResolver
	baseDir     string
	files       []*fileLayout
	pieceLength int64
	totalSize   int64
	// mtMu guards stateFileMt and dirty, which the shared-read-lock WriteBlock and
	// SaveState paths mutate concurrently. dirty records files written since the last
	// persist; their mtimes are captured lazily in SaveState instead of via a stat
	// syscall on every completed piece (mirrors the mmap backend).
	mtMu        sync.Mutex
	stateFileMt map[string]int64
	dirty       map[*fileLayout]struct{}
	closed      atomic.Bool
}

// NewFileStorage creates the target directories and pre-allocates files to their
// respective sizes.
func NewFileStorage(baseDir string, files []FileInfo, pieceLength int64) (*FileStorage, error) {
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

	return &FileStorage{
		resolver:    resolver,
		baseDir:     resolver.BaseDir(),
		files:       layouts,
		pieceLength: pieceLength,
		totalSize:   currentOffset,
		stateFileMt: stateFileMt,
		dirty:       make(map[*fileLayout]struct{}, len(layouts)),
	}, nil
}

// BaseDir returns the base directory of the storage.
func (s *FileStorage) BaseDir() string {
	return s.baseDir
}

// TotalSize returns the total size of all files in the torrent.
func (s *FileStorage) TotalSize() int64 {
	return s.totalSize
}

// PieceLengthValue returns the standard piece length (not the actual length of a specific piece).
func (s *FileStorage) PieceLengthValue() int64 {
	return s.pieceLength
}

// PieceLength returns the length of a piece at pieceIndex.
// All pieces except the last are equal to the standard pieceLength.
func (s *FileStorage) PieceLength(pieceIndex int64) int64 {
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
func (s *FileStorage) ReadBlock(pieceIndex int64, offset int64, buf []byte) (int, error) {
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
	if s.closed.Load() {
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
			if s.closed.Load() {
				file.invalidateReader()
				return 0, ErrStorageClosed
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
func (s *FileStorage) WriteBlock(pieceIndex int64, offset int64, data []byte) error {
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

	// Writes take the shared read lock: positional WriteAt on the cached O_RDWR
	// handle is concurrency-safe, so completed-piece writes (on the background write
	// pool) run in parallel with each other and with the per-block seed reads instead
	// of stalling every peer goroutine behind an exclusive lock for a multi-MB write.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
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

			f, fileRepaired, err := file.writer()
			if err != nil {
				return fmt.Errorf("failed to open file %s for writing: %w", file.path, err)
			}
			if fileRepaired {
				repaired = true
			}
			if s.closed.Load() {
				file.invalidateWriter()
				return ErrStorageClosed
			}

			n, err := f.WriteAt(data[bufOffset:bufOffset+nBytes], fileOffset)
			if err != nil {
				return fmt.Errorf("write error on file %s: %w", file.path, err)
			}
			if int64(n) != nBytes {
				return fmt.Errorf("short write on file %s: expected %d bytes, got %d", file.path, nBytes, n)
			}

			// Defer the mtime refresh to SaveState: mark the file dirty rather than
			// paying a stat syscall on every completed piece. SaveState is infrequent
			// (~once per second) and off the block path.
			s.mtMu.Lock()
			s.dirty[file] = struct{}{}
			s.mtMu.Unlock()
		}
	}

	if repaired {
		return ErrFileRepaired
	}
	return nil
}

// verifyChunkSize bounds the reusable buffer VerifyPiece streams file bytes
// through. It is comfortably larger than a 16 KB block (so ReadAt syscall overhead
// is amortized) while staying small enough that re-checking a multi-GB torrent on
// resume no longer allocates a full piece — potentially many MB — per call.
const verifyChunkSize = 1 << 18 // 256 KiB

// VerifyPiece computes the SHA-1 hash of the piece and compares it with expectedHash.
func (s *FileStorage) VerifyPiece(pieceIndex int64, expectedHash [20]byte) (bool, error) {
	pieceLen := s.PieceLength(pieceIndex)
	if pieceLen == 0 {
		return false, fmt.Errorf("invalid piece index: %d", pieceIndex)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return false, ErrStorageClosed
	}

	// Stream the piece through the hasher via a single reusable chunk buffer instead
	// of materializing the whole piece. Files are laid out in ascending offset order,
	// so iterating s.files feeds the hasher the piece bytes in order across file
	// boundaries. The buffer is capped to the piece length so tiny pieces stay tiny.
	h := sha1.New()
	chunkLen := pieceLen
	if chunkLen > verifyChunkSize {
		chunkLen = verifyChunkSize
	}
	chunk := make([]byte, chunkLen)
	globalStart := pieceIndex * s.pieceLength
	globalEnd := globalStart + pieceLen

	for _, file := range s.files {
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)

			f, err := file.reader()
			if err != nil {
				return false, fmt.Errorf("failed to open file %s for reading: %w", file.path, err)
			}
			if s.closed.Load() {
				file.invalidateReader()
				return false, ErrStorageClosed
			}

			fileOffset := overlapStart - file.startOffset
			remaining := overlapEnd - overlapStart
			for remaining > 0 {
				readLen := int64(len(chunk))
				if readLen > remaining {
					readLen = remaining
				}
				n, err := f.ReadAt(chunk[:readLen], fileOffset)
				if err != nil && err != io.EOF {
					return false, fmt.Errorf("read error on file %s: %w", file.path, err)
				}
				if int64(n) != readLen {
					return false, fmt.Errorf("short read on file %s: expected %d bytes, got %d", file.path, readLen, n)
				}
				if _, err := h.Write(chunk[:readLen]); err != nil {
					return false, err
				}
				fileOffset += readLen
				remaining -= readLen
			}
		}
	}

	var actualHash [20]byte
	copy(actualHash[:], h.Sum(nil))
	return actualHash == expectedHash, nil
}

// Close marks storage closed and releases cached file handles. It is idempotent.
// Close is deliberately non-blocking with respect to active reads: a session may
// be closing because background verification is wedged in disk I/O, and teardown
// must still release ownership immediately. Operations that start after Close
// return ErrStorageClosed; operations already in progress may finish or fail as
// their underlying file handles are invalidated.
func (s *FileStorage) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	for _, file := range s.files {
		file.tryInvalidateReader()
		file.tryInvalidateWriter()
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

// refreshDirtyLocked captures the on-disk mtime of every file written since the
// last persist and clears the dirty set. WriteAt already bumps the mtime, so a
// plain stat suffices; failures are ignored (mirroring the previous per-write
// behavior) since a stale mtime only costs an extra re-verify on the next resume.
// The caller must hold mtMu.
func (s *FileStorage) refreshDirtyLocked() {
	for file := range s.dirty {
		if fi, err := os.Stat(file.absPath); err == nil {
			s.stateFileMt[file.path] = fi.ModTime().UnixNano()
		}
		delete(s.dirty, file)
	}
}

// SaveState writes the completed pieces and file metadata to a fast-resume state file.
func (s *FileStorage) SaveState(infoHashHex string, completedPieces []int) error {
	if s.closed.Load() {
		return ErrStorageClosed
	}

	statePath, err := s.resolver.ResolveAndValidate("." + infoHashHex + ".state")
	if err != nil {
		return err
	}

	type fileMeta struct {
		path  string
		size  int64
		mtime int64
	}
	var filesMeta []fileMeta

	s.mu.RLock()
	closed := s.closed.Load()
	if !closed {
		s.mtMu.Lock()
		// Capture the current mtime of every file written since the last persist
		// (deferred from WriteBlock) before snapshotting the metadata.
		s.refreshDirtyLocked()
		filesMeta = make([]fileMeta, 0, len(s.files))
		for _, f := range s.files {
			mtime := int64(0)
			if mt, ok := s.stateFileMt[f.path]; ok {
				mtime = mt
			}
			filesMeta = append(filesMeta, fileMeta{
				path:  f.path,
				size:  f.length,
				mtime: mtime,
			})
		}
		s.mtMu.Unlock()
	}
	s.mu.RUnlock()

	if closed {
		return ErrStorageClosed
	}

	state := FastResumeState{
		InfoHashHex:     infoHashHex,
		CompletedPieces: completedPieces,
	}

	for _, fm := range filesMeta {
		state.Files = append(state.Files, struct {
			Path  string `json:"path"`
			Size  int64  `json:"size"`
			Mtime int64  `json:"mtime"`
		}{
			Path:  fm.path,
			Size:  fm.size,
			Mtime: fm.mtime,
		})
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return os.WriteFile(statePath, data, 0644)
}

// LoadState reads and validates the fast-resume state file, returning completed piece indices if valid.
func (s *FileStorage) LoadState(infoHashHex string) ([]int, error) {
	if s.closed.Load() {
		return nil, ErrStorageClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, ErrStorageClosed
	}

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

// Package storage handles thread-safe read/write operations and verification
// of torrent files on disk.
package storage

import (
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
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
	// Pad marks a BEP 47 pad file: Length zero bytes that only align the next
	// file. A pad file is never created, opened or recorded in resume state; its
	// range reads as zeros, writes to it are dropped, and verification hashes
	// zeros for it. Its Path is ignored.
	Pad bool
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
		return NewStorage, nil
	case BackendMMap:
		return mmapFactory()
	case BackendMemory:
		return func(baseDir string, files []FileInfo, pieceLength int64) (Storage, error) {
			st, err := NewMemStorage(baseDir, files, pieceLength)
			if err != nil {
				return nil, err
			}
			return st, nil
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
//
// Every Factory returns an untyped nil Storage on error. Returning the failed
// (*FileStorage)(nil) directly would box it into a non-nil interface, and a
// caller testing st == nil would install it and crash on the first call.
func NewStorage(baseDir string, files []FileInfo, pieceLength int64) (Storage, error) {
	st, err := NewFileStorage(baseDir, files, pieceLength)
	if err != nil {
		return nil, err
	}
	return st, nil
}

// fileLayout holds a file's byte range within the torrent plus lazily-opened,
// cached read and write handles. Paths are validated once and every operation is
// anchored to downloadRoot, so a later symlink swap cannot redirect payload I/O.
// The cached handles count against the process-wide fileHandles budget, which
// closes the least recently used ones once too many are open.
//
// A cache hit takes no lock: the handles are published through atomic pointers
// and set or cleared only under rmu or wmu. Whoever clears one (an eviction, a
// repair, Close) swaps it out before closing it, and os.File lets an I/O call
// that has already started finish across a concurrent Close, so an operation
// only sees a closed handle if it loaded it just before the swap. Its I/O then
// fails with os.ErrClosed, and it takes a fresh handle and retries (see
// retryClosedHandle).
type fileLayout struct {
	repaired     atomic.Bool
	path         string // relative path (torrent-declared)
	idx          int    // position in FileStorage.files
	downloadRoot *DownloadRoot
	volumeGuard  *externalVolumeGuard
	length       int64
	startOffset  int64
	endOffset    int64

	// lastUse is the fileHandles clock when a cached handle of this file was last
	// taken; the budget evicts the files with the oldest stamps first.
	lastUse atomic.Int64
	// cached is how many handles this file holds and heldSlot its index in
	// fileHandles.held while that is nonzero; both are guarded by fileHandles.mu.
	cached   int
	heldSlot int

	// readHandle is an O_RDONLY handle opened on first read and reused for every
	// subsequent block read of this file — eliminating the open/close syscall pair
	// per 16 KB block on the seed path. Where reads share the write handle
	// (readsShareWriteHandle), a read only opens it when the file has no cached
	// write handle. Set and cleared under rmu. Invalidated when a write
	// recreates/resizes the file so a stale handle to an orphaned inode is dropped,
	// on eviction, and on Close.
	rmu        sync.Mutex
	readHandle atomic.Pointer[os.File]

	// writeHandle is an O_RDWR handle opened (and, if the file vanished or was
	// resized, repaired) on first write and reused for every subsequent block write
	// of this file — the write-side analogue of readHandle, eliminating the
	// open/stat/close syscall churn per completed piece on the download path. Reads
	// reuse it too where readsShareWriteHandle allows, which keeps a file being
	// downloaded and verified to one descriptor. Set and cleared under wmu.
	// Invalidated on eviction and Close, and when a periodic check finds that the
	// file it was opened on is no longer the one at path (see writer).
	wmu         sync.Mutex
	writeHandle atomic.Pointer[os.File]
	// writerChecked is the storageClock time writeHandle was last opened or
	// checked against the file at path.
	writerChecked atomic.Int64
}

// readsShareWriteHandle is whether a read uses a file's cached write handle
// rather than opening a read handle of its own, which halves the descriptors a
// file being downloaded and seeded holds. Not on Windows: there every
// positional read and write through one os.File takes that file's single I/O
// lock, so a shared handle would queue the upload reads of a file behind its
// piece writes, where separate handles let them overlap. Windows handles are
// not scarce, so nothing is lost there. A var so tests can exercise both; treat
// it as a constant.
var readsShareWriteHandle = runtime.GOOS != "windows"

// touch stamps f with the budget clock on a cache hit. The clock only moves on
// opens, so in steady state this is two loads and no store.
func (f *fileLayout) touch() {
	if now := fileHandles.clock.Load(); f.lastUse.Load() != now {
		f.lastUse.Store(now)
	}
}

// reader returns a cached handle to read from: the read handle, else the write
// handle (where readsShareWriteHandle), else a newly opened O_RDONLY one.
func (f *fileLayout) reader() (*os.File, error) {
	if h := f.readHandle.Load(); h != nil {
		f.touch()
		return h, nil
	}
	if readsShareWriteHandle {
		if h := f.writeHandle.Load(); h != nil {
			f.touch()
			return h, nil
		}
	}

	f.rmu.Lock()
	// Another read may have opened the handle while this one waited.
	if h := f.readHandle.Load(); h != nil {
		f.rmu.Unlock()
		return h, nil
	}
	if readsShareWriteHandle {
		if h := f.writeHandle.Load(); h != nil {
			f.rmu.Unlock()
			return h, nil
		}
	}
	h, err := f.openReaderLocked()
	if err != nil {
		f.rmu.Unlock()
		return nil, err
	}
	f.readHandle.Store(h)
	evictNow := fileHandles.acquire(f)
	f.rmu.Unlock()
	if evictNow {
		fileHandles.evictOverrun()
	}
	return h, nil
}

func (f *fileLayout) openReaderLocked() (*os.File, error) {
	if err := f.volumeGuard.validate(); err != nil {
		return nil, err
	}
	return rootOpenNoFollow(f.downloadRoot, f.path, os.O_RDONLY, 0)
}

// invalidateReader closes and drops the cached read handle.
func (f *fileLayout) invalidateReader() {
	f.rmu.Lock()
	f.invalidateReaderLocked()
	f.rmu.Unlock()
}

// tryInvalidateReader closes and drops the cached read handle unless rmu is
// held, reporting whether it closed one.
func (f *fileLayout) tryInvalidateReader() bool {
	if !f.rmu.TryLock() {
		return false
	}
	closed := f.invalidateReaderLocked()
	f.rmu.Unlock()
	return closed
}

func (f *fileLayout) invalidateReaderLocked() bool {
	h := f.readHandle.Swap(nil)
	if h == nil {
		return false
	}
	closeCachedHandle(h)
	fileHandles.release(f)
	return true
}

// writeHandleRecheckInterval is how often a cached write handle is checked
// against the file its path now names. Checking costs a stat of the handle and a
// rooted lstat of the path, far more than the write bookkeeping around it, so it
// is not paid on every completed piece: between checks a write costs one clock
// read. A file that is deleted, moved, replaced or truncated under the client is
// therefore noticed by the first write at least this long after the previous
// check. A var so tests can shorten it; treat it as a constant.
var writeHandleRecheckInterval = time.Second

// storageClockBase anchors the monotonic clock the recheck stamps are read from.
var storageClockBase = time.Now()

// storageClock returns monotonic nanoseconds since the package was loaded.
func storageClock() int64 {
	return int64(time.Since(storageClockBase))
}

// writer returns the cached O_RDWR handle, opening it on first use. The open
// doubles as the repair check: if the file vanished it is recreated, and if it
// is now shorter than the declared length it is grown back; either case reports
// repaired=true and drops the now-stale read handle. A longer file is left alone
// for the same reason NewFileStorage never shrinks one.
//
// Once cached, the handle is reused, so the open/stat/close churn is paid once
// per file rather than once per completed piece. A cached handle keeps working
// after the file it was opened on is deleted, moved or replaced, though, and
// every write through it would then land in a file nobody can reach while
// reporting success. So at most once per writeHandleRecheckInterval a cache hit
// checks that the handle still is the full-length file at f.path, and otherwise
// drops it and reopens, which repairs the file and reports repaired=true.
func (f *fileLayout) writer() (*os.File, bool, error) {
	if h := f.writeHandle.Load(); h != nil {
		if f.recheckDue() {
			switch f.checkCachedWriter(h) {
			case writerStale:
				return f.reopenWriter(h, true)
			case writerClosed:
				return f.reopenWriter(h, false)
			}
		}
		f.touch()
		return h, false, nil
	}

	f.wmu.Lock()
	if h := f.writeHandle.Load(); h != nil {
		f.touch()
		f.wmu.Unlock()
		return h, false, nil
	}
	return f.openAndCacheWriterUnlock()
}

// recheckDue reports whether the cached write handle is due a check, and claims
// that check: of several writes finding it due at once, one runs it.
func (f *fileLayout) recheckDue() bool {
	now := storageClock()
	last := f.writerChecked.Load()
	return now-last >= int64(writeHandleRecheckInterval) && f.writerChecked.CompareAndSwap(last, now)
}

// writerState is what a check of a cached write handle found.
type writerState int

const (
	// writerLive: the handle is the full-length file at f.path, or the check
	// could not tell, and the handle stays in use.
	writerLive writerState = iota
	// writerStale: the file was deleted, moved, replaced or truncated.
	writerStale
	// writerClosed: an eviction or Close closed the handle under the check.
	writerClosed
)

// checkCachedWriter checks a cached write handle against the file f.path now
// names. Only positive evidence makes it stale: a path that is gone, is now a
// symlink or names a different file, or a file shorter than declared. A stat
// that fails for any other reason (a permission prompt, a volume going away)
// proves nothing about the file, and calling it stale would make the session
// discard its whole progress, so the handle is kept as it was before the check
// existed.
func (f *fileLayout) checkCachedWriter(h *os.File) writerState {
	handleInfo, err := h.Stat()
	if err != nil {
		if errors.Is(err, os.ErrClosed) {
			return writerClosed
		}
		return writerLive
	}
	if handleInfo.Size() < f.length {
		return writerStale
	}
	pathInfo, err := f.downloadRoot.Lstat(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return writerStale
		}
		return writerLive
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(pathInfo, handleInfo) {
		return writerStale
	}
	return writerLive
}

// reopenWriter replaces a stale or concurrently closed cached write handle.
// When stale is true, the file changed underneath the handle, so any cached
// read handle is also dropped and repaired is reported even if the replacement
// file was already at least f.length bytes long.
func (f *fileLayout) reopenWriter(expected *os.File, stale bool) (*os.File, bool, error) {
	f.wmu.Lock()
	if cur := f.writeHandle.Load(); cur != nil && cur != expected {
		f.touch()
		f.wmu.Unlock()
		return cur, stale, nil
	}
	f.invalidateWriterLocked()
	if stale {
		f.invalidateReader()
	}
	h, repaired, err := f.openAndCacheWriterUnlock()
	if err != nil {
		return nil, false, err
	}
	return h, repaired || stale, nil
}

// openAndCacheWriterUnlock opens (and if needed repairs) the write handle,
// caches it, releases wmu, and enforces the global handle budget. Caller must
// hold wmu.
func (f *fileLayout) openAndCacheWriterUnlock() (*os.File, bool, error) {
	h, repaired, err := f.openWriterLocked()
	if err != nil {
		f.wmu.Unlock()
		return nil, false, err
	}
	// The open just checked the file, so the next check is a full interval away.
	f.writerChecked.Store(storageClock())
	f.writeHandle.Store(h)
	evictNow := fileHandles.acquire(f)
	f.wmu.Unlock()
	if evictNow {
		fileHandles.evictOverrun()
	}
	return h, repaired, nil
}

// openWriterLocked opens, and if needed repairs, the file for writing. The
// caller holds wmu.
func (f *fileLayout) openWriterLocked() (h *os.File, repaired bool, err error) {
	if err := f.volumeGuard.validate(); err != nil {
		return nil, false, err
	}

	h, err = rootOpenNoFollow(f.downloadRoot, f.path, os.O_RDWR, 0644)
	if os.IsNotExist(err) {
		if mkErr := mkdirAllInRoot(f.downloadRoot, filepath.Dir(f.path), 0755); mkErr != nil {
			return nil, false, mkErr
		}
		h, err = rootOpenNoFollow(f.downloadRoot, f.path, os.O_CREATE|os.O_RDWR, 0644)
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
	if fi.Size() < f.length {
		if err := h.Truncate(f.length); err != nil {
			_ = h.Close()
			return nil, false, err
		}
		repaired = true
		f.invalidateReader()
	}
	return h, repaired, nil
}

// invalidateWriter closes and drops the cached write handle.
func (f *fileLayout) invalidateWriter() {
	f.wmu.Lock()
	f.invalidateWriterLocked()
	f.wmu.Unlock()
}

// tryInvalidateWriter closes and drops the cached write handle unless wmu is
// held, reporting whether it closed one.
func (f *fileLayout) tryInvalidateWriter() bool {
	if !f.wmu.TryLock() {
		return false
	}
	closed := f.invalidateWriterLocked()
	f.wmu.Unlock()
	return closed
}

func (f *fileLayout) invalidateWriterLocked() bool {
	h := f.writeHandle.Swap(nil)
	if h == nil {
		return false
	}
	closeCachedHandle(h)
	fileHandles.release(f)
	return true
}

// tryEvict closes f's cached handles for the budget and returns how many it
// closed. It only try-locks, so it never waits on a file whose handle is being
// opened; an operation using a handle it closes retries with a fresh one.
func (f *fileLayout) tryEvict() int {
	closed := 0
	if f.tryInvalidateReader() {
		closed++
	}
	if f.tryInvalidateWriter() {
		closed++
	}
	return closed
}

// maxHandleRetries bounds how often one operation takes a fresh handle after
// finding its cached one closed under it. Eviction closes the least recently
// used handles, so one just opened is practically never closed again before
// the retry uses it.
const maxHandleRetries = 8

// retryClosedHandle reports whether an I/O error means that the cached handle
// the operation loaded was closed under it by an eviction or a repair, so the
// operation should take a fresh handle and try again. Positional reads and
// writes repeat safely, so a retry cannot shift or duplicate any bytes.
func (s *FileStorage) retryClosedHandle(err error, attempt int) bool {
	return attempt < maxHandleRetries && errors.Is(err, os.ErrClosed) && !s.closed.Load()
}

// ioError wraps an I/O error on a payload file, reporting ErrStorageClosed if
// Close invalidated the handle under the operation.
func (s *FileStorage) ioError(op string, file *fileLayout, err error) error {
	if errors.Is(err, os.ErrClosed) && s.closed.Load() {
		return ErrStorageClosed
	}
	return fmt.Errorf("%s error on file %s: %w", op, file.path, err)
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
	mu       sync.RWMutex
	resolver *PathResolver
	baseDir  string
	// files holds the payload files in offset order. BEP 47 pad files get no
	// layout: their ranges are the gaps between consecutive layouts (and after
	// the last one, up to totalSize), which the block paths read and hash as
	// zeros. Everything indexed by file (resume state, mappings, handles) thus
	// only ever sees files that exist on disk.
	files       []*fileLayout
	pieceLength int64
	totalSize   int64
	// mtMu guards stateFileMt, stateFileInfo, initialInfo, dirty, checkpoint and
	// mtDelta/mtResync,
	// which the shared-read-lock WriteBlock and persistence paths mutate
	// concurrently. dirty records what each file still owes: a metadata snapshot
	// (captured lazily when state is persisted instead of via a stat syscall on
	// every completed piece) and a flush before the next durable checkpoint.
	mtMu          sync.Mutex
	stateFileMt   map[string]int64
	stateFileInfo map[string]os.FileInfo
	initialInfo   map[string]os.FileInfo
	// trustedIdentity is the identity each file carried the last time this client
	// had reason to believe it held content it trusts: when the storage was opened,
	// when a checkpoint proved it, or when its own write moved the metadata. A
	// checkpoint trusts a file it did not write only if the file still carries it.
	// An empty entry means no identity is known, which on Windows is the case until
	// a checkpoint records one, and the checkpoint falls back to the snapshot.
	trustedIdentity map[string]string
	dirty           map[*fileLayout]dirtyState
	allCreated      bool
	checkpoint      resumeCheckpoint
	// mtDelta queues the stateFileMt changes the hint writer has not folded into
	// its own per-file copy yet; mtResync replaces it with one full copy when it
	// would have grown without bound. Both are guarded by mtMu.
	mtDelta      []mtUpdate
	mtResync     bool
	hint         resumeHint
	downloadRoot *DownloadRoot
	closed       atomic.Bool
}

// dirtyState records the bookkeeping a file still owes. dirtyMeta means this
// client's own action moved the file's metadata, so the recorded snapshot is
// stale; it is cleared whenever that snapshot is refreshed. dirtySync survives
// cheap hint writes and is cleared only by a durable checkpoint that flushed the
// file, so a checkpoint never re-syncs payload files nothing has written to.
type dirtyState uint8

const (
	dirtyMeta dirtyState = 1 << iota
	dirtySync
)

// NewFileStorage creates the target directories and pre-allocates files to their
// respective sizes.
func NewFileStorage(baseDir string, files []FileInfo, pieceLength int64) (*FileStorage, error) {
	if pieceLength <= 0 {
		return nil, fmt.Errorf("piece length must be positive, got %d", pieceLength)
	}

	downloadRoot, err := OpenDownloadRoot(baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open base directory: %w", err)
	}
	keepDownloadRoot := false
	defer func() {
		if !keepDownloadRoot {
			_ = downloadRoot.Close()
		}
	}()
	// Resolve the base directory once; every per-file path is validated against
	// this single canonical base instead of re-running EvalSymlinks per file.
	resolver, err := NewPathResolver(baseDir)
	if err != nil {
		return nil, err
	}
	volumeGuard, err := newExternalVolumeGuard(resolver.BaseDir(), downloadRoot)
	if err != nil {
		return nil, err
	}

	var layouts []*fileLayout
	var currentOffset int64
	seenPaths := make(map[string]bool)

	for _, file := range files {
		if file.Length < 0 {
			return nil, fmt.Errorf("file length cannot be negative: %q has length %d", file.Path, file.Length)
		}
		if currentOffset > math.MaxInt64-file.Length {
			return nil, fmt.Errorf("total file length overflows int64")
		}
		if file.Pad {
			// A pad range is a gap between layouts (see FileStorage.files).
			currentOffset += file.Length
			continue
		}
		if file.Path == "" {
			return nil, fmt.Errorf("file path cannot be empty")
		}
		if err := checkNoTrailingSeparator(file.Path); err != nil {
			return nil, err
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
			return nil, fmt.Errorf("file path uses reserved internal name %q: %q", topComponent, file.Path)
		}

		key := pathKey(file.Path)
		if seenPaths[key] {
			return nil, fmt.Errorf("duplicate file path detected: %q", file.Path)
		}
		seenPaths[key] = true

		// Verify containment and reject symlinks before opening the same relative
		// path through the anchored root used for all later operations.
		if _, err := resolver.ResolveAndValidate(file.Path); err != nil {
			return nil, err
		}

		layout := &fileLayout{
			path:         file.Path,
			idx:          len(layouts),
			downloadRoot: downloadRoot,
			volumeGuard:  volumeGuard,
			length:       file.Length,
			startOffset:  currentOffset,
			endOffset:    currentOffset + file.Length,
		}
		layouts = append(layouts, layout)
		currentOffset += file.Length
	}
	// Piece indexes are ints (pieceCount, per-piece resume state). A count that
	// does not fit would silently truncate on 32-bit builds, leaving most of
	// the payload outside every piece; refuse it before touching the disk.
	if currentOffset > 0 && (currentOffset-1)/pieceLength+1 > int64(math.MaxInt) {
		return nil, fmt.Errorf("piece count for %d bytes at piece length %d overflows int", currentOffset, pieceLength)
	}

	createdDirs := map[string]struct{}{".": {}}
	type createdFile struct {
		path string
		info os.FileInfo
	}
	createdFiles := make([]createdFile, 0, len(files))
	keepCreatedFiles := false
	defer func() {
		if keepCreatedFiles {
			return
		}
		for i := len(createdFiles) - 1; i >= 0; i-- {
			created := createdFiles[i]
			current, err := downloadRoot.Lstat(created.path)
			if err == nil && current.Mode()&os.ModeSymlink == 0 && os.SameFile(current, created.info) {
				_ = downloadRoot.Remove(created.path)
			}
		}
	}()
	stateFileMt := make(map[string]int64, len(files))
	stateFileInfo := make(map[string]os.FileInfo, len(files))
	initialInfo := make(map[string]os.FileInfo, len(files))
	trustedIdentity := make(map[string]string, len(files))
	// openedObjects catches two layouts that open the same file even though
	// their names differ after folding: 8.3 short names and normalization forms
	// the fold does not model. Both would keep overwriting one file. Hard links
	// that already existed are the exception (see sharedObjectAllowed): a dedup
	// tool leaves them between identical payload files, and refusing them made
	// such a torrent impossible to restore.
	openedObjects := make(map[fileObjectKey]openedObject, len(files))
	allCreated := true

	// hintMt seeds the hint writer's per-file mtime copy so its first tick needs no
	// full copy under mtMu.
	hintMt := make([]int64, len(layouts))
	for i, layout := range layouts {
		path := layout.path

		parentDir := filepath.Clean(filepath.Dir(path))
		if _, exists := createdDirs[parentDir]; !exists {
			if err := mkdirAllInRoot(downloadRoot, parentDir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create directories for file %s: %w", path, err)
			}
			createdDirs[parentDir] = struct{}{}
		}
		if err := volumeGuard.validate(); err != nil {
			return nil, err
		}

		// Open/Create the file and set its size without following a final symlink.
		// We do not retain this handle: read handles are cached lazily on first read
		// (see fileLayout.reader) and writes open on demand, which keeps construction
		// cheap and lets tests that swap a file for a FIFO still block on first access.
		f, created, err := openOrCreateRootFile(downloadRoot, path, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open/create file %s: %w", path, err)
		}
		if layout.length > 0 && !created {
			allCreated = false
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to stat file %s: %w", path, err)
		}
		if created {
			createdFiles = append(createdFiles, createdFile{path: path, info: fi})
		}
		if key, links, ok := fileObjectKeyOf(f, fi); ok {
			opened := openedObject{path: path, length: layout.length, created: created}
			if first, dup := openedObjects[key]; !dup {
				openedObjects[key] = opened
			} else if !sharedObjectAllowed(first, opened, links) {
				f.Close()
				return nil, fmt.Errorf("duplicate file path detected: %q and %q open the same file", first.path, path)
			}
		}
		// Grow a short file (sparsely), but never shrink one: a torrent naming a
		// file that already exists must not destroy its tail before a single piece
		// has been verified. Block I/O only ever touches [0, length), and the size
		// mismatch keeps every resume check (LoadState, LoadResumeState and the
		// checkpoint) from trusting the file, so its pieces are always rehashed.
		if fi.Size() < layout.length {
			if err := f.Truncate(layout.length); err != nil {
				f.Close()
				return nil, fmt.Errorf("failed to pre-allocate size for file %s: %w", path, err)
			}
			fi, err = f.Stat()
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("failed to stat file %s after resize: %w", path, err)
			}
		}
		stateFileInfo[path] = fi
		initialInfo[path] = fi
		stateFileMt[path] = fi.ModTime().UnixNano()
		hintMt[i] = stateFileMt[path]
		// Capture the identity while the handle is open: Windows reads the change
		// timestamp from the handle, so this is the only chance to anchor a file the
		// session never writes to without paying a second open.
		trustedIdentity[path] = fileIdentity(f, fi)
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("failed to close file %s: %w", path, err)
		}
	}

	keepCreatedFiles = true
	keepDownloadRoot = true
	return &FileStorage{
		resolver:        resolver,
		baseDir:         resolver.BaseDir(),
		files:           layouts,
		pieceLength:     pieceLength,
		totalSize:       currentOffset,
		stateFileMt:     stateFileMt,
		stateFileInfo:   stateFileInfo,
		initialInfo:     initialInfo,
		trustedIdentity: trustedIdentity,
		dirty:           make(map[*fileLayout]dirtyState, len(layouts)),
		allCreated:      allCreated,
		hint:            resumeHint{mtimes: hintMt, synced: true},
		downloadRoot:    downloadRoot,
	}, nil
}

func openOrCreateRootFile(root *DownloadRoot, path string, perm os.FileMode) (*os.File, bool, error) {
	f, err := rootOpenNoFollow(root, path, os.O_RDWR, perm)
	if err == nil {
		return f, false, nil
	}
	if !os.IsNotExist(err) {
		return nil, false, err
	}
	f, err = rootOpenNoFollow(root, path, os.O_CREATE|os.O_EXCL|os.O_RDWR, perm)
	if os.IsExist(err) {
		f, err = rootOpenNoFollow(root, path, os.O_RDWR, perm)
		return f, false, err
	}
	return f, err == nil, err
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

// firstFileEndingAfter returns the index of the first file that ends after
// globalStart. Files are laid out in ascending offset order, so the block paths
// start there and stop at the first file starting at or past the block's end:
// O(log files) per block instead of a scan of every file, which a torrent with
// hundreds of thousands of files turned into milliseconds per 16 KiB request.
func (s *FileStorage) firstFileEndingAfter(globalStart int64) int {
	return sort.Search(len(s.files), func(i int) bool { return s.files[i].endOffset > globalStart })
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

	// covered is the end of what has been filled so far; a gap before the next
	// file, or after the last one, is a pad range and reads as zeros.
	covered := globalStart
	for _, file := range s.files[s.firstFileEndingAfter(globalStart):] {
		if file.startOffset >= globalEnd {
			break
		}
		// Check overlap between [globalStart, globalEnd) and [file.startOffset, file.endOffset)
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)

			fileOffset := overlapStart - file.startOffset
			bufOffset := overlapStart - globalStart
			nBytes := overlapEnd - overlapStart

			if overlapStart > covered {
				clear(buf[covered-globalStart : bufOffset])
			}
			if err := s.readFileAt(file, buf[bufOffset:bufOffset+nBytes], fileOffset); err != nil {
				return 0, err
			}
			covered = overlapEnd
		}
	}
	if covered < globalEnd {
		clear(buf[covered-globalStart:])
	}

	return len(buf), nil
}

// readFileAt fills buf from one file at fileOffset through its cached handle.
// The caller holds s.mu, shared or exclusive.
func (s *FileStorage) readFileAt(file *fileLayout, buf []byte, fileOffset int64) error {
	for attempt := 0; ; attempt++ {
		f, err := file.reader()
		if err != nil {
			return fmt.Errorf("failed to open file %s for reading: %w", file.path, err)
		}
		if s.closed.Load() {
			file.invalidateReader()
			return ErrStorageClosed
		}

		n, err := f.ReadAt(buf, fileOffset)
		if err != nil && err != io.EOF {
			if s.retryClosedHandle(err, attempt) {
				continue
			}
			return s.ioError("read", file, err)
		}
		if n != len(buf) {
			return fmt.Errorf("short read on file %s: expected %d bytes, got %d", file.path, len(buf), n)
		}
		return nil
	}
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

	// Only payload files are written: bytes falling in a gap between them belong
	// to a pad range and are dropped.
	for _, file := range s.files[s.firstFileEndingAfter(globalStart):] {
		if file.startOffset >= globalEnd {
			break
		}
		// Check overlap between [globalStart, globalEnd) and [file.startOffset, file.endOffset)
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)

			fileOffset := overlapStart - file.startOffset
			bufOffset := overlapStart - globalStart
			nBytes := overlapEnd - overlapStart

			fileRepaired, err := s.writeFileAt(file, data[bufOffset:bufOffset+nBytes], fileOffset)
			repaired = repaired || fileRepaired
			if err != nil {
				return err
			}
		}
	}

	if repaired {
		return ErrFileRepaired
	}
	return nil
}

// writeFileAt writes data to one file at fileOffset through its cached handle,
// reporting whether opening that handle had to repair the file. The caller
// holds s.mu, shared or exclusive.
func (s *FileStorage) writeFileAt(file *fileLayout, data []byte, fileOffset int64) (repaired bool, err error) {
	for attempt := 0; ; attempt++ {
		f, fileRepaired, err := file.writer()
		if err != nil {
			return repaired, fmt.Errorf("failed to open file %s for writing: %w", file.path, err)
		}
		if fileRepaired {
			file.repaired.Store(true)
			repaired = true
		}
		if s.closed.Load() {
			file.invalidateWriter()
			return repaired, ErrStorageClosed
		}

		n, err := f.WriteAt(data, fileOffset)
		if err != nil {
			if s.retryClosedHandle(err, attempt) {
				continue
			}
			return repaired, s.ioError("write", file, err)
		}
		if n != len(data) {
			return repaired, fmt.Errorf("short write on file %s: expected %d bytes, got %d", file.path, len(data), n)
		}
		break
	}

	// Defer the mtime refresh and the flush to the persistence paths: mark the
	// file dirty rather than paying a stat or fsync syscall on every completed
	// piece. Both paths are infrequent and off the block path.
	s.mtMu.Lock()
	s.dirty[file] |= dirtyMeta | dirtySync
	s.allCreated = false
	s.mtMu.Unlock()
	return repaired, nil
}

// verifyChunkSize bounds the reusable buffer VerifyPiece streams file bytes
// through. It is comfortably larger than a 16 KB block (so ReadAt syscall overhead
// is amortized) while staying small enough that re-checking a multi-GB torrent on
// resume no longer allocates a full piece — potentially many MB — per call.
const verifyChunkSize = 1 << 18 // 256 KiB

var verifyBuffers = sync.Pool{New: func() any {
	buffer := make([]byte, verifyChunkSize)
	return &buffer
}}

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
	buffer := verifyBuffers.Get().(*[]byte)
	defer verifyBuffers.Put(buffer)
	chunk := (*buffer)[:chunkLen]
	globalStart := pieceIndex * s.pieceLength
	globalEnd := globalStart + pieceLen

	covered := globalStart
	for _, file := range s.files[s.firstFileEndingAfter(globalStart):] {
		if file.startOffset >= globalEnd {
			break
		}
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)
			if overlapStart > covered {
				hashZeros(h, overlapStart-covered)
			}
			if err := s.hashFileRange(h, file, overlapStart-file.startOffset, overlapEnd-overlapStart, chunk); err != nil {
				return false, err
			}
			covered = overlapEnd
		}
	}
	if covered < globalEnd {
		hashZeros(h, globalEnd-covered)
	}

	var actualHash [20]byte
	copy(actualHash[:], h.Sum(nil))
	return actualHash == expectedHash, nil
}

// zeroChunk is a read-only block of zeros that pad ranges are hashed from, so
// verifying a piece that holds pad bytes neither allocates nor touches disk.
var zeroChunk [32 << 10]byte

// hashZeros feeds length zero bytes, the content of a pad range, to h. A
// hash.Hash never returns an error from Write.
func hashZeros(h hash.Hash, length int64) {
	for length > 0 {
		n := min(int64(len(zeroChunk)), length)
		_, _ = h.Write(zeroChunk[:n])
		length -= n
	}
}

// hashFileRange feeds length bytes of one file, starting at fileOffset, to h
// through its cached handle, reading them chunk by chunk. The caller holds s.mu,
// shared or exclusive.
func (s *FileStorage) hashFileRange(h hash.Hash, file *fileLayout, fileOffset, length int64, chunk []byte) error {
	var f *os.File
	for attempt := 0; length > 0; {
		if f == nil {
			var err error
			if f, err = file.reader(); err != nil {
				return fmt.Errorf("failed to open file %s for reading: %w", file.path, err)
			}
			if s.closed.Load() {
				file.invalidateReader()
				return ErrStorageClosed
			}
		}
		readLen := min(int64(len(chunk)), length)
		n, err := f.ReadAt(chunk[:readLen], fileOffset)
		if err != nil && err != io.EOF {
			if s.retryClosedHandle(err, attempt) {
				attempt++
				f = nil
				continue
			}
			return s.ioError("read", file, err)
		}
		if int64(n) != readLen {
			return fmt.Errorf("short read on file %s: expected %d bytes, got %d", file.path, readLen, n)
		}
		if _, err := h.Write(chunk[:readLen]); err != nil {
			return err
		}
		fileOffset += readLen
		length -= readLen
	}
	return nil
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
	if s.downloadRoot != nil {
		return s.downloadRoot.Close()
	}
	return nil
}

// FastResumeState defines the structure for saving completed pieces metadata
type FastResumeState struct {
	Version         int               `json:"version,omitempty"`
	PieceLength     int64             `json:"piece_length,omitempty"`
	VerifiedPieces  []int             `json:"verified_pieces,omitempty"`
	Identities      []string          `json:"identities,omitempty"`
	IdentityMode    string            `json:"identity_mode,omitempty"`
	InfoHashHex     string            `json:"info_hash_hex"`
	Files           []resumeFileEntry `json:"files"`
	CompletedPieces []int             `json:"completed_pieces"`
}

// resumeFileEntry is the per-file metadata a resume state records.
type resumeFileEntry struct {
	Path  string `json:"path"`
	Size  int64  `json:"size"`
	Mtime int64  `json:"mtime"`
}

// refreshDirtyLocked captures the on-disk mtime of every file written since the
// last persist and clears their metadata debt. WriteAt already bumps the mtime, so
// a plain stat suffices; failures are ignored (mirroring the previous per-write
// behavior) since a stale mtime only costs an extra re-verify on the next resume.
// The pending-flush bit survives: only a durable checkpoint clears that.
// The caller must hold mtMu.
func (s *FileStorage) refreshDirtyLocked() {
	for file, flags := range s.dirty {
		if flags&dirtyMeta == 0 {
			continue
		}
		if fi, err := file.downloadRoot.Stat(file.path); err == nil {
			s.setStateMtLocked(file, fi.ModTime().UnixNano())
			s.stateFileInfo[file.path] = fi
			// The snapshot now describes a file this client moved itself, so the
			// identity it was trusted under no longer applies. Where a stat carries
			// the whole identity (Unix) this re-anchors it immediately, keeping the
			// window in which a written file cannot be cross-checked down to one
			// persist; elsewhere it clears, and the snapshot comparison takes over.
			s.trustedIdentity[file.path] = fileIdentity(nil, fi)
		}
		flags &^= dirtyMeta
		if flags == 0 {
			delete(s.dirty, file)
		} else {
			s.dirty[file] = flags
		}
	}
}

// SaveState writes the completed pieces and file metadata to a fast-resume state file.
func (s *FileStorage) SaveState(infoHashHex string, completedPieces []int) error {
	if s.closed.Load() {
		return ErrStorageClosed
	}

	stateName := "." + infoHashHex + ".state"
	if _, err := s.resolver.ResolveAndValidate(stateName); err != nil {
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
		state.Files = append(state.Files, resumeFileEntry{Path: fm.path, Size: fm.size, Mtime: fm.mtime})
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return s.writeResumeAtomically(stateName, data, false)
}

// LoadState reads and validates the fast-resume state file, returning completed
// piece indices if valid. It is the conservative Storage-interface fallback used by
// backends without a durable checkpoint: every index it returns is a hint that the
// caller must still hash. Backends implementing ResumeStorage use LoadResumeState.
func (s *FileStorage) LoadState(infoHashHex string) ([]int, error) {
	if s.closed.Load() {
		return nil, ErrStorageClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, ErrStorageClosed
	}

	stateName := "." + infoHashHex + ".state"
	if _, err := s.resolver.ResolveAndValidate(stateName); err != nil {
		return nil, err
	}
	stateFile, err := rootOpenNoFollow(s.downloadRoot, stateName, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(stateFile)
	closeErr := stateFile.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
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

		// The rooted stat intentionally pays for a secure component walk. LoadState
		// is a once-per-torrent control path, so avoiding a symlink race matters more
		// here than matching the syscall count of an absolute-path stat.
		fi, err := f.downloadRoot.Stat(f.path)
		if err != nil {
			return nil, fmt.Errorf("file stat error: %w", err)
		}

		if fi.Size() != savedFile.Size || fi.ModTime().UnixNano() != savedFile.Mtime {
			return nil, fmt.Errorf("file modification mismatch for %s", f.path)
		}
	}
	s.mtMu.Lock()
	for i, f := range s.files {
		s.setStateMtLocked(f, state.Files[i].Mtime)
	}
	s.mtMu.Unlock()

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

	// Verify that no component of the path is a symlink, and that every existing
	// parent is a real directory: Go reports a Windows junction as irregular,
	// never as a symlink or a directory.
	current := r.canonicalBase
	components := strings.Split(rel, string(filepath.Separator))
	for i, comp := range components {
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
		if i < len(components)-1 && !fi.IsDir() {
			return "", fmt.Errorf("path component is not a directory: %s", current)
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
// to avoid a storage -> dht import cycle. It compares folded names with trailing
// dots and spaces removed: on a case-insensitive filesystem ".DHT_NODES" opens
// the same file, and Windows drops the trailing characters of ".dht_nodes. ".
func isReservedStorageName(name string) bool {
	name = strings.TrimRight(name, ". ")
	// Every reserved name starts with a dot, which folding leaves alone.
	if !strings.HasPrefix(name, ".") {
		return false
	}
	name = pathKey(name)
	if name == ".dht_nodes" {
		return true
	}
	// Per-torrent fast-resume files: a leading dot plus a ".state" suffix.
	return strings.HasSuffix(name, ".state")
}

// fileObjectKey names the on-disk object an opened file refers to, the same
// for every path that reaches it (see fileObjectKeyOf).
type fileObjectKey struct{ a, b uint64 }

// openedObject records a layout of a NewFileStorage call that opened an on-disk
// object: its path, its torrent length, and whether that call created the
// object.
type openedObject struct {
	path    string
	length  int64
	created bool
}

// sharedObjectAllowed reports whether a later layout, which opened the same
// on-disk object as first, may keep it. links is the object's current link
// count.
//
// Only distinct, pre-existing directory entries of one multiply-linked file
// qualify: the hard links rdfind or jdupes -L leave between identical files,
// which a torrent restores as they are. A file this call created has a single
// name, so a second layout reaching it is an alias of that name (case, Unicode
// normalization or an 8.3 short name) and is refused. So is a pre-existing
// file with one link, since two names can then only be aliases of one entry,
// and a pair whose paths fold to one key (duplicate layouts are normally
// refused earlier; this keeps the check self-contained). An unknown link count
// of zero is refused too. Identical files have one length, so a pair the
// torrent gives two lengths is refused as well: growing the file for the
// longer layout would leave the shorter one's name on a file of the wrong
// size, which no checkpoint trusts.
func sharedObjectAllowed(first, later openedObject, links uint64) bool {
	return !first.created && !later.created && links >= 2 && first.length == later.length &&
		pathKey(first.path) != pathKey(later.path)
}

// checkNoTrailingSeparator refuses a layout path ending in a separator. Go's
// os.Root followed a final symlink for such a path (GO-2026-4970), and the
// storage relies on os.Root to keep payload I/O inside the download directory.
// Torrent paths are joined from sanitized components and never end in one, so
// refusing it keeps that defence independent of the toolchain.
func checkNoTrailingSeparator(path string) error {
	if path != "" && os.IsPathSeparator(path[len(path)-1]) {
		return fmt.Errorf("file path ends in a path separator: %q", path)
	}
	return nil
}

// pathFolder is stateless and safe for concurrent use (see cases.Fold).
var pathFolder = cases.Fold()

// pathKey is the duplicate-detection key for a relative path, the same one
// torrent.Parse uses: Unicode NFC over full case folding, so two names that a
// case- or normalization-insensitive filesystem (APFS, NTFS) stores as one
// file collide here too.
func pathKey(p string) string {
	p = filepath.Clean(p)
	for i := 0; i < len(p); i++ {
		if p[i] >= utf8.RuneSelf {
			return norm.NFC.String(pathFolder.String(norm.NFD.String(p)))
		}
	}
	// ASCII, as nearly every payload path is: both normalization forms leave
	// it unchanged and full case folding maps only A-Z, so skip the Unicode
	// tables, which cost about a microsecond per file at every add and restore.
	return strings.ToLower(p)
}

// PathKey is the key under which two paths name the same file on a case- or
// normalization-insensitive filesystem: the cleaned path, fully case-folded,
// in Unicode NFC. The downloader compares keys of resolved payload paths to
// keep two torrents from sharing a file.
func PathKey(p string) string {
	return pathKey(p)
}

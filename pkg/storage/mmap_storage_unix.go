//go:build !windows

package storage

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// errMappingFault reports that touching a mapped page faulted: the file shrank
// under its mapping (anything, including another torrent naming the same path,
// can truncate it) or the disk filled up while a sparse page was written.
var errMappingFault = errors.New("memory-mapped file access faulted: the file shrank or the disk is full")

// maxLiveMappings bounds the file mappings the mmap backend keeps across the
// whole process. Each mapped file costs one entry of the per-process mapping
// allowance (vm.max_map_count, 65530 by default on Linux), which the Go runtime
// draws its heap and stacks from too and treats running out of as fatal. A
// torrent with more files than this would otherwise map every one it touched.
const maxLiveMappings = 32 << 10

// liveMappings counts the mappings every MMapStorage holds. mappingLimit
// overrides maxLiveMappings when positive (tests).
var liveMappings, mappingLimit atomic.Int64

// reserveMapping claims a slot for one more mapping, reporting false when the
// process already holds as many as it may.
func reserveMapping() bool {
	limit := int64(maxLiveMappings)
	if override := mappingLimit.Load(); override > 0 {
		limit = override
	}
	if liveMappings.Add(1) > limit {
		liveMappings.Add(-1)
		return false
	}
	return true
}

// releaseMapping returns the slot of a mapping that has been unmapped.
func releaseMapping() {
	liveMappings.Add(-1)
}

// mappedFile pairs a layout with its mapping. MMapStorage.maps holds one per
// FileStorage.files entry, in the same order, so a file index found in files
// addresses maps too.
type mappedFile struct {
	layout *fileLayout
	data   []byte
	// viaHandles marks a file this storage serves through FileStorage's cached
	// handles because the mapping budget was spent when it needed a mapping. It
	// sticks for the storage's life, so a file is never mapped while writes reach
	// it through a handle: without a unified buffer cache (OpenBSD) the two views
	// of it are not coherent. Set under the exclusive lock, read under either.
	viaHandles bool
	// stale marks a mapping that faulted. It is set under the shared lock and the
	// mapping is dropped and re-established under the exclusive one, so no copy
	// can still be using it when it is unmapped.
	stale atomic.Bool
}

// unmapLocked drops the mapping and returns its budget slot. The caller holds
// s.mu exclusively.
func (m *mappedFile) unmapLocked() error {
	if err := unix.Munmap(m.data); err != nil {
		return err
	}
	m.data = nil
	releaseMapping()
	return nil
}

// MMapStorage serves torrent content from shared file-backed memory mappings.
type MMapStorage struct {
	*FileStorage
	maps  []*mappedFile
	dirty map[*fileLayout]struct{}
	// checkpointing is set while a durable checkpoint holds the files released and
	// captures their metadata. Re-establishing a mapping during that window would
	// move the change timestamp the checkpoint just recorded, so reads fall back to
	// the file handles instead of remapping.
	checkpointing atomic.Bool
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
	served, err := s.copyMapped(globalStart, globalEnd, buf)
	if err != nil {
		return 0, err
	}
	if !served {
		// A durable checkpoint released the mappings while this read was starting.
		// Serve it from the file handles rather than re-establishing a mapping the
		// checkpoint would then have to distrust.
		return s.FileStorage.ReadBlock(pieceIndex, offset, buf)
	}
	return len(buf), nil
}

// copyMapped copies the range out of the mappings, reporting false when a file it
// needs is no longer mapped. Checking and copying share one read lock: a checkpoint
// releasing the mappings in between would leave nothing to copy from.
func (s *MMapStorage) copyMapped(globalStart, globalEnd int64, buf []byte) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return false, ErrStorageClosed
	}
	if !s.rangeMappedLocked(globalStart, globalEnd) {
		return false, nil
	}
	if err := s.copyFromMappingsLocked(globalStart, globalEnd, buf); err != nil {
		if errors.Is(err, errMappingFault) {
			s.markStaleLocked(globalStart, globalEnd)
		}
		return false, err
	}
	return true, nil
}

// copyFromMappingsLocked copies a mapped range with memory faults contained. The
// caller holds s.mu and has checked that the range is mapped.
func (s *MMapStorage) copyFromMappingsLocked(globalStart, globalEnd int64, buf []byte) (err error) {
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer recoverMappingFault(&err)
	for _, mapped := range s.maps[s.firstFileEndingAfter(globalStart):] {
		file := mapped.layout
		if file.startOffset >= globalEnd {
			break
		}
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)
			fileOffset := overlapStart - file.startOffset
			bufOffset := overlapStart - globalStart
			nBytes := overlapEnd - overlapStart
			if mapped.viaHandles {
				if err := s.readFileAt(file, buf[bufOffset:bufOffset+nBytes], fileOffset); err != nil {
					return err
				}
				continue
			}
			copy(buf[bufOffset:bufOffset+nBytes], mapped.data[fileOffset:fileOffset+nBytes])
		}
	}
	return nil
}

// recoverMappingFault turns the panic raised by touching a lost mapped page into
// errMappingFault. The block paths run their mapped copies and hashes with
// debug.SetPanicOnFault set, so a truncated file costs one failed request rather
// than a fatal SIGBUS that takes every torrent down with it. Only a memory fault
// carries Addr(); any other panic, a nil dereference included, is a bug and is
// raised again. It must be deferred directly for recover to see the panic.
func recoverMappingFault(err *error) {
	r := recover()
	if r == nil {
		return
	}
	if _, ok := r.(interface{ Addr() uintptr }); ok {
		*err = errMappingFault
		return
	}
	panic(r)
}

// markStaleLocked flags every mapping overlapping a range that faulted, so the
// next access re-establishes it (and its size check) instead of faulting again.
// The caller holds s.mu, shared or exclusive.
func (s *MMapStorage) markStaleLocked(globalStart, globalEnd int64) {
	for _, mapped := range s.maps[s.firstFileEndingAfter(globalStart):] {
		if mapped.layout.startOffset >= globalEnd {
			break
		}
		if len(mapped.data) > 0 {
			mapped.stale.Store(true)
		}
	}
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

	globalStart := pieceIndex*s.pieceLength + offset
	globalEnd := globalStart + int64(len(data))
	repaired, err := s.writeMappedLocked(globalStart, globalEnd, data)
	if errors.Is(err, errMappingFault) {
		// A file shrank under its mapping. Mapping it afresh grows it back to its
		// declared length, so retry once and report the repair: whatever the
		// truncation cut off must be rechecked. A second fault means the disk
		// really is full.
		s.markStaleLocked(globalStart, globalEnd)
		_, err = s.writeMappedLocked(globalStart, globalEnd, data)
		repaired = true
	}
	if err != nil {
		return err
	}
	if repaired {
		return ErrFileRepaired
	}
	return nil
}

// writeMappedLocked maps (repairing as needed) every file the range touches and
// copies data into the mappings with memory faults contained. The caller holds
// s.mu exclusively.
func (s *MMapStorage) writeMappedLocked(globalStart, globalEnd int64, data []byte) (repaired bool, err error) {
	first := s.firstFileEndingAfter(globalStart)
	for _, mapped := range s.maps[first:] {
		file := mapped.layout
		if file.startOffset >= globalEnd {
			break
		}
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			wasRepaired, err := s.ensureMappedFileLocked(mapped, true)
			if err != nil {
				return repaired, err
			}
			repaired = repaired || wasRepaired
		}
	}
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer recoverMappingFault(&err)
	for _, mapped := range s.maps[first:] {
		file := mapped.layout
		if file.startOffset >= globalEnd {
			break
		}
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)
			fileOffset := overlapStart - file.startOffset
			bufOffset := overlapStart - globalStart
			nBytes := overlapEnd - overlapStart
			if mapped.viaHandles {
				// The handle write marks the file dirty for FileStorage itself; the
				// set below is only for files whose mtime a mapping holds back.
				fileRepaired, err := s.writeFileAt(file, data[bufOffset:bufOffset+nBytes], fileOffset)
				repaired = repaired || fileRepaired
				if err != nil {
					return repaired, err
				}
				continue
			}
			copy(mapped.data[fileOffset:fileOffset+nBytes], data[bufOffset:bufOffset+nBytes])
			s.dirty[file] = struct{}{}
		}
	}
	return repaired, nil
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
	served, actualHash, err := s.hashMapped(globalStart, globalEnd)
	if err != nil {
		return false, err
	}
	if !served {
		// The mappings went away with a durable checkpoint; hash through the file
		// handles instead of remapping behind its metadata capture.
		return s.FileStorage.VerifyPiece(pieceIndex, expectedHash)
	}
	return actualHash == expectedHash, nil
}

// hashMapped hashes the range from the mappings, reporting false when a file it
// needs is no longer mapped. Checking and hashing share one read lock.
func (s *MMapStorage) hashMapped(globalStart, globalEnd int64) (bool, [20]byte, error) {
	var actualHash [20]byte
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return false, actualHash, ErrStorageClosed
	}
	if !s.rangeMappedLocked(globalStart, globalEnd) {
		return false, actualHash, nil
	}
	h := sha1.New()
	if err := s.hashMappingsLocked(h, globalStart, globalEnd); err != nil {
		if errors.Is(err, errMappingFault) {
			s.markStaleLocked(globalStart, globalEnd)
		}
		return false, actualHash, err
	}
	copy(actualHash[:], h.Sum(nil))
	return true, actualHash, nil
}

// hashMappingsLocked feeds a mapped range to h with memory faults contained. The
// caller holds s.mu and has checked that the range is mapped.
func (s *MMapStorage) hashMappingsLocked(h hash.Hash, globalStart, globalEnd int64) (err error) {
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer recoverMappingFault(&err)
	// A file served through its handle is hashed through a pooled chunk buffer,
	// taken only once such a file is reached.
	var buffer *[]byte
	defer func() {
		if buffer != nil {
			verifyBuffers.Put(buffer)
		}
	}()
	for _, mapped := range s.maps[s.firstFileEndingAfter(globalStart):] {
		file := mapped.layout
		if file.startOffset >= globalEnd {
			break
		}
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			overlapStart := max(globalStart, file.startOffset)
			overlapEnd := min(globalEnd, file.endOffset)
			fileOffset := overlapStart - file.startOffset
			nBytes := overlapEnd - overlapStart
			if mapped.viaHandles {
				if buffer == nil {
					buffer = verifyBuffers.Get().(*[]byte)
				}
				if err := s.hashFileRange(h, file, fileOffset, nBytes, *buffer); err != nil {
					return err
				}
				continue
			}
			if _, err := h.Write(mapped.data[fileOffset : fileOffset+nBytes]); err != nil {
				return err
			}
		}
	}
	return nil
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
			releaseMapping()
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
	allMapped := s.rangeMappedLocked(globalStart, globalEnd)
	s.mu.RUnlock()
	if allMapped {
		return nil
	}
	if s.checkpointing.Load() {
		// A durable checkpoint is capturing this file's metadata. Mapping it now
		// would move the change timestamp the checkpoint records and cost a rehash
		// on the next launch, so the caller reads through the file handles until the
		// checkpoint is done — without ever waiting on the exclusive mapping lock.
		return nil
	}

	// Slow path: at least one overlapping file still needs its initial mapping.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrStorageClosed
	}
	for _, mapped := range s.maps[s.firstFileEndingAfter(globalStart):] {
		file := mapped.layout
		if file.startOffset >= globalEnd {
			break
		}
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			if _, err := s.ensureMappedFileLocked(mapped, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// rangeMappedLocked reports whether every non-empty file overlapping the range is
// mapped by a mapping that has not faulted. The caller holds s.mu.
func (s *MMapStorage) rangeMappedLocked(globalStart, globalEnd int64) bool {
	for _, mapped := range s.maps[s.firstFileEndingAfter(globalStart):] {
		file := mapped.layout
		if file.startOffset >= globalEnd {
			break
		}
		if globalStart < file.endOffset && globalEnd > file.startOffset {
			if file.length != 0 && !mapped.viaHandles && (len(mapped.data) == 0 || mapped.stale.Load()) {
				return false
			}
		}
	}
	return true
}

func (s *MMapStorage) ensureMappedFileLocked(mapped *mappedFile, repair bool) (bool, error) {
	layout := mapped.layout
	if mapped.stale.Load() {
		// The mapping faulted. Drop it and map the file afresh, which checks (or,
		// for a write, repairs) its size instead of faulting on every access.
		if len(mapped.data) > 0 {
			if err := mapped.unmapLocked(); err != nil {
				return false, fmt.Errorf("failed to release faulted mapping of %s: %w", layout.path, err)
			}
		}
		mapped.stale.Store(false)
	}
	if layout.length == 0 || len(mapped.data) > 0 || mapped.viaHandles {
		return false, nil
	}
	if !reserveMapping() {
		// Every mapping the process may hold is taken. Serve this file through the
		// budgeted handles from now on; the block paths dispatch on viaHandles, and
		// a write through its handle repairs the file as a mapping would.
		mapped.viaHandles = true
		return false, nil
	}

	data, repaired, err := mapOrRepairFile(layout, repair)
	if err != nil || data == nil {
		releaseMapping()
	}
	if err != nil {
		return false, err
	}
	mapped.data = data
	if repaired {
		layout.repaired.Store(true)
	}
	return repaired, nil
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
	// Only [0, length) is mapped, so a longer file maps as is; like
	// NewFileStorage, a repair grows a short file but never shrinks one. Mapping
	// past the end of a short file would fault on first access.
	if fi.Size() < layout.length {
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
	if err := s.refreshMappedMtimes(); err != nil {
		return err
	}
	return s.FileStorage.SaveState(infoHashHex, completedPieces)
}

// refreshMappedMtimes stamps the files written through a mapping since the last
// persist. Mapped writes do not bump the mtime promptly, so the cheap hint path
// sets it explicitly instead of paying a syscall on every completed piece.
func (s *MMapStorage) refreshMappedMtimes() error {
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

	touched := make([]*fileLayout, 0, len(dirty))
	for file := range dirty {
		if err := touchMappedFile(file); err != nil {
			s.mu.Lock()
			for pending := range dirty {
				s.dirty[pending] = struct{}{}
			}
			s.mu.Unlock()
			return err
		}
		touched = append(touched, file)
	}
	s.markFileStorageDirty(touched, dirtyMeta|dirtySync)
	return nil
}

// unmapForCheckpoint releases every mapping before a durable checkpoint reads the
// files back. Tearing the mapping down is what settles a mapped write: it flushes
// the dirty pages and applies their timestamp bump, so the checkpoint sees stable
// metadata instead of racing a page-out that happens during (or after) its own
// flush. Mappings are re-established lazily once the checkpoint is done.
func (s *MMapStorage) unmapForCheckpoint() error {
	if s.closed.Load() {
		return ErrStorageClosed
	}
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return ErrStorageClosed
	}
	// Writes hold this lock exclusively, so the set of files this client has written
	// to cannot change while the mappings are classified.
	unflushed := s.pendingFlush()
	var written []*fileLayout
	var released []releasedMapping
	var firstErr error
	for _, mapped := range s.maps {
		if len(mapped.data) == 0 {
			continue
		}
		layout := mapped.layout
		_, mappedWrite := s.dirty[layout]
		wrote := mappedWrite || unflushed[layout]
		// A mapping this client only ever read through still has its change timestamp
		// moved by the release, so record the identity the file carries while the
		// mapping is up: only a file that still matches what the last checkpoint
		// trusted may adopt the metadata the release leaves behind.
		var before os.FileInfo
		if !wrote {
			before, _ = layout.downloadRoot.Stat(layout.path)
		}
		if err := mapped.unmapLocked(); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to flush mapping for file %s: %w", layout.path, err)
			}
			continue
		}
		mapped.stale.Store(false)
		if wrote {
			written = append(written, layout)
			delete(s.dirty, layout)
			continue
		}
		released = append(released, releasedMapping{file: layout, before: before})
	}
	s.mu.Unlock()
	if firstErr != nil {
		return firstErr
	}
	// Our own writes moved these files' timestamps and they still owe a flush; the
	// rest are trusted only if they were unchanged when their mapping went away.
	s.markFileStorageDirty(written, dirtyMeta|dirtySync)
	s.markReleasedMappings(released)
	return nil
}

func (s *MMapStorage) markFileStorageDirty(files []*fileLayout, flags dirtyState) {
	if len(files) == 0 {
		return
	}
	s.mtMu.Lock()
	for _, file := range files {
		s.FileStorage.dirty[file] |= flags
	}
	s.mtMu.Unlock()
}

func touchMappedFile(file *fileLayout) error {
	h, err := rootOpenNoFollow(file.downloadRoot, file.path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open file %s for mtime refresh: %w", file.path, err)
	}
	tv := unix.NsecToTimeval(time.Now().UnixNano())
	touchErr := unix.Futimes(int(h.Fd()), []unix.Timeval{tv, tv})
	closeErr := h.Close()
	if touchErr != nil {
		return fmt.Errorf("failed to refresh mtime for file %s: %w", file.path, touchErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close file %s after mtime refresh: %w", file.path, closeErr)
	}
	return nil
}

// SaveResumeState settles mapped writes before the ordinary checkpoint. Active
// transfers only stamp timestamps and persist a lightweight hint; a durable
// checkpoint releases the mappings first so its flush and metadata capture see a
// file no page-out can move underneath them.
func (s *MMapStorage) SaveResumeState(hash string, verified, unverified []int, durable bool) error {
	if durable {
		// Keep the files unmapped for the whole checkpoint: a mapping re-established
		// between the release and the metadata capture would move the change
		// timestamp again right after it was recorded, and the next launch would
		// rehash a torrent this checkpoint had just proved.
		s.checkpointing.Store(true)
		defer s.checkpointing.Store(false)
		if err := s.unmapForCheckpoint(); err != nil {
			return err
		}
		return s.FileStorage.SaveResumeState(hash, verified, unverified, true)
	}
	if err := s.refreshMappedMtimes(); err != nil {
		return err
	}
	return s.FileStorage.SaveResumeState(hash, verified, unverified, false)
}

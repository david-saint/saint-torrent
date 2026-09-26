package downloader

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// memStorageFactory keeps metadata tests off the disk.
func memStorageFactory(dir string, files []storage.FileInfo, pieceLength int64) (storage.Storage, error) {
	return storage.NewMemStorage(dir, files, pieceLength)
}

// newTestMagnetSession returns a metadata-mode session for infoBytes, closed at
// the end of the test.
func newTestMagnetSession(t *testing.T, infoBytes []byte, factory storage.Factory) *Session {
	t.Helper()
	sess, err := NewSession(&torrent.Torrent{Name: "magnet", InfoHash: sha1.Sum(infoBytes)}, nil, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sess.storageFactory = factory
	t.Cleanup(sess.Close)
	return sess
}

func testInfoDict(t *testing.T, name string) []byte {
	t.Helper()
	infoBytes, err := bencode.Marshal(map[string]interface{}{
		"name":         name,
		"piece length": int64(16),
		"pieces":       string(make([]byte, 20)),
		"length":       int64(16),
	})
	if err != nil {
		t.Fatal(err)
	}
	return infoBytes
}

// TestMetadataWithTrailingDataIsRejected: the magnet's hash covers the whole
// ut_metadata buffer, but the session parsed "d4:info" + buffer + "e". An info
// dict followed by an extra key/value pair parsed as the first dict alone, so
// the session served and cached info bytes that no longer hashed to the magnet
// and came back under a different hash after a restart.
func TestMetadataWithTrailingDataIsRejected(t *testing.T) {
	dict := testInfoDict(t, "first.bin")
	infoBytes := append(dict[:len(dict):len(dict)], "3:xyzi1e"...)
	sess := newTestMagnetSession(t, infoBytes, memStorageFactory)
	// The accumulator as the ut_metadata handler leaves it on completion.
	sess.mu.Lock()
	sess.metadataSize = len(infoBytes)
	sess.metadataBuf = append([]byte(nil), infoBytes...)
	sess.metadataPieces = []bool{true}
	sess.mu.Unlock()

	if err := sess.onMetadataDownloaded(infoBytes); err == nil {
		t.Fatal("metadata with a value after the info dict was accepted")
	}
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	if !sess.metadataMode || sess.Storage != nil || len(sess.Torrent.InfoBytes) != 0 {
		t.Fatalf("metadataMode=%v storage=%v infoBytes=%d, want the pre-metadata session", sess.metadataMode, sess.Storage, len(sess.Torrent.InfoBytes))
	}
	if sess.statusErr == nil {
		t.Fatal("invalid metadata was not surfaced as the session status")
	}
	// The bytes matched the hash, so every peer sends the same ones: fetching
	// them again would only repeat the failure.
	if !sess.metadataCompleted || sess.metadataBuf != nil {
		t.Fatalf("metadataCompleted=%v buffered=%d, want fetching stopped and the buffer released", sess.metadataCompleted, len(sess.metadataBuf))
	}
	// A handshake handler that sized its request loop before this still
	// indexes the piece map, so it must stay as it was.
	if sess.metadataSize != len(infoBytes) || len(sess.metadataPieces) != 1 {
		t.Fatalf("metadataSize=%d pieces=%d, want the piece map kept", sess.metadataSize, len(sess.metadataPieces))
	}
}

// blockingFactory wraps factory so each call reports that it started and then
// waits for proceed before building anything.
func blockingFactory(factory storage.Factory, entered chan<- struct{}, proceed <-chan struct{}) storage.Factory {
	return func(dir string, files []storage.FileInfo, pieceLength int64) (storage.Storage, error) {
		entered <- struct{}{}
		<-proceed
		return factory(dir, files, pieceLength)
	}
}

// closeRecordingStorage records whether the session closed the storage it was
// handed.
type closeRecordingStorage struct {
	storage.Storage
	closed atomic.Bool
}

func (c *closeRecordingStorage) Close() error {
	c.closed.Store(true)
	return c.Storage.Close()
}

// TestMetadataStorageIsBuiltWithoutSessionLock: the storage factory creates,
// sizes and stats every file of a torrent that may declare hundreds of
// thousands. It ran under s.mu, freezing every peer loop, the UI snapshot and
// the manager's saveState for as long as that took.
func TestMetadataStorageIsBuiltWithoutSessionLock(t *testing.T) {
	infoBytes := testInfoDict(t, "offlock.bin")
	entered := make(chan struct{}, 1)
	proceed := make(chan struct{})
	sess := newTestMagnetSession(t, infoBytes, blockingFactory(memStorageFactory, entered, proceed))

	done := make(chan error, 1)
	go func() { done <- sess.onMetadataDownloaded(infoBytes) }()
	<-entered

	locked := make(chan bool, 1)
	go func() {
		sess.mu.Lock()
		metadataMode := sess.metadataMode
		sess.mu.Unlock()
		locked <- metadataMode
	}()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		close(proceed)
		<-done
		t.Fatal("s.mu was held while the storage factory ran")
	}
	close(proceed)
	if err := <-done; err != nil {
		t.Fatalf("onMetadataDownloaded: %v", err)
	}
	if sess.IsMetadataMode() || sess.TotalSize() != 16 {
		t.Fatalf("metadataMode=%v size=%d, want the torrent published", sess.IsMetadataMode(), sess.TotalSize())
	}
}

// TestMetadataAfterCloseCreatesNoStorage: metadata completing on a peer
// goroutine Close does not wait for (a routed inbound connection) created the
// payload files and installed storage on a session that was already closed.
// Nothing closed that storage, and RemoveSession had already read the empty
// file list it would delete.
func TestMetadataAfterCloseCreatesNoStorage(t *testing.T) {
	infoBytes := testInfoDict(t, "late.bin")
	var calls atomic.Int32
	sess := newTestMagnetSession(t, infoBytes, func(dir string, files []storage.FileInfo, pieceLength int64) (storage.Storage, error) {
		calls.Add(1)
		return memStorageFactory(dir, files, pieceLength)
	})
	sess.Close()

	if err := sess.onMetadataDownloaded(infoBytes); err == nil {
		t.Fatal("metadata was applied to a closed session")
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("storage factory ran %d time(s) for a closed session", n)
	}
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	if sess.Storage != nil || len(sess.Torrent.Files) != 0 {
		t.Fatalf("closed session got storage %v and %d files", sess.Storage, len(sess.Torrent.Files))
	}
}

// TestCloseWaitsForMetadataStorageBeingBuilt: storage built while Close runs
// must end up closed by Close, with its files visible to RemoveSession, never
// orphaned with an open download-directory handle.
func TestCloseWaitsForMetadataStorageBeingBuilt(t *testing.T) {
	infoBytes := testInfoDict(t, "racing.bin")
	entered := make(chan struct{}, 1)
	proceed := make(chan struct{})
	var built *closeRecordingStorage
	sess := newTestMagnetSession(t, infoBytes, blockingFactory(func(dir string, files []storage.FileInfo, pieceLength int64) (storage.Storage, error) {
		st, err := memStorageFactory(dir, files, pieceLength)
		if err != nil {
			return nil, err
		}
		built = &closeRecordingStorage{Storage: st}
		return built, nil
	}, entered, proceed))

	metaDone := make(chan error, 1)
	go func() { metaDone <- sess.onMetadataDownloaded(infoBytes) }()
	<-entered

	closeDone := make(chan struct{})
	go func() {
		sess.Close()
		close(closeDone)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if sess.mu.TryRLock() {
			closing := sess.closing
			sess.mu.RUnlock()
			if closing {
				break
			}
		}
		if time.Now().After(deadline) {
			close(proceed)
			t.Fatal("Close never started while the storage was being built")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-closeDone:
		t.Fatal("Close returned before the storage being built was published")
	default:
	}

	close(proceed)
	<-metaDone
	<-closeDone
	if built == nil || !built.closed.Load() {
		t.Fatal("storage built during Close was never closed")
	}
	if files := sess.Files(); len(files) != 1 {
		t.Fatalf("session files = %v, want the published file list for RemoveSession", files)
	}
}

// TestMetadataRefusesNilPointerStorage: a factory that reports success with a
// nil pointer boxed in the interface must not have it installed.
func TestMetadataRefusesNilPointerStorage(t *testing.T) {
	infoBytes := testInfoDict(t, "typednil.bin")
	sess := newTestMagnetSession(t, infoBytes, func(string, []storage.FileInfo, int64) (storage.Storage, error) {
		return (*storage.MemStorage)(nil), nil
	})
	if err := sess.onMetadataDownloaded(infoBytes); err == nil {
		t.Fatal("a nil *MemStorage was accepted as storage")
	}
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	if sess.Storage != nil || len(sess.PieceStates) != 0 || !sess.metadataMode {
		t.Fatalf("storage=%T pieces=%d metadataMode=%v, want the pre-metadata session", sess.Storage, len(sess.PieceStates), sess.metadataMode)
	}
}

// TestPieceCompletingWhileClosingIsDropped: Close takes its shutdown checkpoint
// after setting closing but before closed. The write workers checked only
// closed, so a piece finishing in between was written and marked after the
// checkpoint: nothing persisted it, and the write moved the file's identity so
// the next launch rehashed the torrent.
func TestPieceCompletingWhileClosingIsDropped(t *testing.T) {
	data := []byte("a piece that arrives during shutdown")
	sess := newPieceTestSession(t, int64(len(data)), [][]byte{data})
	sess.mu.Lock()
	sess.PieceStates[0] = PieceDownloading
	sess.removeNeededLocked(0)
	sess.closing = true
	sess.stateDirty = false
	sess.mu.Unlock()

	result := make(chan pieceWriteResult, 1)
	sess.processCompletedPiece(pieceWriteJob{index: 0, hash: sha1.Sum(data), data: data, result: result})
	if r := <-result; r.status != pieceWriteSkipped {
		t.Fatalf("piece write status = %v, want skipped while closing", r.status)
	}
	buf := make([]byte, len(data))
	if _, err := sess.Storage.ReadBlock(0, 0, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) == string(data) {
		t.Fatal("piece was written after the session started closing")
	}

	// A write already past that check must not re-arm the checkpoint either.
	sess.markPieceCompleted(0)
	sess.mu.RLock()
	dirty := sess.stateDirty
	sess.mu.RUnlock()
	if dirty {
		t.Fatal("a piece completed while closing re-armed the resume checkpoint")
	}
}

// unaddressablePieceStorage reports a piece length no buffer can hold.
type unaddressablePieceStorage struct{ storage.Storage }

func (unaddressablePieceStorage) PieceLengthValue() int64 { return 1 << 62 }

// TestPieceBufferRefusesUnallocatableLengths: getPieceBuf rounded every buffer
// up to the storage's piece length, so a one-block piece of a torrent declaring
// a 2^62-byte piece length died in make() on the peer goroutine, taking the
// whole process down.
func TestPieceBufferRefusesUnallocatableLengths(t *testing.T) {
	sess := &Session{Storage: unaddressablePieceStorage{}}
	bp := sess.getPieceBuf(BlockSize)
	if len(*bp) != BlockSize || cap(*bp) != BlockSize {
		t.Fatalf("buffer len=%d cap=%d, want exactly one block", len(*bp), cap(*bp))
	}
	sess.putPieceBuf(bp)

	for _, length := range []int64{-1, torrent.MaxPieceLength + 1, 1 << 62} {
		if bp := sess.getPieceBuf(length); len(*bp) != 0 {
			t.Fatalf("getPieceBuf(%d) returned %d bytes, want an empty buffer", length, len(*bp))
		}
	}
}

// BenchmarkPieceBufBorrow measures the per-piece borrow and return on the
// piece completion path.
func BenchmarkPieceBufBorrow(b *testing.B) {
	st, err := storage.NewMemStorage(b.TempDir(), []storage.FileInfo{{Path: "bench.bin", Length: 1 << 20}}, 1<<20)
	if err != nil {
		b.Fatal(err)
	}
	sess := &Session{Storage: st}
	b.ReportAllocs()
	for b.Loop() {
		sess.putPieceBuf(sess.getPieceBuf(1 << 20))
	}
}

// TestMagnetStorageFailureKeepsVerifiedMetadata: a storage failure after the
// metadata verified discarded the whole accumulator, so every peer that
// connected afterwards was asked for the full metadata again, only for the
// build to fail the same way. A magnet whose files another torrent holds
// (which a restore now falls back to) re-downloaded its metadata from the swarm
// for as long as it ran. The verified bytes must be kept, fetching stopped, and
// the build retried from them.
func TestMagnetStorageFailureKeepsVerifiedMetadata(t *testing.T) {
	infoBytes := testInfoDict(t, "retry.bin")
	var unavailable atomic.Bool
	unavailable.Store(true)
	var calls atomic.Int32
	sess := newTestMagnetSession(t, infoBytes, func(dir string, files []storage.FileInfo, pieceLength int64) (storage.Storage, error) {
		calls.Add(1)
		if unavailable.Load() {
			return nil, errors.New("volume not mounted")
		}
		return memStorageFactory(dir, files, pieceLength)
	})
	// The accumulator as the ut_metadata handler leaves it on completion.
	sess.mu.Lock()
	sess.metadataSize = len(infoBytes)
	sess.metadataBuf = append([]byte(nil), infoBytes...)
	sess.metadataPieces = []bool{true}
	sess.mu.Unlock()

	if err := sess.onMetadataDownloaded(infoBytes); err == nil {
		t.Fatal("metadata was applied although no storage could be built")
	}
	sess.mu.RLock()
	completed, size, pieces := sess.metadataCompleted, sess.metadataSize, len(sess.metadataPieces)
	kept, retry, status := sess.verifiedMetadata, sess.metadataRetryTimer, sess.statusErr
	sess.mu.RUnlock()
	// metadataCompleted is what stops the handshake handler from requesting
	// the metadata again from each new peer.
	if !completed || size != len(infoBytes) || pieces != 1 {
		t.Fatalf("metadataCompleted=%v size=%d pieces=%d, want fetching stopped with the piece map kept", completed, size, pieces)
	}
	if !bytes.Equal(kept, infoBytes) || retry == nil || status == nil {
		t.Fatalf("kept=%d bytes retry=%v status=%v, want the verified bytes kept, a retry armed and the error shown", len(kept), retry != nil, status)
	}

	// A retry that fails again keeps them and backs off.
	sess.retryMetadataStorage()
	sess.mu.RLock()
	delay := sess.metadataRetryDelay
	sess.mu.RUnlock()
	if delay != 2*metadataStorageRetryMin || !sess.IsMetadataMode() {
		t.Fatalf("retry delay after a second failure = %v (metadata mode %v), want %v", delay, sess.IsMetadataMode(), 2*metadataStorageRetryMin)
	}

	unavailable.Store(false)
	sess.retryMetadataStorage()
	if sess.IsMetadataMode() || sess.TotalSize() != 16 || sess.Status() == "Error" {
		t.Fatalf("metadataMode=%v size=%d status=%q, want the torrent published once storage is available", sess.IsMetadataMode(), sess.TotalSize(), sess.Status())
	}
	sess.mu.RLock()
	leftover := sess.verifiedMetadata != nil || sess.metadataBuf != nil || sess.metadataRetryTimer != nil
	sess.mu.RUnlock()
	if leftover {
		t.Fatal("metadata copies or a retry were kept after the torrent was published")
	}
	if n := calls.Load(); n != 3 {
		t.Fatalf("storage factory ran %d times, want 3", n)
	}
}

// TestCloseStopsMetadataStorageRetry: a pending retry must neither outlive
// the session nor build storage on a closed one.
func TestCloseStopsMetadataStorageRetry(t *testing.T) {
	infoBytes := testInfoDict(t, "closed-retry.bin")
	var calls atomic.Int32
	sess := newTestMagnetSession(t, infoBytes, func(string, []storage.FileInfo, int64) (storage.Storage, error) {
		calls.Add(1)
		return nil, errors.New("volume not mounted")
	})
	if err := sess.onMetadataDownloaded(infoBytes); err == nil {
		t.Fatal("metadata was applied although no storage could be built")
	}
	sess.Close()
	sess.mu.RLock()
	retry := sess.metadataRetryTimer
	sess.mu.RUnlock()
	if retry != nil {
		t.Fatal("Close left the metadata storage retry armed")
	}
	sess.retryMetadataStorage()
	if n := calls.Load(); n != 1 {
		t.Fatalf("storage factory ran %d times, want only the first attempt before Close", n)
	}
}

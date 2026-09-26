package downloader

import (
	"crypto/sha1"
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

// hugePieceStorage reports a piece length no buffer can hold.
type hugePieceStorage struct{ storage.Storage }

func (hugePieceStorage) PieceLengthValue() int64 { return 1 << 62 }

// TestPieceBufferRefusesUnallocatableLengths: getPieceBuf rounded every buffer
// up to the storage's piece length, so a one-block piece of a torrent declaring
// a 2^62-byte piece length died in make() on the peer goroutine, taking the
// whole process down.
func TestPieceBufferRefusesUnallocatableLengths(t *testing.T) {
	sess := &Session{Storage: hugePieceStorage{}}
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

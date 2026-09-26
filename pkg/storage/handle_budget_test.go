package storage

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// setHandleLimit lowers the process-wide cached-handle limit for one test.
func setHandleLimit(t *testing.T, limit int64) {
	t.Helper()
	previous := fileHandles.limitValue()
	fileHandles.limit.Store(limit)
	t.Cleanup(func() { fileHandles.limit.Store(previous) })
}

// waitForHandles waits for the background sweep to bring the cached handles of
// the whole process down to limit.
func waitForHandles(t *testing.T, limit int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for fileHandles.open.Load() > limit {
		if time.Now().After(deadline) {
			t.Fatalf("%d cached handles are still open, want at most %d", fileHandles.open.Load(), limit)
		}
		time.Sleep(time.Millisecond)
	}
}

// openFDs counts the descriptors this process has open, reporting false where
// the platform offers no way to list them.
func openFDs() (int, bool) {
	for _, dir := range []string{"/proc/self/fd", "/dev/fd"} {
		if entries, err := os.ReadDir(dir); err == nil {
			return len(entries), true
		}
	}
	return 0, false
}

// cachedHandles reports how many handles f holds, as the budget accounts them.
func cachedHandles(f *fileLayout) int {
	fileHandles.mu.Lock()
	defer fileHandles.mu.Unlock()
	return f.cached
}

// hasReadHandle reports whether f holds a cached O_RDONLY handle.
func hasReadHandle(f *fileLayout) bool {
	return f.readHandle.Load() != nil
}

func manySmallFiles(count int, length int64) []FileInfo {
	files := make([]FileInfo, count)
	for i := range files {
		files[i] = FileInfo{Path: filepath.Join("t", fmt.Sprintf("f%05d", i)), Length: length}
	}
	return files
}

// budgetPiece is the content the budget tests store in piece i.
func budgetPiece(i int, length int64) []byte {
	piece := make([]byte, length)
	for j := range piece {
		piece[j] = byte(i*31 + j)
	}
	return piece
}

// TestFileStorageBoundsCachedHandles is the regression for a torrent of a few
// thousand files exhausting RLIMIT_NOFILE: every file written and verified kept
// a read and a write handle until Close, 6000 descriptors for these 3000 files,
// where macOS allows about 10k for the whole process.
func TestFileStorageBoundsCachedHandles(t *testing.T) {
	const limit = 64
	const fileCount = 3000
	setHandleLimit(t, limit)
	// Storages earlier tests left open hold handles that count too; evict them
	// so that the peak below measures this storage.
	for fileHandles.open.Load() > limit {
		if fileHandles.evict() == 0 {
			break
		}
	}
	baseOpen := fileHandles.open.Load()
	baseFDs, countFDs := openFDs()

	st, err := NewFileStorage(t.TempDir(), manySmallFiles(fileCount, 16), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	var peak int64
	observe := func() { peak = max(peak, fileHandles.open.Load()) }
	for i := range fileCount {
		if err := st.WriteBlock(int64(i), 0, budgetPiece(i, 16)); err != nil {
			t.Fatalf("WriteBlock(%d): %v", i, err)
		}
		observe()
	}
	got := make([]byte, 16)
	for i := range fileCount {
		ok, err := st.VerifyPiece(int64(i), sha1.Sum(budgetPiece(i, 16)))
		if err != nil || !ok {
			t.Fatalf("VerifyPiece(%d) = %v, %v; want a match", i, ok, err)
		}
		observe()
		if _, err := st.ReadBlock(int64(i), 0, got); err != nil || !bytes.Equal(got, budgetPiece(i, 16)) {
			t.Fatalf("ReadBlock(%d) = %v, %v; want the written piece", i, got, err)
		}
		observe()
	}
	// The opener evicts by itself once the budget is half again over its limit,
	// so it never runs far past it even while the background sweep lags.
	if peak > 2*limit {
		t.Fatalf("%d handles were cached at once, want at most %d", peak, 2*limit)
	}

	waitForHandles(t, limit)
	if countFDs {
		fds, _ := openFDs()
		// The storage also holds its download root open.
		if fds-baseFDs > limit+8 {
			t.Fatalf("%d descriptors open after touching %d files, %d before; want at most %d more", fds, fileCount, baseFDs, limit+8)
		}
	}

	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if got := fileHandles.open.Load(); got > baseOpen {
		t.Fatalf("%d cached handles open after Close, %d before the storage opened", got, baseOpen)
	}
}

// TestFileStorageEvictsLeastRecentlyUsed: the files a sweep closes are the
// ones used longest ago, and a file whose handle it closed reads on.
func TestFileStorageEvictsLeastRecentlyUsed(t *testing.T) {
	const limit = 8
	setHandleLimit(t, limit)
	const fileCount = 64
	st, err := NewFileStorage(t.TempDir(), manySmallFiles(fileCount, 16), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	buf := make([]byte, 16)
	for i := range fileCount {
		if _, err := st.ReadBlock(int64(i), 0, buf); err != nil {
			t.Fatal(err)
		}
	}
	waitForHandles(t, limit)
	if hasReadHandle(st.files[0]) {
		t.Fatal("the least recently used handle survived eviction")
	}
	if !hasReadHandle(st.files[fileCount-1]) {
		t.Fatal("eviction closed the most recently used handle")
	}
	if _, err := st.ReadBlock(0, 0, buf); err != nil || !bytes.Equal(buf, make([]byte, 16)) {
		t.Fatalf("ReadBlock of an evicted file = %v, %v", buf, err)
	}
}

// TestFileStorageRetriesHandleClosedUnderIt: the cache hit path takes no lock,
// so an eviction can close a handle an operation has just loaded. That
// operation's I/O then fails with os.ErrClosed, which must send it back for a
// fresh handle rather than fail the read, and must report a storage that Close
// took away as closed.
func TestFileStorageRetriesHandleClosedUnderIt(t *testing.T) {
	st, err := NewFileStorage(t.TempDir(), []FileInfo{{Path: "a", Length: 16}}, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	f := st.files[0]
	loaded, err := f.reader()
	if err != nil {
		t.Fatal(err)
	}
	if n := f.tryEvict(); n != 1 {
		t.Fatalf("tryEvict closed %d handles, want 1", n)
	}
	_, err = loaded.ReadAt(make([]byte, 16), 0)
	if !st.retryClosedHandle(err, 0) {
		t.Fatalf("I/O on an evicted handle failed with %v, which is not retried", err)
	}
	if st.retryClosedHandle(err, maxHandleRetries) {
		t.Fatal("retries are not bounded")
	}
	if _, err := st.ReadBlock(0, 0, make([]byte, 16)); err != nil {
		t.Fatalf("ReadBlock after eviction: %v", err)
	}

	loaded, err = f.reader()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = loaded.ReadAt(make([]byte, 16), 0)
	if st.retryClosedHandle(err, 0) {
		t.Fatal("I/O on a handle Close invalidated is retried")
	}
	if err := st.ioError("read", f, err); err != ErrStorageClosed {
		t.Fatalf("I/O on a handle Close invalidated reports %v, want ErrStorageClosed", err)
	}
}

// TestFileStorageReadsReuseWriteHandle: a file being downloaded and verified
// holds one descriptor, not an O_RDWR and an O_RDONLY one, except where reads
// keep a handle of their own (readsShareWriteHandle off, as on Windows, where
// one os.File serializes its reads with its writes).
func TestFileStorageReadsReuseWriteHandle(t *testing.T) {
	for _, shared := range []bool{true, false} {
		t.Run(fmt.Sprintf("shared=%v", shared), func(t *testing.T) {
			previous := readsShareWriteHandle
			readsShareWriteHandle = shared
			t.Cleanup(func() { readsShareWriteHandle = previous })

			st, err := NewFileStorage(t.TempDir(), []FileInfo{{Path: "a", Length: 32}}, 32)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			piece := budgetPiece(0, 32)
			if err := st.WriteBlock(0, 0, piece); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, 32)
			if _, err := st.ReadBlock(0, 0, got); err != nil || !bytes.Equal(got, piece) {
				t.Fatalf("ReadBlock = %v, %v; want the written piece", got, err)
			}
			if ok, err := st.VerifyPiece(0, sha1.Sum(piece)); err != nil || !ok {
				t.Fatalf("VerifyPiece = %v, %v; want a match", ok, err)
			}
			wantHandles := 2
			if shared {
				wantHandles = 1
			}
			if got := hasReadHandle(st.files[0]); got == shared {
				t.Fatalf("read handle cached = %v, want %v", got, !shared)
			}
			if n := cachedHandles(st.files[0]); n != wantHandles {
				t.Fatalf("file holds %d cached handles, want %d", n, wantHandles)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			if n := cachedHandles(st.files[0]); n != 0 {
				t.Fatalf("closed storage still holds %d cached handles", n)
			}
		})
	}
}

// TestFileStorageConcurrentIOUnderHandleBudget runs reads, writes and verifies
// from several goroutines over far more files than the budget holds, so the
// sweep constantly closes handles that operations have just loaded. No
// operation may fail and every read must see the stored bytes, whether reads
// share the write handle or keep their own (readsShareWriteHandle).
func TestFileStorageConcurrentIOUnderHandleBudget(t *testing.T) {
	for _, shared := range []bool{true, false} {
		t.Run(fmt.Sprintf("shared=%v", shared), func(t *testing.T) {
			previous := readsShareWriteHandle
			readsShareWriteHandle = shared
			t.Cleanup(func() { readsShareWriteHandle = previous })
			testConcurrentIOUnderHandleBudget(t)
		})
	}
}

func testConcurrentIOUnderHandleBudget(t *testing.T) {
	setHandleLimit(t, 8)
	const fileCount, fileLength, pieceLength = 200, 48, 64
	st, err := NewFileStorage(t.TempDir(), manySmallFiles(fileCount, fileLength), pieceLength)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	pieces := int(st.TotalSize() / pieceLength)
	for i := range pieces {
		if err := st.WriteBlock(int64(i), 0, budgetPiece(i, pieceLength)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(worker), 1))
			buf := make([]byte, 16)
			for range 400 {
				i := rng.IntN(pieces)
				piece := budgetPiece(i, pieceLength)
				switch rng.IntN(3) {
				case 0:
					offset := rng.Int64N(pieceLength - int64(len(buf)) + 1)
					if _, err := st.ReadBlock(int64(i), offset, buf); err != nil {
						t.Errorf("ReadBlock(%d, %d): %v", i, offset, err)
						return
					}
					if !bytes.Equal(buf, piece[offset:offset+int64(len(buf))]) {
						t.Errorf("ReadBlock(%d, %d) = %v, want %v", i, offset, buf, piece[offset:offset+int64(len(buf))])
						return
					}
				case 1:
					if err := st.WriteBlock(int64(i), 0, piece); err != nil {
						t.Errorf("WriteBlock(%d): %v", i, err)
						return
					}
				default:
					if ok, err := st.VerifyPiece(int64(i), sha1.Sum(piece)); err != nil || !ok {
						t.Errorf("VerifyPiece(%d) = %v, %v; want a match", i, ok, err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
}

func TestHandleLimitFor(t *testing.T) {
	for _, tc := range []struct {
		soft uint64
		want int64
	}{
		{0, 16},
		{256, 128},    // never more than half of a tiny limit
		{1024, 256},   // the floor
		{10240, 4864}, // macOS kern.maxfilesperproc: half, less the reserve
		{20000, 8192}, // the ceiling
		{1 << 20, 8192},
		{^uint64(0), 8192}, // RLIM_INFINITY
		{1<<63 - 1, 8192},
	} {
		if got := handleLimitFor(tc.soft); got != tc.want {
			t.Errorf("handleLimitFor(%d) = %d, want %d", tc.soft, got, tc.want)
		}
	}
}

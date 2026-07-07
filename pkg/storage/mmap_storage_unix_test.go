//go:build !windows

package storage

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestMMapStorageCommonSuite(t *testing.T) {
	files := []FileInfo{
		{Path: filepath.Join("dir1", "file1.bin"), Length: 30},
		{Path: filepath.Join("dir2", "file2.bin"), Length: 40},
	}
	st, err := NewMMapStorage(t.TempDir(), files, 25)
	if err != nil {
		t.Fatalf("NewMMapStorage: %v", err)
	}
	defer st.Close()

	if got := st.TotalSize(); got != 70 {
		t.Fatalf("TotalSize() = %d, want 70", got)
	}
	if got := st.PieceLengthValue(); got != 25 {
		t.Fatalf("PieceLengthValue() = %d, want 25", got)
	}
	if got := st.PieceLength(2); got != 20 {
		t.Fatalf("PieceLength(2) = %d, want 20", got)
	}

	data := []byte("ABCDEFGHIJ")
	if err := st.WriteBlock(1, 0, data); err != nil {
		t.Fatalf("WriteBlock spanning files: %v", err)
	}
	got := make([]byte, len(data))
	n, err := st.ReadBlock(1, 0, got)
	if err != nil {
		t.Fatalf("ReadBlock spanning files: %v", err)
	}
	if n != len(data) || !bytes.Equal(got, data) {
		t.Fatalf("read mismatch: n=%d got=%q want=%q", n, got, data)
	}

	piece2 := bytes.Repeat([]byte{'z'}, 20)
	if err := st.WriteBlock(2, 0, piece2); err != nil {
		t.Fatalf("WriteBlock last piece: %v", err)
	}
	ok, err := st.VerifyPiece(2, sha1.Sum(piece2))
	if err != nil {
		t.Fatalf("VerifyPiece: %v", err)
	}
	if !ok {
		t.Fatal("VerifyPiece returned false for matching hash")
	}
	ok, err = st.VerifyPiece(2, sha1.Sum([]byte("wrong")))
	if err != nil {
		t.Fatalf("VerifyPiece wrong hash: %v", err)
	}
	if ok {
		t.Fatal("VerifyPiece returned true for wrong hash")
	}

	infoHashHex := "0123456789abcdef0123456789abcdef01234567"
	if err := st.SaveState(infoHashHex, []int{1, 2}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	pieces, err := st.LoadState(infoHashHex)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if !slices.Equal(pieces, []int{1, 2}) {
		t.Fatalf("LoadState pieces = %v, want [1 2]", pieces)
	}

	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := st.ReadBlock(1, 0, got); !errors.Is(err, ErrStorageClosed) {
		t.Fatalf("ReadBlock after close = %v, want ErrStorageClosed", err)
	}
	if err := st.WriteBlock(1, 0, data); !errors.Is(err, ErrStorageClosed) {
		t.Fatalf("WriteBlock after close = %v, want ErrStorageClosed", err)
	}
}

func TestMMapStorageWriteBlockRepairsMissingFile(t *testing.T) {
	tmpDir := t.TempDir()
	st, err := NewMMapStorage(tmpDir, []FileInfo{{Path: "missing.bin", Length: 32}}, 32)
	if err != nil {
		t.Fatalf("NewMMapStorage: %v", err)
	}
	defer st.Close()

	filePath := filepath.Join(tmpDir, "missing.bin")
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("remove mapped file: %v", err)
	}

	data := bytes.Repeat([]byte{'x'}, 32)
	err = st.WriteBlock(0, 0, data)
	if !errors.Is(err, ErrFileRepaired) {
		t.Fatalf("WriteBlock = %v, want ErrFileRepaired", err)
	}
	got, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read repaired file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("repaired file mismatch: got %q want %q", got, data)
	}
}

func TestMMapStorageReadBlockDoesNotRepairMissingFile(t *testing.T) {
	tmpDir := t.TempDir()
	st, err := NewMMapStorage(tmpDir, []FileInfo{{Path: "missing-read.bin", Length: 32}}, 32)
	if err != nil {
		t.Fatalf("NewMMapStorage: %v", err)
	}
	defer st.Close()

	filePath := filepath.Join(tmpDir, "missing-read.bin")
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("remove mapped file: %v", err)
	}

	if _, err := st.ReadBlock(0, 0, make([]byte, 1)); err == nil {
		t.Fatal("expected ReadBlock to report the missing file")
	}
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatalf("ReadBlock should not recreate missing file, stat err: %v", err)
	}
}

func TestMMapStorageBackendSelection(t *testing.T) {
	st, err := NewStorageWithBackend(BackendMMap, t.TempDir(), []FileInfo{{Path: "data.bin", Length: 1}}, 1)
	if err != nil {
		t.Fatalf("NewStorageWithBackend: %v", err)
	}
	defer st.Close()
	if _, ok := st.(*MMapStorage); !ok {
		t.Fatalf("backend type = %T, want *MMapStorage", st)
	}
}

func TestMMapStorageMapsFilesLazily(t *testing.T) {
	st, err := NewMMapStorage(t.TempDir(), []FileInfo{{Path: "data.bin", Length: 32}}, 32)
	if err != nil {
		t.Fatalf("NewMMapStorage: %v", err)
	}
	defer st.Close()
	if len(st.maps) != 1 {
		t.Fatalf("map count = %d, want 1", len(st.maps))
	}
	if st.maps[0].data != nil {
		t.Fatal("NewMMapStorage should not mmap files at construction time")
	}
	if _, err := st.ReadBlock(0, 0, make([]byte, 1)); err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if st.maps[0].data == nil {
		t.Fatal("ReadBlock should mmap the file lazily")
	}
}

// TestMMapStorageConcurrentReadsAfterMapping drives many concurrent readers and a
// verifier once every file is mapped, exercising the shared-read-lock fast path in
// ensureMappedRange (no exclusive lock in steady state) for correctness under -race.
func TestMMapStorageConcurrentReadsAfterMapping(t *testing.T) {
	const pieceLen = 1 << 14 // 16 KB
	const numPieces = 16
	total := int64(pieceLen * numPieces)

	st, err := NewMMapStorage(t.TempDir(), []FileInfo{{Path: "data.bin", Length: total}}, pieceLen)
	if err != nil {
		t.Fatalf("NewMMapStorage: %v", err)
	}
	defer st.Close()

	pieceBytes := func(p int) []byte {
		b := make([]byte, pieceLen)
		for i := range b {
			b[i] = byte((p*97 + i) & 0xff)
		}
		return b
	}
	for p := 0; p < numPieces; p++ {
		if err := st.WriteBlock(int64(p), 0, pieceBytes(p)); err != nil {
			t.Fatalf("seed write piece %d: %v", p, err)
		}
	}
	// Force the single file fully mapped so subsequent reads take the fast path.
	if _, err := st.VerifyPiece(0, sha1.Sum(pieceBytes(0))); err != nil {
		t.Fatalf("prime mapping: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, numPieces*4)
	for g := 0; g < numPieces*4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			p := g % numPieces
			buf := make([]byte, pieceLen)
			if _, err := st.ReadBlock(int64(p), 0, buf); err != nil {
				errCh <- err
				return
			}
			if !bytes.Equal(buf, pieceBytes(p)) {
				errCh <- errors.New("content mismatch")
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent mmap read failed: %v", err)
		}
	}
}

func TestMMapStorageCloseWaitsForActiveReaders(t *testing.T) {
	st, err := NewMMapStorage(t.TempDir(), []FileInfo{{Path: "data.bin", Length: 32}}, 32)
	if err != nil {
		t.Fatalf("NewMMapStorage: %v", err)
	}

	st.mu.RLock()
	done := make(chan struct{})
	go func() {
		_ = st.Close()
		close(done)
	}()

	deadline := time.After(time.Second)
	for !st.closed.Load() {
		select {
		case <-deadline:
			st.mu.RUnlock()
			t.Fatal("Close did not start")
		default:
			runtime.Gosched()
		}
	}
	select {
	case <-done:
		st.mu.RUnlock()
		t.Fatal("Close returned while a reader held the mmap read lock")
	default:
	}

	st.mu.RUnlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after reader released lock")
	}
}

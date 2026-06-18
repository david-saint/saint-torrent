package storage

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestStorageBackendsCommonSuite(t *testing.T) {
	for _, tc := range []struct {
		name string
		new  func(string, []FileInfo, int64) (Storage, error)
	}{
		{name: "file", new: func(baseDir string, files []FileInfo, pieceLength int64) (Storage, error) {
			return NewFileStorage(baseDir, files, pieceLength)
		}},
		{name: "mmap", new: func(baseDir string, files []FileInfo, pieceLength int64) (Storage, error) {
			return NewMMapStorage(baseDir, files, pieceLength)
		}},
		{name: "mem", new: func(baseDir string, files []FileInfo, pieceLength int64) (Storage, error) {
			return NewMemStorage(baseDir, files, pieceLength)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := []FileInfo{
				{Path: filepath.Join("dir1", "file1.bin"), Length: 30},
				{Path: filepath.Join("dir2", "file2.bin"), Length: 40},
			}
			st, err := tc.new(t.TempDir(), files, 25)
			if err != nil {
				if tc.name == "mmap" && runtime.GOOS == "windows" {
					t.Skipf("mmap unavailable on windows: %v", err)
				}
				t.Fatalf("new storage: %v", err)
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
			if !equalInts(pieces, []int{1, 2}) {
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
		})
	}
}

func TestMMapStorageWriteBlockRepairsMissingFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap storage is not supported on windows")
	}

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

func TestMMapStorageWriteBlockRepairsSameSizeReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap storage is not supported on windows")
	}

	tmpDir := t.TempDir()
	st, err := NewMMapStorage(tmpDir, []FileInfo{{Path: "replace.bin", Length: 32}}, 32)
	if err != nil {
		t.Fatalf("NewMMapStorage: %v", err)
	}
	defer st.Close()

	filePath := filepath.Join(tmpDir, "replace.bin")
	replacementPath := filepath.Join(tmpDir, "replacement.bin")
	if err := os.WriteFile(replacementPath, bytes.Repeat([]byte{'y'}, 32), 0644); err != nil {
		t.Fatalf("write replacement file: %v", err)
	}
	if err := os.Rename(replacementPath, filePath); err != nil {
		t.Fatalf("replace mapped file: %v", err)
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

func TestMMapStorageCloseWaitsForActiveReaders(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap storage is not supported on windows")
	}

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

func TestStorageBackendSelection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		backend Backend
		want    any
	}{
		{name: "file", backend: BackendFile, want: (*FileStorage)(nil)},
		{name: "mmap", backend: BackendMMap, want: (*MMapStorage)(nil)},
		{name: "mem", backend: BackendMemory, want: (*MemStorage)(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := NewStorageWithBackend(tc.backend, t.TempDir(), []FileInfo{{Path: "data.bin", Length: 1}}, 1)
			if err != nil {
				if tc.backend == BackendMMap && runtime.GOOS == "windows" {
					t.Skipf("mmap unavailable on windows: %v", err)
				}
				t.Fatalf("NewStorageWithBackend: %v", err)
			}
			defer st.Close()
			switch tc.want.(type) {
			case *FileStorage:
				if _, ok := st.(*FileStorage); !ok {
					t.Fatalf("backend type = %T, want *FileStorage", st)
				}
			case *MMapStorage:
				if _, ok := st.(*MMapStorage); !ok {
					t.Fatalf("backend type = %T, want *MMapStorage", st)
				}
			case *MemStorage:
				if _, ok := st.(*MemStorage); !ok {
					t.Fatalf("backend type = %T, want *MemStorage", st)
				}
			}
		})
	}

	if got, err := ParseBackend("memory"); err != nil || got != BackendMemory {
		t.Fatalf("ParseBackend(memory) = %q, %v; want %q, nil", got, err, BackendMemory)
	}
	if _, err := ParseBackend("bogus"); err == nil {
		t.Fatal("expected invalid backend error")
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

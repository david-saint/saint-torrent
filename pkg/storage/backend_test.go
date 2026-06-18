package storage

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestStorageBackendsCommonSuite(t *testing.T) {
	for _, tc := range []struct {
		name string
		new  func(string, []FileInfo, int64) (Storage, error)
	}{
		{name: "file", new: func(baseDir string, files []FileInfo, pieceLength int64) (Storage, error) {
			return NewFileStorage(baseDir, files, pieceLength)
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
		})
	}
}

func TestStorageBackendSelection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		backend Backend
		want    any
	}{
		{name: "file", backend: BackendFile, want: (*FileStorage)(nil)},
		{name: "mem", backend: BackendMemory, want: (*MemStorage)(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := NewStorageWithBackend(tc.backend, t.TempDir(), []FileInfo{{Path: "data.bin", Length: 1}}, 1)
			if err != nil {
				t.Fatalf("NewStorageWithBackend: %v", err)
			}
			defer st.Close()
			switch tc.want.(type) {
			case *FileStorage:
				if _, ok := st.(*FileStorage); !ok {
					t.Fatalf("backend type = %T, want *FileStorage", st)
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

func TestMemStorageRejectsSymlinkPathComponents(t *testing.T) {
	tmpDir := t.TempDir()
	outside := t.TempDir()
	linkPath := filepath.Join(tmpDir, "linkdir")
	if err := os.Symlink(outside, linkPath); err != nil {
		t.Skipf("symlink creation not available: %v", err)
	}

	_, err := NewMemStorage(tmpDir, []FileInfo{{Path: "linkdir/file.bin", Length: 1}}, 1)
	if err == nil {
		t.Fatal("expected symlink path component to be rejected")
	}
}

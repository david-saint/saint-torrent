package storage

import (
	"bytes"
	"crypto/sha1"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// preexistingFixture writes a 9000-byte user file where a torrent is about to
// declare a 16-byte one.
func preexistingFixture(t *testing.T) (dir string, original []byte) {
	t.Helper()
	dir = t.TempDir()
	original = bytes.Repeat([]byte("user data "), 900)
	if err := os.WriteFile(filepath.Join(dir, "thesis.docx"), original, 0644); err != nil {
		t.Fatal(err)
	}
	return dir, original
}

func requireUserTail(t *testing.T, dir string, original []byte) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(dir, "thesis.docx"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(original) || !bytes.Equal(got[16:], original[16:]) {
		t.Fatalf("pre-existing file is %d bytes with tail intact=%v; want %d bytes, tail untouched", len(got), len(got) >= 16 && bytes.Equal(got[16:], original[16:]), len(original))
	}
}

// TestNewFileStorageNeverShrinksExistingFile: adding a torrent that names a
// file already in the download directory truncated it to the declared length
// at once, before any piece had been received or verified, destroying
// everything past it. The file is now only ever grown, block I/O stays inside
// the declared length, and the size mismatch keeps resume from trusting it.
func TestNewFileStorageNeverShrinksExistingFile(t *testing.T) {
	dir, original := preexistingFixture(t)
	files := []FileInfo{{Path: "thesis.docx", Length: 16}}
	st, err := NewFileStorage(dir, files, 16)
	if err != nil {
		t.Fatal(err)
	}
	requireUserTail(t, dir, original)

	piece := bytes.Repeat([]byte{'z'}, 16)
	if err := st.WriteBlock(0, 0, piece); err != nil {
		t.Fatalf("WriteBlock over a longer pre-existing file: %v", err)
	}
	got := make([]byte, 16)
	if _, err := st.ReadBlock(0, 0, got); err != nil || !bytes.Equal(got, piece) {
		t.Fatalf("ReadBlock = %q, %v; want %q", got, err, piece)
	}
	if ok, err := st.VerifyPiece(0, sha1.Sum(piece)); err != nil || !ok {
		t.Fatalf("VerifyPiece = %v, %v; want true", ok, err)
	}
	requireUserTail(t, dir, original)

	const hash = "preexisting"
	if err := st.SaveResumeState(hash, []int{0}, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = NewFileStorage(dir, files, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	result, err := st.LoadResumeState(hash)
	if err != nil {
		t.Fatal(err)
	}
	if want := (ResumeState{Recheck: []int{0}}); !reflect.DeepEqual(result, want) {
		t.Fatalf("LoadResumeState = %+v, want %+v: a file whose size differs from the torrent must be rehashed", result, want)
	}
	requireUserTail(t, dir, original)
}

// TestNewFileStorageGrowsShortExistingFile keeps the sparse pre-allocation of a
// file shorter than declared.
func TestNewFileStorageGrowsShortExistingFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a"), []byte("abc"), 0644); err != nil {
		t.Fatal(err)
	}
	st, err := NewFileStorage(dir, []FileInfo{{Path: "a", Length: 32}}, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if fi, err := os.Stat(filepath.Join(dir, "a")); err != nil || fi.Size() != 32 {
		t.Fatalf("short pre-existing file: size %v, err %v; want 32", fi, err)
	}
}

package torrent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestReadFileRefusesOversizedFile: a .torrent was read whole with
// os.ReadFile and then decoded into a tree tens of times its size. The size
// must be refused before anything is read (the file here is sparse).
func TestReadFileRefusesOversizedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "huge.torrent")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(MaxFileSize + 1); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if data, err := ReadFile(path); err == nil || !strings.Contains(err.Error(), "larger than the maximum") {
		t.Fatalf("ReadFile of a %d-byte file = %d bytes, %v; want a size error", MaxFileSize+1, len(data), err)
	}
}

// TestReadFileReturnsWholeFile: a file under the bound is returned exactly.
func TestReadFileReturnsWholeFile(t *testing.T) {
	want := []byte("d4:infod6:lengthi1e4:name1:x12:piece lengthi16384e6:pieces20:01234567890123456789ee")
	path := filepath.Join(t.TempDir(), "small.torrent")
	if err := os.WriteFile(path, want, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFile(path)
	if err != nil || string(got) != string(want) {
		t.Fatalf("ReadFile = %q, %v; want the file", got, err)
	}
}

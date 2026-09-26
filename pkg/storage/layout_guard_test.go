package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestReservedStorageNamesIgnoreTrailingDotsAndSpaces: Windows strips trailing
// dots and spaces from a name, so ".dht_nodes." opened the DHT routing table
// there while the case-folded check still let it through.
func TestReservedStorageNamesIgnoreTrailingDotsAndSpaces(t *testing.T) {
	for _, path := range []string{".dht_nodes.", ".dht_nodes ", ".DHT_NODES. .", ".abc.state.", ".abc.STATE  "} {
		files := []FileInfo{{Path: path, Length: 1}}
		if _, err := NewFileStorage(t.TempDir(), files, 16); err == nil || !strings.Contains(err.Error(), "reserved internal name") {
			t.Errorf("NewFileStorage(%q) = %v, want reserved-name rejection", path, err)
		}
		if _, err := NewMemStorage(t.TempDir(), files, 16); err == nil || !strings.Contains(err.Error(), "reserved internal name") {
			t.Errorf("NewMemStorage(%q) = %v, want reserved-name rejection", path, err)
		}
	}
	for _, path := range []string{".dht_nodes.bak", "movie.state.x", ".state.x"} {
		st, err := NewFileStorage(t.TempDir(), []FileInfo{{Path: path, Length: 1}}, 16)
		if err != nil {
			t.Errorf("NewFileStorage(%q) = %v, want it accepted", path, err)
			continue
		}
		_ = st.Close()
	}
}

// TestStorageRejectsTrailingSeparator: os.Root followed a final symlink for a
// path ending in a separator (GO-2026-4970). Torrent paths never end in one,
// so the layout refuses any that does instead of relying on the toolchain.
func TestStorageRejectsTrailingSeparator(t *testing.T) {
	for _, path := range []string{"dir/", "dir" + string(filepath.Separator), "a/b/"} {
		files := []FileInfo{{Path: path, Length: 1}}
		dir := t.TempDir()
		if _, err := NewFileStorage(dir, files, 16); err == nil || !strings.Contains(err.Error(), "separator") {
			t.Errorf("NewFileStorage(%q) = %v, want a trailing-separator rejection", path, err)
		}
		if entries, _ := os.ReadDir(dir); len(entries) != 0 {
			t.Errorf("NewFileStorage(%q) created %d entries before rejecting", path, len(entries))
		}
		if _, err := NewMemStorage(dir, files, 16); err == nil || !strings.Contains(err.Error(), "separator") {
			t.Errorf("NewMemStorage(%q) = %v, want a trailing-separator rejection", path, err)
		}
	}
}

// TestNewFileStorageRejectsPathsOpeningOneFile: names the path fold cannot
// model (8.3 short names, other normalization forms, hard links) can still open
// one file from two layouts, which then keep overwriting each other. A hard link
// stands in for them here, since it behaves the same on every filesystem.
func TestNewFileStorageRejectsPathsOpeningOneFile(t *testing.T) {
	dir := t.TempDir()
	original := []byte("user")
	if err := os.WriteFile(filepath.Join(dir, "a"), original, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(dir, "a"), filepath.Join(dir, "b")); err != nil {
		t.Skipf("hard links unsupported here: %v", err)
	}
	files := []FileInfo{{Path: "new", Length: 4}, {Path: "a", Length: 4}, {Path: "b", Length: 8}}
	if _, err := NewFileStorage(dir, files, 16); err == nil || !strings.Contains(err.Error(), "open the same file") {
		t.Fatalf("NewFileStorage over two names of one file = %v, want a duplicate rejection", err)
	}
	for _, name := range []string{"a", "b"} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || !bytes.Equal(got, original) {
			t.Fatalf("%s after the rejected add = %q, %v; want it untouched", name, got, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "new")); !os.IsNotExist(err) {
		t.Fatalf("file created by the rejected add was left behind: %v", err)
	}
}

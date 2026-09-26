package storage

import (
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

// TestSharedObjectAllowed: names the path fold cannot model (8.3 short names,
// other normalization forms) can still open one file from two layouts, which
// then keep overwriting each other, so NewFileStorage refuses a second layout on
// an object it already opened. It refused pre-existing hard links too, which a
// dedup tool leaves between identical payload files, so such a torrent could no
// longer be restored. Only those now pass: an alias landing on a file this call
// created, or on a pre-existing file with a single link, is still refused, and
// so are links the torrent gives two lengths, which no dedup tool leaves.
// A case-sensitive filesystem offers no alias a test can create, so the
// decision is checked directly here; TestNewFileStorageAcceptsPreexistingHardLinks,
// TestNewFileStorageRejectsHardLinksOfDifferentLengths and, on Windows,
// TestNewFileStorageRejectsShortNameAlias cover it end to end.
func TestSharedObjectAllowed(t *testing.T) {
	existing := openedObject{path: filepath.Join("disc1", "track.flac"), length: 16}
	created := openedObject{path: existing.path, length: 16, created: true}
	other := openedObject{path: filepath.Join("disc2", "track.flac"), length: 16}
	otherCreated := openedObject{path: other.path, length: 16, created: true}
	otherLonger := openedObject{path: other.path, length: 32}
	folded := openedObject{path: strings.ToUpper(existing.path), length: 16}
	for _, tc := range []struct {
		name  string
		first openedObject
		later openedObject
		links uint64
		want  bool
	}{
		{"pre-existing hard links", existing, other, 2, true},
		{"three pre-existing hard links", existing, other, 3, true},
		{"alias of a file this call created", created, other, 2, false},
		{"alias of a file this call created, one link", created, other, 1, false},
		{"second open created the file", existing, otherCreated, 2, false},
		{"alias of a pre-existing single-link file", existing, other, 1, false},
		{"unknown link count", existing, other, 0, false},
		{"paths equal under the fold", existing, folded, 2, false},
		{"hard links of different lengths", existing, otherLonger, 2, false},
	} {
		if got := sharedObjectAllowed(tc.first, tc.later, tc.links); got != tc.want {
			t.Errorf("%s: sharedObjectAllowed = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestResolveAndValidateRequiresDirectoryParents: the path check only refused
// symlinks, and Go reports a Windows junction as irregular rather than as a
// symlink, so a junction in the middle of a path passed. Every existing parent
// must now be a real directory; a regular file stands in for the junction here.
func TestResolveAndValidateRequiresDirectoryParents(t *testing.T) {
	base := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "file"), nil, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveAndValidatePath(base, filepath.Join("file", "x")); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("ResolveAndValidatePath through a non-directory = %v, want a refusal", err)
	}
	if _, err := ResolveAndValidatePath(base, "file"); err != nil {
		t.Fatalf("ResolveAndValidatePath(file) = %v, want the file itself accepted", err)
	}
	if _, err := ResolveAndValidatePath(base, filepath.Join("missing", "x")); err != nil {
		t.Fatalf("ResolveAndValidatePath(missing/x) = %v, want a path still to be created accepted", err)
	}
}

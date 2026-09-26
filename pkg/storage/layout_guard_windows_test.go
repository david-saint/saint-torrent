package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

// shortBaseName returns the final component of path's 8.3 short form, which is
// the long name itself on a volume that generates no short names.
func shortBaseName(t *testing.T, path string) string {
	t.Helper()
	long, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]uint16, windows.MAX_LONG_PATH)
	n, err := windows.GetShortPathName(long, &buf[0], uint32(len(buf)))
	if err != nil || n == 0 || n >= uint32(len(buf)) {
		t.Skipf("GetShortPathName(%s) = %d, %v", path, n, err)
	}
	return filepath.Base(windows.UTF16ToString(buf[:n]))
}

// TestNewFileStorageRejectsShortNameAlias: an 8.3 short name opens the same
// file as its long name, and the path fold cannot model it. Accepting
// pre-existing hard links must not let it through: the alias is refused both
// when this call created the file and when the file already existed with a
// single link.
func TestNewFileStorageRejectsShortNameAlias(t *testing.T) {
	const long = "longfilename.txt"
	probe := filepath.Join(t.TempDir(), long)
	if err := os.WriteFile(probe, nil, 0644); err != nil {
		t.Fatal(err)
	}
	short := shortBaseName(t, probe)
	if strings.EqualFold(short, long) {
		t.Skip("this volume generates no 8.3 short names")
	}
	files := []FileInfo{{Path: long, Length: 4}, {Path: short, Length: 4}}
	requireRefused := func(what, dir string) {
		t.Helper()
		st, err := NewFileStorage(dir, files, 16)
		if err == nil {
			_ = st.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "open the same file") {
			t.Fatalf("NewFileStorage over %s and its short name %s = %v, want a duplicate rejection", what, short, err)
		}
	}

	dir := t.TempDir()
	requireRefused("a file it creates", dir)
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("the rejected add left %d entries behind", len(entries))
	}

	original := []byte("user")
	if err := os.WriteFile(filepath.Join(dir, long), original, 0644); err != nil {
		t.Fatal(err)
	}
	if got := shortBaseName(t, filepath.Join(dir, long)); !strings.EqualFold(got, short) {
		t.Skipf("short name %s differs from the probe's %s", got, short)
	}
	requireRefused("a pre-existing single-link file", dir)
	if got, err := os.ReadFile(filepath.Join(dir, long)); err != nil || !bytes.Equal(got, original) {
		t.Fatalf("pre-existing file after the rejected add = %q, %v; want it untouched", got, err)
	}
}

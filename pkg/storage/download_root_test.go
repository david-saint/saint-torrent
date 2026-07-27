package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenDownloadRootCreatesAnchoredDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "one", "two")
	root, err := OpenDownloadRoot(dir)
	if err != nil {
		t.Fatalf("OpenDownloadRoot: %v", err)
	}
	defer root.Close()

	f, err := root.OpenFile("probe", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("create through root: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "probe")); err != nil {
		t.Fatalf("anchored file missing: %v", err)
	}
}

func TestMkdirAllInRootCreatesNestedPath(t *testing.T) {
	root, err := OpenDownloadRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := mkdirAllInRoot(root, filepath.Join("a", "b", "c"), 0755); err != nil {
		t.Fatalf("mkdirAllInRoot: %v", err)
	}
	if info, err := root.Stat(filepath.Join("a", "b", "c")); err != nil || !info.IsDir() {
		t.Fatalf("nested directory not created: info=%v err=%v", info, err)
	}
}

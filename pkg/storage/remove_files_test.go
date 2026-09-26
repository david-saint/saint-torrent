package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTree(t *testing.T, base string, paths ...string) {
	t.Helper()
	for _, path := range paths {
		full := filepath.Join(base, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(path), 0644); err != nil {
			t.Fatal(err)
		}
	}
}

func requireExists(t *testing.T, base string, paths ...string) {
	t.Helper()
	for _, path := range paths {
		if _, err := os.Lstat(filepath.Join(base, filepath.FromSlash(path))); err != nil {
			t.Errorf("%s should still exist: %v", path, err)
		}
	}
}

func requireGone(t *testing.T, base string, paths ...string) {
	t.Helper()
	for _, path := range paths {
		if _, err := os.Lstat(filepath.Join(base, filepath.FromSlash(path))); !os.IsNotExist(err) {
			t.Errorf("%s should be gone: %v", path, err)
		}
	}
}

func TestRemoveFilesDeletesFilesAndEmptyDirectories(t *testing.T) {
	base := t.TempDir()
	writeTree(t, base, "t/a/1", "t/a/2", "t/b/c/3", "top", "t/keep", "other/x")
	rel := []string{
		filepath.FromSlash("t/b/c/3"), filepath.FromSlash("t/a/2"), "top", filepath.FromSlash("t/a/1"),
		filepath.FromSlash("t/missing"), filepath.FromSlash("gone/dir/file"), "top",
	}
	removed, err := RemoveFiles(base, rel)
	if err != nil || removed != 4 {
		t.Fatalf("RemoveFiles = %d, %v; want 4 files and no error", removed, err)
	}
	requireGone(t, base, "t/a", "t/b", "top")
	requireExists(t, base, "t/keep", "other/x")

	removed, err = RemoveFiles(base, []string{filepath.FromSlash("t/keep")})
	if err != nil || removed != 1 {
		t.Fatalf("RemoveFiles(t/keep) = %d, %v; want 1 and no error", removed, err)
	}
	requireGone(t, base, "t")
	requireExists(t, base, ".", "other/x")
}

// TestRemoveFilesCreatesNothing: removal from a download directory that is
// gone must not recreate it (or anything else) on the way.
func TestRemoveFilesCreatesNothing(t *testing.T) {
	base := filepath.Join(t.TempDir(), "unmounted", "downloads")
	removed, err := RemoveFiles(base, []string{"a"})
	if err != nil || removed != 0 {
		t.Fatalf("RemoveFiles(missing base) = %d, %v; want nothing to do", removed, err)
	}
	requireGone(t, filepath.Dir(base), ".")
}

func TestRemoveFilesRefusesPathsOutsideTheRoot(t *testing.T) {
	parent := t.TempDir()
	base := filepath.Join(parent, "dl")
	writeTree(t, parent, "victim", "dl/dir/inside")
	for _, rel := range []string{filepath.Join("..", "victim"), filepath.Join(parent, "victim"), "", ".", "dir" + string(filepath.Separator)} {
		removed, err := RemoveFiles(base, []string{rel})
		if err == nil || removed != 0 {
			t.Errorf("RemoveFiles(%q) = %d, %v; want a refusal", rel, removed, err)
		}
	}
	requireExists(t, parent, "victim", "dl/dir/inside")
}

// TestRemoveFilesRefusesSymlinks: "delete with files" removed each path by name
// after a separate Lstat walk, so a symlink planted in the tree, or a directory
// swapped for one between the check and the delete, redirected the delete to
// files outside the download directory.
func TestRemoveFilesRefusesSymlinks(t *testing.T) {
	parent := t.TempDir()
	base := filepath.Join(parent, "dl")
	writeTree(t, parent, "outside/victim", "dl/real/victim", "dl/realdir/victim")
	links := map[string]string{
		"dl/escape":  filepath.Join(parent, "outside"),         // directory link out of the root
		"dl/inner":   filepath.Join(base, "realdir"),           // directory link inside the root
		"dl/fileref": filepath.Join(base, "real", "victim"),    // final-component link
		"dl/rel":     filepath.Join("..", "outside", "victim"), // relative final link out
	}
	for link, target := range links {
		if err := os.Symlink(target, filepath.Join(parent, filepath.FromSlash(link))); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
	}
	rel := []string{
		filepath.Join("escape", "victim"), filepath.Join("inner", "victim"), "fileref", "rel",
	}
	removed, err := RemoveFiles(base, rel)
	if removed != 0 || err == nil {
		t.Fatalf("RemoveFiles through links = %d, %v; want every path refused", removed, err)
	}
	for _, want := range []string{"escape", "inner", "fileref", "rel"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not report %s: %v", want, err)
		}
	}
	requireExists(t, parent, "outside/victim", "dl/real/victim", "dl/realdir/victim", "dl/escape", "dl/inner", "dl/fileref", "dl/rel")
}

// BenchmarkRemoveFilesDeepTree shows the walk validates each directory once:
// removing 200 files 16 directories deep costs about what it does at depth 1,
// where re-walking every path would multiply the lookups by the depth.
func BenchmarkRemoveFilesDeepTree(b *testing.B) {
	for _, depth := range []int{1, 16} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			dir := filepath.Join(strings.Split(strings.Repeat("d/", depth), "/")...)
			rel := make([]string, 200)
			for i := range rel {
				rel[i] = filepath.Join(dir, fmt.Sprintf("f%03d", i))
			}
			for b.Loop() {
				b.StopTimer()
				base := b.TempDir()
				if err := os.MkdirAll(filepath.Join(base, dir), 0755); err != nil {
					b.Fatal(err)
				}
				for _, path := range rel {
					if err := os.WriteFile(filepath.Join(base, path), nil, 0644); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if removed, err := RemoveFiles(base, rel); err != nil || removed != len(rel) {
					b.Fatalf("RemoveFiles = %d, %v", removed, err)
				}
			}
		})
	}
}

package fsync

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileSyncsFileAndDirectory(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString("payload"); err != nil {
		t.Fatal(err)
	}
	if err := File(f); err != nil {
		t.Fatalf("File(regular file) = %v", err)
	}

	d, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	// Some platforms refuse to sync a directory; callers treat it as best
	// effort, so only a panic or a hang would be a failure here.
	_ = File(d)
}

func TestFileRejectsNilAndClosed(t *testing.T) {
	if err := File(nil); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("File(nil) = %v, want os.ErrInvalid", err)
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "closed"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := File(f); err == nil {
		t.Fatal("File(closed file) = nil, want an error")
	}
}

// TestNoBareSync keeps every flush in the module on File: a bare
// (*os.File).Sync on macOS blocks the GC for as long as the disk takes, which
// froze the TUI for minutes on a USB-attached download volume.
func TestNoBareSync(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	self, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	visit := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); path == self || name == "testdata" || strings.HasPrefix(name, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 0 {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Sync" {
				t.Errorf("%s: bare .Sync() call; use fsync.File so macOS does not stall the GC", fset.Position(call.Pos()))
			}
			return true
		})
		return nil
	}
	for _, dir := range []string{"cmd", "pkg", "internal"} {
		if err := filepath.WalkDir(filepath.Join(root, dir), visit); err != nil {
			t.Fatal(err)
		}
	}
}

package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// writeLegacyFile writes data at rel under dir, as an older version laid the
// file out, and skips the test where this filesystem cannot hold the name.
func writeLegacyFile(t *testing.T, dir, rel string, data []byte) {
	t.Helper()
	path := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Skipf("this filesystem cannot hold %q: %v", rel, err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Skipf("this filesystem cannot hold %q: %v", rel, err)
	}
}

// TestMoveLegacyFiles: files an older version wrote under names the current
// sanitizer changes are moved to the current names with their contents, and
// the old directories they leave empty go away.
func TestMoveLegacyFiles(t *testing.T) {
	for name, move := range map[string]struct{ from, to string }{
		"RLM":             {"Movie \u200f(RLM).mkv", "Movie (RLM).mkv"},
		"BOM":             {"ep1\ufeff.mkv", "ep1.mkv"},
		"invalid UTF-8":   {"Caf\xe9 Latin1.mkv", "Caf_ Latin1.mkv"},
		"control":         {"Tab\tName.txt", "Tab_Name.txt"},
		"directory":       {filepath.Join("Show \u200bZWSP", "Season 1", "ep1\ufeff.mkv"), filepath.Join("Show ZWSP", "Season 1", "ep1.mkv")},
		"file under root": {filepath.Join("Show", "ep\x01.mkv"), filepath.Join("Show", "ep_.mkv")},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			data := []byte("verified payload")
			writeLegacyFile(t, dir, move.from, data)
			moves := []LegacyMove{{From: move.from, To: move.to, Length: int64(len(data))}}
			moved, err := MoveLegacyFiles(dir, moves)
			if err != nil || len(moved) != 1 {
				t.Fatalf("MoveLegacyFiles = %v, %v; want the file moved", moved, err)
			}
			if got, err := os.ReadFile(filepath.Join(dir, move.to)); err != nil || !bytes.Equal(got, data) {
				t.Fatalf("new path holds %q, %v; want the old file's contents", got, err)
			}
			if _, err := os.Lstat(filepath.Join(dir, move.from)); !os.IsNotExist(err) {
				t.Fatalf("old path still exists: %v", err)
			}
			if top := topComponent(move.from); top != topComponent(move.to) {
				if _, err := os.Lstat(filepath.Join(dir, top)); !os.IsNotExist(err) {
					t.Fatalf("emptied old directory %q kept: %v", top, err)
				}
			}
		})
	}
}

// TestMoveLegacyFilesLeavesOthersAlone: a move replaces nothing, follows no
// symlink and takes only a file of the torrent's length.
func TestMoveLegacyFilesLeavesOthersAlone(t *testing.T) {
	data := []byte("payload")
	length := int64(len(data))

	t.Run("new path exists", func(t *testing.T) {
		dir := t.TempDir()
		writeLegacyFile(t, dir, "old\u200b.bin", data)
		current := []byte("current")
		writeLegacyFile(t, dir, "old.bin", current)
		if moved, err := MoveLegacyFiles(dir, []LegacyMove{{From: "old\u200b.bin", To: "old.bin", Length: length}}); err != nil || len(moved) != 0 {
			t.Fatalf("MoveLegacyFiles = %v, %v; want nothing moved", moved, err)
		}
		if got, _ := os.ReadFile(filepath.Join(dir, "old.bin")); !bytes.Equal(got, current) {
			t.Fatalf("existing file replaced with %q", got)
		}
		if _, err := os.Lstat(filepath.Join(dir, "old\u200b.bin")); err != nil {
			t.Fatalf("old file gone: %v", err)
		}
	})

	t.Run("other length", func(t *testing.T) {
		dir := t.TempDir()
		writeLegacyFile(t, dir, "old\u200b.bin", data)
		if moved, err := MoveLegacyFiles(dir, []LegacyMove{{From: "old\u200b.bin", To: "old.bin", Length: length + 1}}); err != nil || len(moved) != 0 {
			t.Fatalf("MoveLegacyFiles = %v, %v; want nothing moved", moved, err)
		}
		if _, err := os.Lstat(filepath.Join(dir, "old.bin")); !os.IsNotExist(err) {
			t.Fatalf("new path created: %v", err)
		}
	})

	t.Run("reserved name", func(t *testing.T) {
		dir := t.TempDir()
		writeLegacyFile(t, dir, ".dht_nodes\u200b", data)
		if moved, _ := MoveLegacyFiles(dir, []LegacyMove{{From: ".dht_nodes\u200b", To: ".dht_nodes", Length: length}}); len(moved) != 0 {
			t.Fatal("a file was moved onto a reserved storage name")
		}
	})

	t.Run("outside the directory", func(t *testing.T) {
		parent := t.TempDir()
		dir := filepath.Join(parent, "downloads")
		writeLegacyFile(t, parent, "outside.bin", data)
		writeLegacyFile(t, dir, "inside.bin", data)
		for _, move := range []LegacyMove{
			{From: filepath.Join("..", "outside.bin"), To: "moved.bin", Length: length},
			{From: "inside.bin", To: filepath.Join("..", "escaped.bin"), Length: length},
		} {
			if moved, _ := MoveLegacyFiles(dir, []LegacyMove{move}); len(moved) != 0 {
				t.Fatalf("move %q -> %q crossed the download directory", move.From, move.To)
			}
		}
		if _, err := os.Lstat(filepath.Join(parent, "outside.bin")); err != nil {
			t.Fatalf("file outside the download directory moved: %v", err)
		}
	})

	t.Run("missing directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "unmounted")
		if moved, err := MoveLegacyFiles(dir, []LegacyMove{{From: "a\u200b", To: "a", Length: length}}); err != nil || len(moved) != 0 {
			t.Fatalf("MoveLegacyFiles = %v, %v; want nothing", moved, err)
		}
		if _, err := os.Lstat(dir); !os.IsNotExist(err) {
			t.Fatalf("missing download directory created: %v", err)
		}
	})

	t.Run("symlinks", func(t *testing.T) {
		dir := t.TempDir()
		elsewhere := t.TempDir()
		writeLegacyFile(t, elsewhere, "target.bin", data)
		writeLegacyFile(t, dir, "real\u200b.bin", data)
		if err := os.Symlink(filepath.Join(elsewhere, "target.bin"), filepath.Join(dir, "link\u200b.bin")); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if err := os.Symlink(elsewhere, filepath.Join(dir, "dir\u200b")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(elsewhere, filepath.Join(dir, "newdir")); err != nil {
			t.Fatal(err)
		}
		moves := []LegacyMove{
			// A symlinked old file, or one under a symlinked directory, is not
			// the file an older version wrote.
			{From: "link\u200b.bin", To: "link.bin", Length: length},
			{From: filepath.Join("dir\u200b", "target.bin"), To: filepath.Join("dir", "target.bin"), Length: length},
			// A symlinked directory on the new side is not followed.
			{From: "real\u200b.bin", To: filepath.Join("newdir", "real.bin"), Length: length},
		}
		moved, err := MoveLegacyFiles(dir, moves)
		if len(moved) != 0 {
			t.Fatalf("moved %v through a symlink", moved)
		}
		if err == nil {
			t.Fatal("symlinked directories were not reported")
		}
		for _, name := range []string{"link.bin", "dir"} {
			if _, err := os.Lstat(filepath.Join(dir, name)); !os.IsNotExist(err) {
				t.Fatalf("%s created: %v", name, err)
			}
		}
		if _, err := os.Lstat(filepath.Join(elsewhere, "real.bin")); !os.IsNotExist(err) {
			t.Fatalf("file moved into the symlinked directory: %v", err)
		}
		if got, err := os.ReadFile(filepath.Join(elsewhere, "target.bin")); err != nil || !bytes.Equal(got, data) {
			t.Fatalf("symlink target changed: %q, %v", got, err)
		}
	})
}

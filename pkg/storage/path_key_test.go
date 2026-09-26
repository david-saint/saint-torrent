package storage

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// TestStorageRejectsFoldedDuplicatePaths: on APFS and NTFS two layouts whose
// names differ only in case or Unicode normalization open the same file, and
// the two entries then keep re-truncating it to their own lengths.
func TestStorageRejectsFoldedDuplicatePaths(t *testing.T) {
	for name, paths := range map[string][2]string{
		"NFC vs NFD":  {"root/caf\u00e9", "root/cafe\u0301"},
		"final sigma": {"root/\u03c3", "root/\u03c2"},
		"case":        {"root/A.txt", "ROOT/a.TXT"},
	} {
		files := []FileInfo{{Path: paths[0], Length: 1}, {Path: paths[1], Length: 2}}
		dir := t.TempDir()
		if _, err := NewFileStorage(dir, files, 16); err == nil || !strings.Contains(err.Error(), "duplicate file path") {
			t.Errorf("%s: NewFileStorage = %v, want duplicate path error", name, err)
		}
		if entries, _ := os.ReadDir(dir); len(entries) != 0 {
			t.Errorf("%s: rejected layout still created %d entries", name, len(entries))
		}
		if _, err := NewMemStorage(dir, files, 16); err == nil || !strings.Contains(err.Error(), "duplicate file path") {
			t.Errorf("%s: NewMemStorage = %v, want duplicate path error", name, err)
		}
	}
}

// TestReservedStorageNamesAreCaseFolded: on a case-insensitive filesystem a
// torrent file named ".DHT_NODES" would overwrite the DHT routing table.
func TestReservedStorageNamesAreCaseFolded(t *testing.T) {
	for _, path := range []string{".DHT_NODES", ".Dht_Nodes", ".ABCDEF.STATE", ".x.State"} {
		if _, err := NewFileStorage(t.TempDir(), []FileInfo{{Path: path, Length: 1}}, 16); err == nil || !strings.Contains(err.Error(), "reserved internal name") {
			t.Errorf("NewFileStorage(%q) = %v, want reserved-name rejection", path, err)
		}
	}
	st, err := NewFileStorage(t.TempDir(), []FileInfo{{Path: filepath.Join("movie", ".DHT_NODES"), Length: 1}}, 16)
	if err != nil {
		t.Fatalf("nested reserved-looking name rejected: %v", err)
	}
	_ = st.Close()
}

// TestNewFileStorageRejectsPieceCountOverflow is the 32-bit guard: 2^32+1
// one-byte pieces truncated to one piece in pieceCount, so almost the whole
// payload fell outside every piece and resume loading indexed out of range.
func TestNewFileStorageRejectsPieceCountOverflow(t *testing.T) {
	if math.MaxInt > math.MaxInt32 {
		t.Skip("the piece count only overflows int on 32-bit builds")
	}
	dir := t.TempDir()
	files := []FileInfo{
		{Path: "a", Length: math.MaxInt32},
		{Path: "b", Length: math.MaxInt32},
		{Path: "c", Length: 3},
	}
	if _, err := NewFileStorage(dir, files, 1); err == nil || !strings.Contains(err.Error(), "overflows int") {
		t.Fatalf("NewFileStorage(2^32+1 one-byte pieces) = %v, want overflow rejection", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("rejected layout still created %d entries", len(entries))
	}
}

// TestPathKeyASCIIShortcutMatchesUnicodeFold: the ASCII shortcut must give
// exactly the key NFC over full case folding gives. A mismatch would let a
// restore, or the downloader's path claims, disagree with the add.
func TestPathKeyASCIIShortcutMatchesUnicodeFold(t *testing.T) {
	reference := func(p string) string {
		return norm.NFC.String(cases.Fold().String(norm.NFD.String(filepath.Clean(p))))
	}
	inputs := []string{"Some.Show.S01/E01.MKV", "../A/./b//C/", "café/A", "K.txt", "ß"}
	for c := 0; c < utf8.RuneSelf; c++ {
		s := string(rune(c))
		inputs = append(inputs, s, "Dir/"+s+"x.MKV", "a"+s+"Z")
	}
	for _, p := range inputs {
		if got, want := pathKey(p), reference(p); got != want {
			t.Errorf("pathKey(%q) = %q, want %q", p, got, want)
		}
	}
}

// BenchmarkPathKey measures the key computed per file at every add, restore
// and path claim.
func BenchmarkPathKey(b *testing.B) {
	for name, p := range map[string]string{
		"ascii":   "/home/user/Downloads/Some.Show.S01.1080p.WEB-DL/Some.Show.S01E01.1080p.WEB-DL.mkv",
		"unicode": "/home/user/Downloads/Café Tacvba/Re/01 - El Ciclón.flac",
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = PathKey(p)
			}
		})
	}
}

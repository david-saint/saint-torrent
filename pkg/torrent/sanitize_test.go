package torrent

import (
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

func TestSanitizeComponent(t *testing.T) {
	long := strings.Repeat("A", 300)
	for _, tc := range []struct {
		name, in, want string
	}{
		{"plain", "movie.mkv", "movie.mkv"},
		// U+202E made "invoice<U+202E>fdp.exe" read as "invoiceexe.pdf".
		{"bidi override", "invoice\u202efdp.exe", "invoicefdp.exe"},
		{"bidi isolates and marks", "\u2066a\u2069\u200eb\u200f\u061c", "ab"},
		{"zero-width", "a\u200bb\u2060c\ufeff", "abc"},
		{"C0 controls", "line\nbreak\x1b[31m\x00", "line_break_[31m_"},
		{"DEL and C1", "a\x7fb\u0085c", "a_b_c"},
		{"invalid UTF-8", "a\x80\xffb", "a__b"},
		{"literal replacement char kept", "a\ufffdb", "a\ufffdb"},
		{"invisible dot-dot", "\u200b..\u200b", "safe_name"},
		{"only invisible", "\u202e", "safe_name"},
		{"traversal", "..", "safe_name"},
		{"separators", "a/b\\c", "a_b_c"},
		{"inner dot-dot", "a..b", "a_b"},
		{"not windows: device name kept", "CON", "CON"},
		{"not windows: colon kept", "a:b", "a:b"},
		{"not windows: trailing dot kept", "a.", "a."},
		{"truncated keeping extension", long + ".mkv", strings.Repeat("A", 251) + ".mkv"},
		{"long extension not kept", "a." + long, "a." + strings.Repeat("A", 253)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sanitizeComponent(tc.in, false); got != tc.want {
				t.Fatalf("sanitizeComponent(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSanitizeComponentTruncatesOnRuneBoundary(t *testing.T) {
	for _, windows := range []bool{false, true} {
		for _, in := range []string{
			strings.Repeat("\u00e9", 200),             // 2-byte runes
			"x" + strings.Repeat("\u8a9e", 120),       // 3-byte runes, offset by one
			strings.Repeat("\U0001f3ac", 80) + ".mkv", // 4-byte runes with an extension
		} {
			got := sanitizeComponent(in, windows)
			if len(got) > maxComponentBytes || !utf8.ValidString(got) {
				t.Fatalf("sanitizeComponent(%d bytes, windows=%v) = %d bytes, valid=%v", len(in), windows, len(got), utf8.ValidString(got))
			}
			if strings.HasSuffix(in, ".mkv") && !strings.HasSuffix(got, ".mkv") {
				t.Fatalf("extension lost: %q", got)
			}
		}
	}
}

// TestSanitizeComponentWindows covers the Win32 rules: device names open the
// device instead of a file, ':' selects an alternate data stream, and trailing
// dots and spaces are stripped by the OS, so "a." and "a" alias one file.
func TestSanitizeComponentWindows(t *testing.T) {
	for _, tc := range []struct {
		in, want string
	}{
		{"movie.mkv", "movie.mkv"},
		{"CON", "_CON"},
		{"con.txt", "_con.txt"},
		{"Nul.tar.gz", "_Nul.tar.gz"},
		{"NUL .txt", "_NUL .txt"},
		{"CON.", "_CON"},
		{"COM1", "_COM1"},
		{"com0.log", "_com0.log"},
		{"LPT9", "_LPT9"},
		{"COM\u00b9", "_COM\u00b9"},
		{"CONOUT$", "_CONOUT$"},
		{"CONSOLE", "CONSOLE"},
		{"COM10", "COM10"},
		{"COM", "COM"},
		{"xCON", "xCON"},
		{"a:b", "a_b"},
		{`<>:"|?*`, "_______"},
		{"file:stream", "file_stream"},
		{"name. . ", "name"},
		{"a.", "a"},
		{"...", "_"},
		{" ", "_"},
		{"a\u202e.", "a"},
	} {
		if got := sanitizeComponent(tc.in, true); got != tc.want {
			t.Errorf("sanitizeComponent(%q, windows) = %q, want %q", tc.in, got, tc.want)
		}
	}
	// A prefixed device name still fits the component limit.
	got := sanitizeComponent("CON."+strings.Repeat("x", 300), true)
	if len(got) > maxComponentBytes || !strings.HasPrefix(got, "_CON.") {
		t.Fatalf("long device name = %q (%d bytes), want _CON. prefix within %d bytes", got[:8], len(got), maxComponentBytes)
	}
	// Names Windows would alias now sanitize to the same string, which the
	// duplicate-path check then catches.
	if sanitizeComponent("a. ", true) != sanitizeComponent("a", true) {
		t.Fatal("trailing dot/space alias not canonicalized")
	}
}

// FuzzSanitizeComponent checks the sanitizer's output contract for any input.
func FuzzSanitizeComponent(f *testing.F) {
	for _, s := range []string{"", ".", "..", "a/../b", "CON.txt", "a. ", "x\u202ey", strings.Repeat("\u00e9", 200) + ".mkv", "\xff\xfe"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, in string) {
		for _, windows := range []bool{false, true} {
			got := sanitizeComponent(in, windows)
			if !safeName(got) || got == "" || got == "." || got == ".." || strings.ContainsAny(got, `/\`) {
				t.Fatalf("sanitizeComponent(%q, %v) = %q, unsafe", in, windows, got)
			}
			if windows && (strings.ContainsAny(got, `<>:"|?*`) || strings.HasSuffix(got, ".") || strings.HasSuffix(got, " ") || isWindowsDeviceName(got)) {
				t.Fatalf("sanitizeComponent(%q, windows) = %q, invalid on Windows", in, got)
			}
		}
	})
}

func TestParseRejectsCaseAndNormalizationAliases(t *testing.T) {
	for name, paths := range map[string][2]string{
		"NFC vs NFD":         {"caf\u00e9", "cafe\u0301"},
		"final sigma":        {"\u03c3", "\u03c2"},
		"case":               {"Readme.TXT", "README.txt"},
		"full case folding":  {"stra\u00dfe", "STRASSE"},
		"invisible variants": {"ab", "a\u200bb"},
	} {
		info := singleFileInfo("root", 16, 2)
		delete(info, "length")
		info["files"] = []interface{}{
			map[string]interface{}{"length": int64(1), "path": []interface{}{paths[0]}},
			map[string]interface{}{"length": int64(1), "path": []interface{}{paths[1]}},
		}
		if _, err := Parse(marshalTorrent(t, info)); err == nil || !strings.Contains(err.Error(), "duplicate file path") {
			t.Errorf("%s: Parse(%q, %q) = %v, want duplicate path rejection", name, paths[0], paths[1], err)
		}
	}
}

func TestParseSanitizesNames(t *testing.T) {
	// A single file named with an RTL override: both the display name and the
	// on-disk name lose it.
	tor, err := Parse(marshalTorrent(t, singleFileInfo("photo\u202egpj.exe", 16, 16)))
	if err != nil {
		t.Fatalf("Parse() = %v", err)
	}
	if tor.Name != "photogpj.exe" || tor.Files[0].Path[0] != "photogpj.exe" {
		t.Fatalf("Name=%q path=%q, want the override removed", tor.Name, tor.Files[0].Path[0])
	}

	// A megabyte-long name (ut_metadata allows 16 MiB) is cut for display and
	// disk alike.
	huge := strings.Repeat("n", 1<<20) + ".iso"
	info := singleFileInfo(huge, 16, 2)
	delete(info, "length")
	info["files"] = []interface{}{
		map[string]interface{}{"length": int64(1), "path": []interface{}{"dir", strings.Repeat("p", 4096)}},
		map[string]interface{}{"length": int64(1), "path": []interface{}{"x\x1b]0;title\x07"}},
	}
	tor, err = Parse(marshalTorrent(t, info))
	if err != nil {
		t.Fatalf("Parse() = %v", err)
	}
	if len(tor.Name) > maxComponentBytes || !strings.HasSuffix(tor.Name, ".iso") {
		t.Fatalf("Name is %d bytes (suffix %q), want at most %d keeping .iso", len(tor.Name), tor.Name[len(tor.Name)-4:], maxComponentBytes)
	}
	for _, f := range tor.Files {
		for _, comp := range f.Path {
			if len(comp) > maxComponentBytes || strings.ContainsAny(comp, "\x1b\x07") {
				t.Fatalf("unsafe path component %q (%d bytes)", comp[:min(len(comp), 16)], len(comp))
			}
		}
	}
}

func TestParseMagnetSanitizesName(t *testing.T) {
	ml, err := ParseMagnet("magnet:?xt=urn:btih:" + testHashHex + "&dn=clip%E2%80%AEvaw.scr%1B%5B2J")
	if err != nil {
		t.Fatalf("ParseMagnet() = %v", err)
	}
	if ml.Name != "clipvaw.scr_[2J" {
		t.Fatalf("Name = %q, want the override removed and ESC replaced", ml.Name)
	}
}

// TestPathKeyASCIIShortcutMatchesUnicodeFold: the ASCII shortcut must give
// exactly the key NFC over full case folding gives, or Parse would miss (or
// invent) duplicates that storage.PathKey sees.
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

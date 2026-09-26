package main

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"
	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/downloader"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// writeTestTorrent bencodes info into a .torrent file and returns its path.
func writeTestTorrent(t *testing.T, info map[string]interface{}) string {
	t.Helper()
	data, err := bencode.Marshal(map[string]interface{}{"announce": "http://127.0.0.1:1/announce", "info": info})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "crafted.torrent")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// An RLO in a file name makes bidi-aware terminals render "Movie<RLO>4pm.exe"
// as "Movieexe.mp4"; every hidden or reordering character must show up as a
// visible placeholder instead.
func TestSanitizeTextMakesHiddenCharactersVisible(t *testing.T) {
	const ph = string(hiddenRunePlaceholder)
	tests := []struct {
		input string
		want  string
	}{
		{"Movie\u202e4pm.exe", "Movie" + ph + "4pm.exe"},                                   // RLO
		{"a\u202ab\u202bc\u202cd\u202de", "a" + ph + "b" + ph + "c" + ph + "d" + ph + "e"}, // LRE RLE PDF LRO
		{"x\u2066y\u2067z\u2068w\u2069", "x" + ph + "y" + ph + "z" + ph + "w" + ph},        // isolates
		{"a\u200bb\u200cc\u200dd", "a" + ph + "b" + ph + "c" + ph + "d"},                   // zero-width
		{"\u200eleft\u200fright\u061c", ph + "left" + ph + "right" + ph},                   // marks
		{"\ufeffbom", ph + "bom"},
		{"line\u2028para\u2029end", "line" + ph + "para" + ph + "end"},
		{"vs\ufe0fsup\U000e0100", "vs" + ph + "sup" + ph},
		{"tag\U000e0001\U000e0041", "tag" + ph + ph},
		{"soft\u00adhyphen", "soft" + ph + "hyphen"},
		// Ordinary non-ASCII text is untouched.
		{"長い torrent 🚀 e\u0301 Ünïcödé", "長い torrent 🚀 e\u0301 Ünïcödé"},
		{"\u00a0nbsp trimmed\u00a0", "nbsp trimmed"},
	}
	for _, tt := range tests {
		if got := sanitizeText(tt.input); got != tt.want {
			t.Errorf("sanitizeText(%q) = %q; want %q", tt.input, got, tt.want)
		}
	}
}

func TestSanitizeTextIsIdempotent(t *testing.T) {
	for _, in := range []string{"a\x1b]0;x\x07 b", "Movie\u202e4pm.exe", "  x\t\ty  ", "\xff\xfe"} {
		once := sanitizeText(in)
		if twice := sanitizeText(once); twice != once {
			t.Errorf("sanitizeText not idempotent for %q: %q then %q", in, once, twice)
		}
	}
}

func TestBoundTextKeepsHeadTailOnRuneBoundaries(t *testing.T) {
	long := strings.Repeat("é", 1000) + ".exe" // 2-byte runes force boundary fixes
	got := boundText(long, 101)
	if len(got) > 101+len("…") {
		t.Fatalf("len = %d; want <= %d", len(got), 101+len("…"))
	}
	if !utf8.ValidString(got) {
		t.Fatalf("bounded text is not valid UTF-8: %q", got)
	}
	if !strings.HasPrefix(got, "éé") || !strings.HasSuffix(got, "é.exe") || !strings.Contains(got, "…") {
		t.Fatalf("bounded text lost its head, tail or marker: %q", got)
	}
	if short := "short"; boundText(short, 101) != short {
		t.Fatal("short text was changed")
	}
}

// A 16 MiB name (the ut_metadata cap) or a megabyte tracker failure reason
// must cost a bounded amount per tick and per frame.
func TestRowSnapshotBoundsHugeNameAndError(t *testing.T) {
	huge := strings.Repeat("A", 16<<20) + "\u202e4pm.exe"
	row := rowFromSnapshot(nil, downloader.SessionSnapshot{
		Name:      huge,
		LastError: errors.New("tracker error: " + strings.Repeat("\x1bB", 1<<20)),
	})
	if len(row.name) > maxDisplayBytes+len("…") {
		t.Fatalf("row name is %d bytes; want <= %d", len(row.name), maxDisplayBytes+len("…"))
	}
	if !strings.HasSuffix(row.name, string(hiddenRunePlaceholder)+"4pm.exe") {
		t.Fatalf("row name lost its tail or kept the RLO: %q", row.name[len(row.name)-20:])
	}
	if len(row.lastErrText) > maxDisplayBytes+len("…") || strings.ContainsRune(row.lastErrText, 0x1b) {
		t.Fatalf("last error not bounded and sanitized: %d bytes", len(row.lastErrText))
	}
}

// Headless startup warnings embed failed-add errors, which quote torrent
// paths; an OSC 52 clipboard write or title change hidden in a path must be
// printed as escapes, never as raw bytes.
func TestWriteHeadlessStartupMessagesEscapesTorrentText(t *testing.T) {
	const payload = "\x1b]52;c;ZWNobyBwd25lZA==\x07\x1b]0;spoofed\x07\x1b[2J"
	var out strings.Builder
	writeHeadlessStartupMessages(&out,
		[]string{"HTTP stats endpoint: http://127.0.0.1:16666/stats"},
		[]string{"Failed to load torrent x.torrent: duplicate file path detected in torrent metadata: x/" + payload + "\u202egpj.scr"})
	got := out.String()
	if strings.ContainsAny(got, "\x1b\x07\u202e") {
		t.Fatalf("raw control or bidi characters reached the terminal: %q", got)
	}
	for _, want := range []string{`\x1b]52;c;ZWNobyBwd25lZA==\x07`, `\u202egpj.scr`, "HTTP stats endpoint: http://127.0.0.1:16666/stats\n"} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q: %q", want, got)
		}
	}
}

// End to end: a crafted multi-file torrent whose duplicate path carries escape
// sequences fails to load in headless mode without the sequences reaching
// the terminal.
func TestHeadlessFailedAddOfCraftedTorrentIsEscaped(t *testing.T) {
	const evil = "\x1b]52;c;ZWNobyBwd25lZA==\x07\x1b]0;spoofed\x07"
	info := map[string]interface{}{
		"name":         "x",
		"piece length": int64(16384),
		"pieces":       string(make([]byte, 20)),
		"files": []interface{}{
			map[string]interface{}{"length": int64(1), "path": []interface{}{evil}},
			map[string]interface{}{"length": int64(1), "path": []interface{}{evil}},
		},
	}
	path := writeTestTorrent(t, info)

	mgr := downloader.NewTorrentManager()
	defer mgr.Close()
	_, err := addTorrentWithDownloadPaths(mgr, path, downloadPathOptions{primary: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "\x1b") {
		t.Skipf("parser no longer echoes the raw path (err=%q); nothing to escape", err)
	}
	var out strings.Builder
	writeHeadlessStartupMessages(&out, nil, []string{fmt.Sprintf("Failed to load torrent %s: %v", path, err)})
	if strings.ContainsAny(out.String(), "\x1b\x07") {
		t.Fatalf("raw escape sequence printed: %q", out.String())
	}
}

func TestEscapeForTerminal(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"plain text / path.torrent", "plain text / path.torrent"},
		{"長い 🚀 é", "長い 🚀 é"},
		{"a\x1b[2Jb", `a\x1b[2Jb`},
		{"line1\nline2\r\tx", `line1\nline2\r\tx`},
		{"del\x7fc1\u0085", `del\x7fc1\u0085`},
		{"rlo\u202eexe", `rlo\u202eexe`},
		{"tag\U000e0041", `tag\U000e0041`},
		{"bad\xffutf8", `bad\xffutf8`},
	}
	for _, tt := range tests {
		if got := escapeForTerminal(tt.input); got != tt.want {
			t.Errorf("escapeForTerminal(%q) = %q; want %q", tt.input, got, tt.want)
		}
	}
	if got := escapeForTerminal(strings.Repeat("x", 1<<20)); len(got) > maxTerminalBytes+len("…") {
		t.Fatalf("escaped output is %d bytes; want bounded", len(got))
	}
}

// The add-confirm prompt shows the torrent's own name; a huge or spoofing
// name must be bounded and its hidden characters made visible.
func TestAddConfirmDisplayNameIsBoundedAndVisible(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()
	m := initialModel(mgr, ".", "", nil)

	name := strings.Repeat("N", 1<<20) + "\u202e4pm.exe"
	item := "magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=" + strings.ReplaceAll(name, "\u202e", "%E2%80%AE")
	updated, _ := m.Update(addTorrentMsg{msg: socketMessage{Items: []string{item}, Confirm: true}})
	m = updated.(model)
	if len(m.pendingItems) != 1 {
		t.Fatalf("pending items = %d, want 1", len(m.pendingItems))
	}
	got := m.pendingItems[0].displayName
	if len(got) > maxDisplayBytes+len("…") || strings.ContainsRune(got, '\u202e') {
		t.Fatalf("display name not bounded/sanitized: %d bytes", len(got))
	}
	if out := ansi.Strip(m.View()); !strings.Contains(out, "Torrent Name") {
		t.Fatalf("confirm view missing name line:\n%s", out)
	}
}

// The file explorer and the delete prompt show torrent paths and names.
func TestFileExplorerShowsSpoofedExtensionVisibly(t *testing.T) {
	name := "Holiday_Photos\u202egpj.scr"
	tor := &torrent.Torrent{
		InfoHash:    sha1.Sum([]byte("bidi-explorer")),
		Name:        name,
		PieceLength: 16,
		PieceHashes: [][20]byte{sha1.Sum([]byte("p"))},
		Files:       []torrent.File{{Length: 16, Path: []string{name}}},
	}
	st, err := storage.NewMemStorage(t.TempDir(), []storage.FileInfo{{Path: filepath.Join("x", "f"), Length: 16}}, tor.PieceLength)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := downloader.NewSession(tor, st, [20]byte{}, 6881, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()
	mgr.AddSession(fmt.Sprintf("%x", tor.InfoHash), sess)

	m := initialModel(mgr, ".", "", nil)
	updated, _ := m.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	m = updated.(model)
	m.viewMode = viewFiles
	m.buildFilesSnapshot()
	out := ansi.Strip(m.View())
	if strings.ContainsRune(out, '\u202e') {
		t.Fatalf("RLO reached the file explorer:\n%s", out)
	}
	if !strings.Contains(out, "Holiday_Photos"+string(hiddenRunePlaceholder)+"gpj.scr") {
		t.Fatalf("spoofed name not shown with a visible placeholder:\n%s", out)
	}

	m.viewMode = viewList
	m.startDelete(false, viewList)
	if strings.ContainsRune(m.deleteTargetName, '\u202e') {
		t.Fatalf("delete prompt keeps the RLO: %q", m.deleteTargetName)
	}
}

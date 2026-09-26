package main

import (
	"crypto/sha1"
	"fmt"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"
	"sainttorrent/pkg/downloader"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// newManyFilesModel returns a model sized width x height whose only session
// lists n files, the first named first and the rest sample_NNNNNN.jpg, and
// which is already in the file explorer.
func newManyFilesModel(t *testing.T, n int, first string, width, height int) model {
	t.Helper()
	files := make([]torrent.File, n)
	files[0] = torrent.File{Length: 1, Path: []string{"pack", first}}
	for i := 1; i < n; i++ {
		files[i] = torrent.File{Length: 1, Path: []string{"pack", fmt.Sprintf("sample_%06d.jpg", i)}}
	}
	tor := &torrent.Torrent{
		InfoHash: sha1.Sum([]byte(fmt.Sprintf("many-files-%d", n))),
		Name:     "pack",
		// One piece spanning every file keeps session setup cheap for huge n.
		PieceLength: int64(n),
		PieceHashes: make([][20]byte, 1),
		Files:       files,
	}
	st, err := storage.NewMemStorage(t.TempDir(), []storage.FileInfo{{Path: "pack/f", Length: 1}}, tor.PieceLength)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := downloader.NewSession(tor, st, [20]byte{}, 6881, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mgr := downloader.NewTorrentManager()
	t.Cleanup(mgr.Close)
	mgr.AddSession(fmt.Sprintf("%x", tor.InfoHash), sess)

	m := initialModel(mgr, ".", "", nil)
	updated, _ := m.Update(tea.WindowSizeMsg{Width: width, Height: height})
	m = updated.(model)
	for _, key := range []tea.KeyMsg{{Type: tea.KeyEnter}, {Type: tea.KeyRunes, Runes: []rune("f")}} {
		updated, _ = m.Update(key)
		m = updated.(model)
	}
	if m.viewMode != viewFiles {
		t.Fatalf("viewMode = %v, want viewFiles", m.viewMode)
	}
	return m
}

// headerPinned reports whether the explorer header sits right under the
// banner (one or two lines, depending on the theme).
func headerPinned(out string) bool {
	lines := strings.SplitN(out, "\n", 4)
	for _, ln := range lines[:min(3, len(lines))] {
		if strings.Contains(ln, "File Explorer:") {
			return true
		}
	}
	return false
}

func pressKey(t *testing.T, m model, key tea.KeyMsg) model {
	t.Helper()
	updated, _ := m.Update(key)
	return updated.(model)
}

// With more files than rows, the view used to render all of them and the
// renderer kept only the last screenful: the header and a malicious first
// file (setup.exe ahead of innocuous samples) were never visible.
func TestFileExplorerWindowsRowsAndKeepsHeaderAndSelectionVisible(t *testing.T) {
	const height = 40
	m := newManyFilesModel(t, 60, "setup.exe", 120, height)

	out := ansi.Strip(m.View())
	lines := strings.Split(out, "\n")
	if len(lines) > height {
		t.Fatalf("view is %d lines on a %d-line terminal", len(lines), height)
	}
	if !headerPinned(out) {
		t.Fatalf("header not pinned at the top:\n%s", out)
	}
	if !strings.Contains(out, "setup.exe") {
		t.Fatalf("first file (selected) not visible:\n%s", out)
	}
	if !strings.Contains(out, "1–") || !strings.Contains(out, "of 60 files") {
		t.Fatalf("range indicator missing:\n%s", out)
	}

	m = pressKey(t, m, tea.KeyMsg{Type: tea.KeyEnd})
	if m.selectedFileIdx != 59 {
		t.Fatalf("end: selectedFileIdx = %d, want 59", m.selectedFileIdx)
	}
	out = ansi.Strip(m.View())
	if !strings.Contains(out, "sample_000059.jpg") || strings.Contains(out, "setup.exe") {
		t.Fatalf("end did not scroll to the last file:\n%s", out)
	}
	if !headerPinned(out) || !strings.Contains(out, "–60 of 60 files") {
		t.Fatalf("header or range lost after scrolling:\n%s", out)
	}

	m = pressKey(t, m, tea.KeyMsg{Type: tea.KeyHome})
	if m.selectedFileIdx != 0 {
		t.Fatalf("home: selectedFileIdx = %d, want 0", m.selectedFileIdx)
	}
	m = pressKey(t, m, tea.KeyMsg{Type: tea.KeyPgDown})
	if m.selectedFileIdx <= 1 || m.selectedFileIdx >= 59 {
		t.Fatalf("pgdown: selectedFileIdx = %d, want about one page", m.selectedFileIdx)
	}
	page := m.selectedFileIdx
	m = pressKey(t, m, tea.KeyMsg{Type: tea.KeyPgUp})
	if m.selectedFileIdx != 0 {
		t.Fatalf("pgup after pgdown (%d): selectedFileIdx = %d, want 0", page, m.selectedFileIdx)
	}
}

// A torrent can list hundreds of thousands of files; a frame must cost only
// the visible rows.
func TestFileExplorerFrameIsBoundedForHugeTorrents(t *testing.T) {
	const height = 30
	m := newManyFilesModel(t, 200000, "first.bin", 115, height)
	out := m.View()
	if n := strings.Count(out, "\n") + 1; n > height {
		t.Fatalf("view is %d lines; want <= %d", n, height)
	}
	if len(out) > 64<<10 {
		t.Fatalf("view is %d bytes; want a single screen", len(out))
	}
	if !strings.Contains(ansi.Strip(out), "of 200000 files") {
		t.Fatal("range indicator missing")
	}

	// Before the first WindowSizeMsg the height is unknown; that must not
	// mean "render every file".
	m.height = 0
	if n := strings.Count(m.View(), "\n") + 1; n > defaultViewHeight+1 {
		t.Fatalf("unsized view is %d lines", n)
	}
}

// The data tick used to rebuild the files snapshot, copying the whole
// priorities slice under the session lock, twice a second.
func TestFilesSnapshotNotRebuiltOnTickUnlessChanged(t *testing.T) {
	m := newManyFilesModel(t, 1000, "a.bin", 120, 40)
	before := &m.files.priorities[0]

	updated, _ := m.Update(tickMsg{})
	m = updated.(model)
	if &m.files.priorities[0] != before {
		t.Fatal("tick rebuilt an unchanged files snapshot")
	}

	// A priority toggle still refreshes it.
	m = pressKey(t, m, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("p")})
	if &m.files.priorities[0] == before || m.files.priorities[0] != downloader.PriorityHigh {
		t.Fatalf("toggle did not refresh the snapshot: %v", m.files.priorities[0])
	}
}

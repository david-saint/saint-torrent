package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"
	"sainttorrent/pkg/downloader"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func newResponsiveTestManager(t *testing.T) (*downloader.TorrentManager, *downloader.Session) {
	t.Helper()

	baseDir := t.TempDir()
	tor := &torrent.Torrent{
		InfoHash:    sha1.Sum([]byte("responsive-render-fixture")),
		Name:        "長い torrent name with emoji 🚀 and combining e\u0301",
		PieceLength: 16,
		PieceHashes: [][20]byte{
			sha1.Sum([]byte("piece-0")),
			sha1.Sum([]byte("piece-1")),
			sha1.Sum([]byte("piece-2")),
			sha1.Sum([]byte("piece-3")),
		},
		Files: []torrent.File{
			{Length: 32, Path: []string{"a-very-long-directory", "first-file.bin"}},
			{Length: 32, Path: []string{"second-file.bin"}},
		},
	}
	fileInfo := make([]storage.FileInfo, len(tor.Files))
	for i, f := range tor.Files {
		fileInfo[i] = storage.FileInfo{Path: filepath.Join(f.Path...), Length: f.Length}
	}
	st, err := storage.NewStorage(baseDir, fileInfo, tor.PieceLength)
	if err != nil {
		t.Fatalf("storage.NewStorage: %v", err)
	}
	sess, err := downloader.NewSession(tor, st, [20]byte{1, 2, 3}, 6881, baseDir)
	if err != nil {
		t.Fatalf("downloader.NewSession: %v", err)
	}
	sess.PieceStates[0] = downloader.PieceCompleted
	sess.PieceStates[1] = downloader.PieceDownloading
	sess.RecomputeStats()
	sess.Peers["203.0.113.10:51413"] = &downloader.PeerState{
		IP:            "203.0.113.10",
		Port:          51413,
		Active:        true,
		DownloadSpeed: 128 * 1024,
		UploadSpeed:   32 * 1024,
	}

	mgr := downloader.NewTorrentManager()
	mgr.AddSession(fmt.Sprintf("%x", tor.InfoHash), sess)
	t.Cleanup(mgr.Close)
	return mgr, sess
}

func addMockTorrents(t *testing.T, mgr *downloader.TorrentManager, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		infoHash := sha1.Sum([]byte(fmt.Sprintf("mock-torrent-%03d", i)))
		tor := &torrent.Torrent{
			InfoHash:    infoHash,
			Name:        fmt.Sprintf("Mock Torrent %03d", i),
			PieceLength: 16,
			PieceHashes: [][20]byte{sha1.Sum([]byte(fmt.Sprintf("piece-%03d", i)))},
			Files: []torrent.File{
				{Length: 16, Path: []string{fmt.Sprintf("mock-%03d.bin", i)}},
			},
		}
		baseDir := t.TempDir()
		st, err := storage.NewStorage(baseDir, []storage.FileInfo{{
			Path: tor.Files[0].Path[0], Length: tor.Files[0].Length,
		}}, tor.PieceLength)
		if err != nil {
			t.Fatalf("mock storage %d: %v", i, err)
		}
		sess, err := downloader.NewSession(tor, st, [20]byte{byte(i + 1)}, 6881, baseDir)
		if err != nil {
			t.Fatalf("mock session %d: %v", i, err)
		}
		mgr.AddSession(fmt.Sprintf("%x", infoHash), sess)
	}
}

func TestOuterBodyWidth(t *testing.T) {
	cases := []struct{ term, wantOuter, wantBody int }{
		{0, 115, 113},   // default before first resize
		{200, 115, 113}, // capped at the +15% ceiling
		{115, 115, 113},
		{100, 100, 98},
		{80, 80, 78},
		{50, 50, 48},
		{2, 2, 2}, // gutter collapses at tiny widths
		{1, 1, 1},
	}
	for _, c := range cases {
		if got := outerWidth(c.term); got != c.wantOuter {
			t.Errorf("outerWidth(%d)=%d want %d", c.term, got, c.wantOuter)
		}
		if got := bodyWidth(c.term); got != c.wantBody {
			t.Errorf("bodyWidth(%d)=%d want %d", c.term, got, c.wantBody)
		}
	}
}

// listColumns must pack exactly into the body and follow the documented
// breakpoints, including the tiny-width fallback where only NAME remains.
func TestListColumnsFit(t *testing.T) {
	for bw := 1; bw <= 200; bw++ {
		l := listColumns(bw)
		total := l.nameW
		if l.showAct {
			total += 1 + l.actW
		}
		if l.showSize {
			total += 1 + l.sizeW
		}
		if l.showDone {
			total += 1 + l.doneW
		}
		if l.showEta {
			total += 1 + l.etaW
		}
		if l.showStatus {
			total += 1 + l.statusW
		}
		if l.showSpeed {
			total += 1 + l.speedW
		}
		if total != bw {
			t.Fatalf("bw=%d total=%d (%+v)", bw, total, l)
		}

		switch {
		case bw >= 90:
			if !l.showAct || !l.showDone || !l.showEta || !l.showSize || !l.showStatus || !l.showSpeed || l.foldSpeed {
				t.Fatalf("bw=%d unexpected ultrawide layout: %+v", bw, l)
			}
		case bw >= 78:
			if !l.showAct || !l.showDone || l.showEta || !l.showSize || !l.showStatus || !l.showSpeed || l.foldSpeed {
				t.Fatalf("bw=%d unexpected wide layout: %+v", bw, l)
			}
		case bw >= 66:
			if !l.showAct || !l.showDone || l.showEta || !l.showSize || !l.showStatus || l.showSpeed || !l.foldSpeed {
				t.Fatalf("bw=%d unexpected medium layout: %+v", bw, l)
			}
		case bw >= 50:
			if !l.showAct || !l.showDone || l.showEta || l.showSize || !l.showStatus || l.showSpeed || l.foldSpeed {
				t.Fatalf("bw=%d unexpected compact layout: %+v", bw, l)
			}
		case bw >= 8:
			if l.showAct || !l.showDone || l.showEta || l.showSize || l.showStatus || l.showSpeed || l.foldSpeed {
				t.Fatalf("bw=%d unexpected narrow layout: %+v", bw, l)
			}
		default:
			if l.showAct || l.showDone || l.showEta || l.showSize || l.showStatus || l.showSpeed || l.foldSpeed {
				t.Fatalf("bw=%d unexpected tiny layout: %+v", bw, l)
			}
		}
		if l.nameW < 1 {
			t.Fatalf("bw=%d nameW=%d < 1", bw, l.nameW)
		}
	}
}

// TestViewMaxLineWidth is the core invariant: across both themes, every view and
// a wide range of terminal sizes, no rendered line may exceed outerWidth.
// TestListETAColumnIsResponsive proves the ETA column appears in the list header at
// wide widths (only reachable now that the cap is +15%) and folds away when narrow.
func TestListETAColumnIsResponsive(t *testing.T) {
	mgr, _ := newResponsiveTestManager(t)
	for _, th := range themes {
		m := initialModel(mgr, ".", "", nil)
		m.theme = th

		wide, _ := m.Update(tea.WindowSizeMsg{Width: 115, Height: 40})
		if out := ansi.Strip(wide.View()); !strings.Contains(out, "ETA") {
			t.Fatalf("theme=%s wide list (w=115) missing ETA column:\n%s", th.name, out)
		}

		narrow, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 40})
		if out := ansi.Strip(narrow.View()); strings.Contains(out, "ETA") {
			t.Fatalf("theme=%s narrow list (w=80) should not show the ETA column:\n%s", th.name, out)
		}
	}
}

func TestViewMaxLineWidth(t *testing.T) {
	widths := []int{1, 2, 5, 10, 20, 30, 50, 68, 80, 100, 200}
	modes := []viewMode{viewList, viewDetail, viewFiles, viewInput, viewAddConfirm, viewDeleteConfirm}
	mgr, sess := newResponsiveTestManager(t)

	longWarn := "startup warning " + strings.Repeat("x", 200)
	for _, th := range themes {
		for _, mode := range modes {
			for _, w := range widths {
				m := initialModel(mgr, ".", longWarn, nil)
				m.theme = th
				m.viewMode = mode
				m.flash = strings.Repeat("flash ", 60)
				m.inputMode = inputAddTorrent
				m.textInput.SetValue(strings.Repeat("magnet:?xt=urn:btih:", 12))
				if mode == viewAddConfirm {
					m.pendingItems = []pendingItem{{
						displayName: strings.Repeat("name", 80),
						downloadDir: "/" + strings.Repeat("dir/", 80),
					}}
				}
				if mode == viewDeleteConfirm {
					m.deleteTargetName = sess.Name()
					m.deleteTargetHash = fmt.Sprintf("%x", sess.Torrent.InfoHash)
					m.deleteWithFiles = true
				}
				um, _ := m.Update(tea.WindowSizeMsg{Width: w, Height: 40})
				out := um.View()
				max := outerWidth(w)
				for i, ln := range strings.Split(out, "\n") {
					if ansi.StringWidth(ln) > max {
						t.Errorf("theme=%s mode=%d w=%d line %d width %d > %d: %q",
							th.name, mode, w, i, ansi.StringWidth(ln), max, ansi.Strip(ln))
					}
				}
			}
		}
	}
}

func TestDraculaPeerSpeedHasSingleUnit(t *testing.T) {
	mgr, _ := newResponsiveTestManager(t)
	m := initialModel(mgr, ".", "", nil)
	m.viewMode = viewDetail
	out := ansi.Strip(m.View())
	if strings.Contains(out, "/s/s") {
		t.Fatalf("peer speed contains duplicate unit:\n%s", out)
	}
}

func TestLongTorrentListKeepsSelectionVisible(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	t.Cleanup(mgr.Close)
	addMockTorrents(t, mgr, 30)

	for _, th := range themes {
		m := initialModel(mgr, ".", "", nil)
		m.theme = th
		updated, _ := m.Update(tea.WindowSizeMsg{Width: 100, Height: 18})
		m = updated.(model)

		for _, idx := range []int{0, 14, 29} {
			m.selectedIdx = idx
			out := ansi.Strip(m.View())
			name := m.sessions[idx].Name()
			if !strings.Contains(out, name) {
				t.Fatalf("theme=%s selected torrent %q is off-screen:\n%s", th.name, name, out)
			}
			if renderedLineCount(out) > m.height {
				t.Fatalf("theme=%s rendered %d lines into height %d", th.name, renderedLineCount(out), m.height)
			}
		}
	}
}

func TestDetailKeyboardAndMouseScrolling(t *testing.T) {
	mgr, _ := newResponsiveTestManager(t)

	for _, th := range themes {
		m := initialModel(mgr, ".", "", nil)
		m.theme = th
		m.viewMode = viewDetail
		updated, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 8})
		m = updated.(model)

		top := ansi.Strip(m.View())
		if m.detailMaxScroll() == 0 {
			t.Fatalf("theme=%s mock details unexpectedly fit without scrolling:\n%s", th.name, top)
		}

		updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyPgDown})
		m = updated.(model)
		if m.detailScroll == 0 {
			t.Fatalf("theme=%s Page Down did not scroll details", th.name)
		}
		if got := ansi.Strip(m.View()); got == top {
			t.Fatalf("theme=%s detail view did not change after Page Down", th.name)
		}

		beforeWheel := m.detailScroll
		updated, _ = m.Update(tea.MouseMsg{
			Button: tea.MouseButtonWheelDown,
			Action: tea.MouseActionPress,
		})
		m = updated.(model)
		if m.detailScroll <= beforeWheel {
			t.Fatalf("theme=%s mouse wheel did not scroll details: before=%d after=%d",
				th.name, beforeWheel, m.detailScroll)
		}

		updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnd})
		m = updated.(model)
		if m.detailScroll != m.detailMaxScroll() {
			t.Fatalf("theme=%s End offset=%d want %d", th.name, m.detailScroll, m.detailMaxScroll())
		}
		bottom := ansi.Strip(m.View())
		if !strings.Contains(bottom, "[q] Quit") {
			t.Fatalf("theme=%s bottom help is not reachable:\n%s", th.name, bottom)
		}

		updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyHome})
		m = updated.(model)
		if m.detailScroll != 0 {
			t.Fatalf("theme=%s Home offset=%d want 0", th.name, m.detailScroll)
		}
	}
}

type immediateQuitModel struct{}

func (immediateQuitModel) Init() tea.Cmd                       { return tea.Quit }
func (immediateQuitModel) Update(tea.Msg) (tea.Model, tea.Cmd) { return immediateQuitModel{}, nil }
func (immediateQuitModel) View() string                        { return "" }

func TestNewTUIProgramUsesAlternateScreen(t *testing.T) {
	var out bytes.Buffer
	p := newTUIProgram(immediateQuitModel{}, tea.WithInput(nil), tea.WithOutput(&out))
	if _, err := p.Run(); err != nil {
		t.Fatalf("program.Run: %v", err)
	}
	rendered := out.String()
	if !strings.Contains(rendered, ansi.SetModeAltScreenSaveCursor) {
		t.Fatalf("program did not enter alternate screen: %q", rendered)
	}
	if !strings.Contains(rendered, ansi.ResetModeAltScreenSaveCursor) {
		t.Fatalf("program did not leave alternate screen: %q", rendered)
	}
}

func TestWindowSizeMsgUpdatesWidths(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	m := initialModel(mgr, ".", "", nil)
	um, _ := m.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	got := um.(model)

	if got.width != 120 || got.height != 40 {
		t.Fatalf("size not stored: %dx%d", got.width, got.height)
	}
	if got.progress.Width != bodyWidth(120) {
		t.Errorf("progress.Width=%d want %d", got.progress.Width, bodyWidth(120))
	}
	wantTI := bodyWidth(120) - dispWidth(got.textInput.Prompt)
	if got.textInput.Width != wantTI {
		t.Errorf("textInput.Width=%d want %d", got.textInput.Width, wantTI)
	}
}

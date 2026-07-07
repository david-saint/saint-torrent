package main

import (
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
	"sainttorrent/pkg/downloader"
)

// naivePieceMap is the pre-optimization renderer: one Render call per piece. The
// batched renderPieceMap must produce output that is visually identical (same
// glyphs, same line breaks) once ANSI styling is stripped.
func naivePieceMap(st styles, states []downloader.PieceState, perRow int, g, have, downloading, pending string) string {
	if perRow < 1 {
		perRow = 1
	}
	var pm strings.Builder
	for i, state := range states {
		if i%perRow == 0 {
			if i > 0 {
				pm.WriteString("\n")
			}
			pm.WriteString(g)
		}
		switch state {
		case downloader.PieceCompleted:
			pm.WriteString(st.PieceHave.Render(have))
		case downloader.PieceDownloading:
			pm.WriteString(st.PieceDownloading.Render(downloading))
		default:
			pm.WriteString(st.PiecePending.Render(pending))
		}
	}
	return pm.String()
}

func TestRenderPieceMapMatchesNaive(t *testing.T) {
	st := draculaStyles()
	states := []downloader.PieceState{
		downloader.PieceCompleted, downloader.PieceCompleted, downloader.PieceDownloading,
		downloader.PieceEmpty, downloader.PieceEmpty, downloader.PieceCompleted,
		downloader.PieceDownloading, downloader.PieceEmpty, downloader.PieceCompleted,
		downloader.PieceCompleted,
	}
	for _, states := range [][]downloader.PieceState{nil, states[:1], states} {
		for _, perRow := range []int{1, 2, 3, 4, 100} {
			got := renderPieceMap(st, states, perRow, "  ", "█", "░", ".")
			want := naivePieceMap(st, states, perRow, "  ", "█", "░", ".")
			if ansi.Strip(got) != ansi.Strip(want) {
				t.Fatalf("perRow=%d stripped mismatch:\n got=%q\nwant=%q",
					perRow, ansi.Strip(got), ansi.Strip(want))
			}
			if strings.Count(got, "\n") != strings.Count(want, "\n") {
				t.Fatalf("perRow=%d newline count: got=%d want=%d",
					perRow, strings.Count(got, "\n"), strings.Count(want, "\n"))
			}
		}
	}
}

func TestPulseStyleForIsCachedAndQuantized(t *testing.T) {
	// GetForeground reports the configured color regardless of the terminal color
	// profile (Render() would strip color when the test has no TTY).
	//
	// Same opacity bucket resolves to the same cached shade.
	a := pulseStyleFor("#50fa7b", 0.800)
	b := pulseStyleFor("#50fa7b", 0.810) // 0.80*24 and 0.81*24 both floor to bucket 19
	if a.GetForeground() != b.GetForeground() {
		t.Fatalf("expected close opacities in one bucket to share a shade: %v vs %v",
			a.GetForeground(), b.GetForeground())
	}
	// Far-apart opacities land in different buckets and get different shades.
	lo := pulseStyleFor("#50fa7b", 0.40)
	hi := pulseStyleFor("#50fa7b", 0.99)
	if lo.GetForeground() == hi.GetForeground() {
		t.Fatalf("expected distinct shades for far-apart opacities")
	}
}

func TestAnimPausesWhenNothingAnimates(t *testing.T) {
	mgr, _ := newResponsiveTestManager(t)
	m := initialModel(mgr, ".", "", nil)

	// List view + a downloading torrent: the pulse loop should run.
	if !m.wantAnim() {
		t.Fatal("expected wantAnim in list view with a downloading torrent")
	}
	um, cmd := m.Update(animMsg(time.Now()))
	m = um.(model)
	if cmd == nil {
		t.Fatal("animMsg should reschedule while a download is visible")
	}

	// The detail view does not animate: the loop must stop.
	m.viewMode = viewDetail
	um, cmd = m.Update(animMsg(time.Now()))
	m = um.(model)
	if cmd != nil {
		t.Fatal("animMsg should stop the loop in a non-animated view")
	}
	if m.animRunning {
		t.Fatal("animRunning should be cleared once the loop stops")
	}
}

func TestTickStartsAndGuardsAnimLoop(t *testing.T) {
	mgr, _ := newResponsiveTestManager(t)
	m := initialModel(mgr, ".", "", nil)
	m.animRunning = false

	// A data tick while downloading in the list view (re)starts the pulse loop.
	um, cmd := m.Update(tickMsg(time.Now()))
	m = um.(model)
	if !m.animRunning {
		t.Fatal("tick should start the anim loop while downloading in the list view")
	}
	if cmd == nil {
		t.Fatal("tick should return commands")
	}

	// A second tick must not spawn a duplicate loop; the guard keeps it single.
	um, _ = m.Update(tickMsg(time.Now()))
	m = um.(model)
	if !m.animRunning {
		t.Fatal("animRunning should remain true across ticks")
	}
}

func TestAnimStaysIdleWithoutDownloads(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	t.Cleanup(mgr.Close)
	m := initialModel(mgr, ".", "", nil)

	if m.wantAnim() {
		t.Fatal("did not expect wantAnim with no sessions")
	}
	um, cmd := m.Update(tickMsg(time.Now()))
	m = um.(model)
	if m.animRunning {
		t.Fatal("idle client must not start the pulse loop")
	}
	if cmd == nil {
		t.Fatal("tick should still reschedule the data tick")
	}
	// An in-flight animMsg (e.g. from before the last download finished) stops.
	_, cmd = m.Update(animMsg(time.Now()))
	if cmd != nil {
		t.Fatal("animMsg should not reschedule when nothing animates")
	}
}

package main

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"sainttorrent/pkg/downloader"
)

// updateAndRun applies msg, then runs the background command it returns and
// applies that command's result, as the Bubble Tea runtime does.
func updateAndRun(t *testing.T, m model, msg tea.Msg) model {
	t.Helper()
	updated, cmd := m.Update(msg)
	m = updated.(model)
	if cmd == nil {
		return m
	}
	updated, _ = m.Update(cmd())
	return updated.(model)
}

func keyRune(r string) tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(r)}
}

// Adding a torrent creates its files and fsyncs session.json and the cached
// .torrent, which takes seconds on a busy macOS disk. None of it may run on
// the event loop, or every key and redraw waits behind it.
func TestConfirmedAddRunsOffEventLoop(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	const hash = "842e85596f7a0dd05eefdb78b0ac1736496f8626"
	pending := []pendingItem{{
		rawURL:      "magnet:?xt=urn:btih:" + hash + "&dn=Async",
		displayName: "Async",
		infoHashHex: hash,
		downloadDir: t.TempDir(),
	}}
	m := initialModel(mgr, ".", "", pending)

	updated, cmd := m.Update(keyRune("y"))
	m = updated.(model)
	if cmd == nil {
		t.Fatal("confirming an add returned no background command")
	}
	if mgr.GetSession(hash) != nil {
		t.Fatal("the add ran on the event loop")
	}
	if !m.confirmAddBusy || m.viewMode != viewAddConfirm {
		t.Fatalf("busy=%v view=%v, want the confirm screen to wait for the add", m.confirmAddBusy, m.viewMode)
	}
	if view := m.View(); !strings.Contains(view, "Adding torrent in the background") {
		t.Fatalf("confirm screen does not show the running add:\n%s", view)
	}

	// Keys pressed while the add runs neither add it again nor skip past it.
	for _, k := range []string{"y", "n"} {
		updated, again := m.Update(keyRune(k))
		m = updated.(model)
		if again != nil || m.pendingIdx != 0 {
			t.Fatalf("%q during the add: cmd=%v pendingIdx=%d", k, again != nil, m.pendingIdx)
		}
	}

	updated, _ = m.Update(cmd())
	m = updated.(model)
	if mgr.GetSession(hash) == nil {
		t.Fatal("torrent was not added")
	}
	if m.confirmAddBusy || m.viewMode != viewList || m.pendingItems != nil {
		t.Fatalf("after the add: busy=%v view=%v pending=%v", m.confirmAddBusy, m.viewMode, m.pendingItems)
	}
	if len(m.rows) != 1 {
		t.Fatalf("list shows %d rows after the add, want 1", len(m.rows))
	}
}

func TestForwardedAddRunsOffEventLoopAndAnswersSocket(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	const hash = "942e85596f7a0dd05eefdb78b0ac1736496f8626"
	m := initialModel(mgr, t.TempDir(), "", nil)
	resp := make(chan addTorrentResponse, 1)
	updated, cmd := m.Update(addTorrentMsg{
		msg:      socketMessage{Items: []string{"magnet:?xt=urn:btih:" + hash + "&dn=Fwd", "not a torrent"}},
		respChan: resp,
	})
	m = updated.(model)
	if cmd == nil {
		t.Fatal("forwarded add returned no background command")
	}
	if mgr.GetSession(hash) != nil {
		t.Fatal("the forwarded add ran on the event loop")
	}
	select {
	case r := <-resp:
		t.Fatalf("socket answered before the add finished: %v", r.err)
	default:
	}

	updated, _ = m.Update(cmd())
	m = updated.(model)
	if mgr.GetSession(hash) == nil {
		t.Fatal("the valid item was not added")
	}
	select {
	case r := <-resp:
		if r.err == nil {
			t.Fatal("the invalid item's error was not reported to the socket")
		}
	default:
		t.Fatal("socket was not answered")
	}
	if len(m.rows) != 1 {
		t.Fatalf("list shows %d rows after the add, want 1", len(m.rows))
	}
}

func TestInputBoxAddRunsOffEventLoop(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	m := initialModel(mgr, t.TempDir(), "", nil)
	m.viewMode = viewInput
	m.inputMode = inputAddTorrent

	const hash = "a42e85596f7a0dd05eefdb78b0ac1736496f8626"
	m.textInput.SetValue("magnet:?xt=urn:btih:" + hash + "&dn=Typed")
	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(model)
	if cmd == nil || mgr.GetSession(hash) != nil {
		t.Fatalf("add from the input box ran on the event loop (cmd=%v)", cmd != nil)
	}
	if view := m.View(); !strings.Contains(view, "Adding torrent in the background") {
		t.Fatalf("input box does not show the running add:\n%s", view)
	}
	if _, again := m.Update(tea.KeyMsg{Type: tea.KeyEnter}); again != nil {
		t.Fatal("a second enter started another add while the first ran")
	}
	updated, _ = m.Update(cmd())
	m = updated.(model)
	if mgr.GetSession(hash) == nil || m.viewMode != viewList || m.inputAddBusy {
		t.Fatalf("after the add: added=%v view=%v busy=%v", mgr.GetSession(hash) != nil, m.viewMode, m.inputAddBusy)
	}

	// A failure is shown in the box, which stays open for a correction.
	m.viewMode = viewInput
	m.inputMode = inputAddTorrent
	m.textInput.SetValue("not a torrent")
	m = updateAndRun(t, m, tea.KeyMsg{Type: tea.KeyEnter})
	if m.viewMode != viewInput || !strings.Contains(m.inputErr, "Failed to load torrent") {
		t.Fatalf("failed add: view=%v inputErr=%q", m.viewMode, m.inputErr)
	}

	// Closed before the add failed: the failure is flashed instead.
	m.inputErr = ""
	updated, cmd = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(model)
	updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = updated.(model)
	updated, _ = m.Update(cmd())
	m = updated.(model)
	if m.viewMode != viewList || !strings.Contains(m.flash, "Failed to load torrent") {
		t.Fatalf("add failing after esc: view=%v flash=%q", m.viewMode, m.flash)
	}
}

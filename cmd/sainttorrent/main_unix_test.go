//go:build !windows

package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"sainttorrent/pkg/downloader"
)

func TestFindTerminalTTY(t *testing.T) {
	input, err := os.Open("/dev/null")
	if err != nil {
		t.Fatalf("failed to open character device: %v", err)
	}
	defer input.Close()

	if got := findTerminalTTY(input, []string{"/dev/null"}); got != "/dev/null" {
		t.Fatalf("expected matching device path, got %q", got)
	}

	tempFile, err := os.CreateTemp("", "sainttorrent-not-a-tty-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	if got := findTerminalTTY(tempFile, []string{tempFile.Name()}); got != "" {
		t.Fatalf("expected regular file to be rejected, got %q", got)
	}
}

// SAINTTORRENT_TIMING_LOG shares the debug log's open: a symlink planted at
// the configured path must not be followed into another file.
func TestPerfReportRefusesSymlinkedTimingLog(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(dir, "victim")
	if err := os.WriteFile(victim, []byte("keep\n"), 0644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "timing.log")
	if err := os.Symlink(victim, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SAINTTORRENT_TIMING_LOG", link)
	prevEnabled, prevMarks := perfEnabled, perfMarks
	perfEnabled, perfMarks = true, []perfMark{{label: "test", at: time.Millisecond}}
	defer func() { perfEnabled, perfMarks = prevEnabled, prevMarks }()

	perfReport(io.Discard)

	data, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "keep\n" {
		t.Fatalf("timing report was written through the symlink: %q", data)
	}
}

// The primary's TUI parses socket-forwarded items inside Update, before any
// confirmation. A FIFO must be refused at once rather than blocking the event
// loop in open, and a device such as /dev/zero must not be read until the
// process runs out of memory.
func TestForwardedFIFOOrDeviceIsRefusedWithoutReading(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "fifo.torrent")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()
	m := initialModel(mgr, ".", "", nil)

	done := make(chan model, 1)
	go func() {
		updated, _ := m.Update(addTorrentMsg{msg: socketMessage{Items: []string{fifo}, Confirm: true}})
		done <- updated.(model)
	}()
	select {
	case got := <-done:
		if len(got.pendingItems) != 1 {
			t.Fatalf("pending items = %d, want 1", len(got.pendingItems))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Update blocked opening a forwarded FIFO")
	}

	for _, item := range []string{fifo, "/dev/zero"} {
		if _, _, err := parseItem(item); err == nil || !strings.Contains(err.Error(), "not a regular file") {
			t.Fatalf("parseItem(%q) err = %v; want not a regular file", item, err)
		}
	}
}

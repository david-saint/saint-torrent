//go:build !windows

package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
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

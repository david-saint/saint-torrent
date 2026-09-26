//go:build unix

package torrent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestReadFileRefusesFIFOWithoutBlocking: os.Open of a FIFO waits for a
// writer, so a FIFO swapped in at a .torrent path froze the caller (the TUI
// adds a torrent synchronously) indefinitely. ReadFile must refuse it at once.
func TestReadFileRefusesFIFOWithoutBlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fifo.torrent")
	if err := unix.Mkfifo(path, 0600); err != nil {
		t.Skipf("cannot create a FIFO here: %v", err)
	}
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		data, err := ReadFile(path)
		done <- result{data, err}
	}()
	select {
	case r := <-done:
		if r.err == nil || !strings.Contains(r.err.Error(), "not a regular file") {
			t.Fatalf("ReadFile(FIFO) = %d bytes, %v; want a not-a-regular-file error", len(r.data), r.err)
		}
	case <-time.After(time.Second):
		// Opening the write end releases the blocked reader, so a failing
		// run does not leak it.
		if w, err := os.OpenFile(path, os.O_WRONLY|unix.O_NONBLOCK, 0); err == nil {
			w.Close()
		}
		t.Fatal("ReadFile blocked on a FIFO")
	}
}

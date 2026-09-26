//go:build !windows

package dht

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// mkfifoOrSkip creates a FIFO at path, skipping where that is not possible.
func mkfifoOrSkip(t *testing.T, path string) {
	t.Helper()
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Skipf("FIFOs unavailable: %v", err)
	}
}

// TestLoadNodesDoesNotBlockOnFIFO is the reported startup hang: a FIFO planted
// at the nodes path in a shared download directory must not block DHT startup
// waiting for a writer, and seeds nothing.
func TestLoadNodesDoesNotBlockOnFIFO(t *testing.T) {
	dir := t.TempDir()
	mkfifoOrSkip(t, filepath.Join(dir, nodesFileName))

	type result struct {
		d   *DHT
		err error
	}
	started := make(chan result, 1)
	go func() {
		d, err := NewDHTWithConn(dir, newFakeConn())
		started <- result{d, err}
	}()
	select {
	case r := <-started:
		if r.err != nil {
			t.Fatalf("failed to start DHT: %v", r.err)
		}
		d := r.d
		defer d.Close()
		if got := d.NodesCount(); got != 0 {
			t.Fatalf("a FIFO seeded %d contacts", got)
		}
	case <-time.After(time.Second):
		t.Fatal("DHT startup blocked on a FIFO at the nodes path")
	}
}

// TestOpenNodesFileNeitherBlocksNorFollows covers the name being swapped after
// readNodesFile's Lstat: the open itself must not wait on a FIFO or follow a
// symlink, and readNodesFile must refuse what it opened.
func TestOpenNodesFileNeitherBlocksNorFollows(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "fifo")
	mkfifoOrSkip(t, fifo)

	opened := make(chan error, 1)
	go func() {
		f, err := openNodesFile(fifo)
		if err == nil {
			info, statErr := f.Stat()
			if statErr == nil && info.Mode().IsRegular() {
				err = os.ErrInvalid
			}
			f.Close()
		}
		opened <- err
	}()
	select {
	case err := <-opened:
		if err != nil {
			t.Fatalf("opening a FIFO failed unexpectedly: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("opening a FIFO blocked waiting for a writer")
	}

	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("d5:nodeslee"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	symlinkOrSkip(t, target, link)
	if f, err := openNodesFile(link); err == nil {
		f.Close()
		t.Fatal("openNodesFile followed a symlink")
	}
	if _, err := readNodesFile(target); err != nil {
		t.Fatalf("a regular nodes file was refused: %v", err)
	}
}

//go:build !windows

package storage

import (
	"runtime"
	"testing"
	"time"
)

func TestMMapStorageCloseWaitsForActiveReaders(t *testing.T) {
	st, err := NewMMapStorage(t.TempDir(), []FileInfo{{Path: "data.bin", Length: 32}}, 32)
	if err != nil {
		t.Fatalf("NewMMapStorage: %v", err)
	}

	st.mu.RLock()
	done := make(chan struct{})
	go func() {
		_ = st.Close()
		close(done)
	}()

	deadline := time.After(time.Second)
	for !st.closed.Load() {
		select {
		case <-deadline:
			st.mu.RUnlock()
			t.Fatal("Close did not start")
		default:
			runtime.Gosched()
		}
	}
	select {
	case <-done:
		st.mu.RUnlock()
		t.Fatal("Close returned while a reader held the mmap read lock")
	default:
	}

	st.mu.RUnlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after reader released lock")
	}
}

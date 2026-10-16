package storage

import (
	"io"
	"os"
	"time"
)

// handleCloseWait bounds how long closing a cached handle waits where
// boundedHandleClose is set. Closing an idle handle takes microseconds, so in
// practice only a close stuck behind the os.File reference leak (see
// boundedHandleClose) waits this long. A var so tests can shorten it; treat it
// as a constant.
var handleCloseWait = 500 * time.Millisecond

// closeCachedHandle closes a cached payload handle that other goroutines may
// still be reading or writing through. Where boundedHandleClose is set it waits
// at most handleCloseWait, so session teardown cannot hang on the handle.
func closeCachedHandle(h *os.File) {
	if !boundedHandleClose {
		_ = h.Close()
		return
	}
	closeWithin(h, handleCloseWait)
}

// closeWithin closes c on its own goroutine and waits at most wait for it,
// reporting whether the close finished in time. A close that is still running
// keeps going in the background.
func closeWithin(c io.Closer, wait time.Duration) bool {
	done := make(chan struct{})
	go func() {
		_ = c.Close()
		close(done)
	}()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

package downloader

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"io"
	"testing"
	"time"
)

func TestSequentialModePrefersEarliestReadahead(t *testing.T) {
	pieces := [][]byte{
		[]byte("piece-000-block"),
		[]byte("piece-001-block"),
		[]byte("piece-002-block"),
		[]byte("piece-003-block"),
		[]byte("piece-004-block"),
		[]byte("piece-005-block"),
	}
	sess := newPieceTestSession(t, int64(len(pieces[0])), pieces)

	sess.mu.Lock()
	for i := range sess.PieceStates {
		sess.setPieceAvailabilityLocked(i, 10)
	}
	sess.setPieceAvailabilityLocked(5, 1)
	got := sess.selectNeededPieceLocked(func(int64) bool { return true })
	sess.mu.Unlock()
	if got != 5 {
		t.Fatalf("expected default rarest-first pick 5, got %d", got)
	}

	sess.SetSequentialMode(true, 0, 3)
	sess.mu.Lock()
	got = sess.selectNeededPieceLocked(func(int64) bool { return true })
	sess.mu.Unlock()
	if got != 0 {
		t.Fatalf("expected sequential mode to pick earliest piece 0, got %d", got)
	}

	sess.mu.Lock()
	got = sess.selectNeededPieceLocked(func(idx int64) bool { return idx != 0 })
	sess.mu.Unlock()
	if got != 1 {
		t.Fatalf("expected sequential mode to pick next available piece 1, got %d", got)
	}

	sess.SetSequentialMode(true, 0, 2)
	sess.mu.Lock()
	got = sess.selectNeededPieceLocked(func(idx int64) bool { return idx >= 2 })
	sess.mu.Unlock()
	if got != 5 {
		t.Fatalf("expected fallback to rarest-first outside unavailable window, got %d", got)
	}
}

func TestTorrentReaderReadsFileWhileDownloadProgresses(t *testing.T) {
	pieces := threePieces()
	sess := newPieceTestSession(t, int64(len(pieces[0])), pieces)

	reader, err := sess.NewFileReader(0, ReaderOptions{ReadaheadPieces: 2})
	if err != nil {
		t.Fatalf("NewFileReader: %v", err)
	}
	defer reader.Close()

	type readResult struct {
		data []byte
		err  error
	}
	done := make(chan readResult, 1)
	go func() {
		data, err := io.ReadAll(reader)
		done <- readResult{data: data, err: err}
	}()

	select {
	case res := <-done:
		t.Fatalf("reader finished before any piece verified: data=%q err=%v", res.data, res.err)
	case <-time.After(50 * time.Millisecond):
	}

	var want []byte
	for i, piece := range pieces {
		want = append(want, piece...)
		sess.processCompletedPiece(pieceWriteJob{
			index: int64(i),
			hash:  sha1.Sum(piece),
			data:  piece,
		})
	}

	var res readResult
	select {
	case res = <-done:
	case <-time.After(time.Second):
		t.Fatal("reader did not finish after all pieces verified")
	}
	if res.err != nil {
		t.Fatalf("ReadAll returned error: %v", res.err)
	}
	if !bytes.Equal(res.data, want) {
		t.Fatalf("reader data mismatch: got %q want %q", res.data, want)
	}

	if pos, err := reader.Seek(int64(len(pieces[0])), io.SeekStart); err != nil || pos != int64(len(pieces[0])) {
		t.Fatalf("Seek to second piece got pos=%d err=%v", pos, err)
	}
	buf := make([]byte, len(pieces[1]))
	n, err := reader.Read(buf)
	if err != nil {
		t.Fatalf("Read after Seek: %v", err)
	}
	if n != len(buf) || !bytes.Equal(buf, pieces[1]) {
		t.Fatalf("seek read mismatch: n=%d data=%q", n, buf)
	}

	at := make([]byte, len(pieces[2]))
	n, err = reader.ReadAt(at, int64(len(pieces[0])+len(pieces[1])))
	if err != nil {
		t.Fatalf("ReadAt third piece: %v", err)
	}
	if n != len(at) || !bytes.Equal(at, pieces[2]) {
		t.Fatalf("ReadAt mismatch: n=%d data=%q", n, at)
	}
}

// TestReaderReturnsCompletedPieceDespiteStatusErr proves a verified, on-disk piece
// is still readable even when a non-fatal session error (such as a fast-resume
// persistence failure) has been recorded in statusErr.
func TestReaderReturnsCompletedPieceDespiteStatusErr(t *testing.T) {
	pieces := threePieces()
	sess := newPieceTestSession(t, int64(len(pieces[0])), pieces)

	sess.processCompletedPiece(pieceWriteJob{index: 0, hash: sha1.Sum(pieces[0]), data: pieces[0]})

	sess.mu.Lock()
	sess.statusErr = errors.New("failed to save fast-resume state")
	sess.mu.Unlock()

	reader, err := sess.NewReader(ReaderOptions{Length: int64(len(pieces[0]))})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()

	buf := make([]byte, len(pieces[0]))
	n, err := reader.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt returned error for a verified piece despite statusErr: %v", err)
	}
	if n != len(buf) || !bytes.Equal(buf, pieces[0]) {
		t.Fatalf("ReadAt mismatch: n=%d data=%q", n, buf)
	}
}

// TestReaderFailsFastOnDeprioritizedFile proves a reader over a skipped file returns
// an error promptly instead of blocking forever on a piece that will never download.
func TestReaderFailsFastOnDeprioritizedFile(t *testing.T) {
	pieces := threePieces()
	sess := newPieceTestSession(t, int64(len(pieces[0])), pieces)

	sess.mu.Lock()
	sess.FilePriorities = []FilePriority{PrioritySkip}
	sess.mu.Unlock()

	reader, err := sess.NewFileReader(0, ReaderOptions{})
	if err != nil {
		t.Fatalf("NewFileReader: %v", err)
	}
	defer reader.Close()

	done := make(chan error, 1)
	go func() {
		_, readErr := reader.Read(make([]byte, len(pieces[0])))
		done <- readErr
	}()

	select {
	case readErr := <-done:
		if readErr == nil {
			t.Fatal("expected an error reading a deprioritized file, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("reader hung on a deprioritized file instead of failing fast")
	}
}

// TestSequentialModeClearedAfterLastReaderCloses proves the sequential picker bias a
// reader turns on is turned back off once the last reader closes, so a transient
// stream does not permanently skew piece selection.
func TestSequentialModeClearedAfterLastReaderCloses(t *testing.T) {
	pieces := threePieces()
	sess := newPieceTestSession(t, int64(len(pieces[0])), pieces)
	for i, p := range pieces {
		sess.processCompletedPiece(pieceWriteJob{index: int64(i), hash: sha1.Sum(p), data: p})
	}

	reader, err := sess.NewFileReader(0, ReaderOptions{ReadaheadPieces: 2})
	if err != nil {
		t.Fatalf("NewFileReader: %v", err)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if enabled, _, _ := sess.SequentialMode(); !enabled {
		t.Fatal("expected sequential mode enabled after a streaming read")
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if enabled, _, _ := sess.SequentialMode(); enabled {
		t.Fatal("expected sequential mode disabled after the last reader closed")
	}
}

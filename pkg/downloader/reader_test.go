package downloader

import (
	"bytes"
	"crypto/sha1"
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

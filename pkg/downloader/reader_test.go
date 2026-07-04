package downloader

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"io"
	"slices"
	"strings"
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

func TestConcurrentReadersKeepIndependentSequentialWindows(t *testing.T) {
	pieces := [][]byte{
		[]byte("piece-000-block"),
		[]byte("piece-001-block"),
		[]byte("piece-002-block"),
		[]byte("piece-003-block"),
		[]byte("piece-004-block"),
		[]byte("piece-005-block"),
		[]byte("piece-006-block"),
		[]byte("piece-007-block"),
	}
	sess := newPieceTestSession(t, int64(len(pieces[0])), pieces)

	first, err := sess.NewReader(ReaderOptions{
		Offset:          0,
		Length:          int64(2 * len(pieces[0])),
		ReadaheadPieces: 2,
	})
	if err != nil {
		t.Fatalf("NewReader first: %v", err)
	}
	defer first.Close()
	second, err := sess.NewReader(ReaderOptions{
		Offset:          int64(4 * len(pieces[0])),
		Length:          int64(2 * len(pieces[0])),
		ReadaheadPieces: 2,
	})
	if err != nil {
		t.Fatalf("NewReader second: %v", err)
	}
	defer second.Close()

	first.prioritize(0, int64(len(pieces[0])))
	second.prioritize(int64(4*len(pieces[0])), int64(len(pieces[0])))

	var got []int
	sess.mu.Lock()
	for len(got) < 4 {
		piece := sess.selectNeededPieceLocked(func(int64) bool { return true })
		if piece == -1 {
			break
		}
		got = append(got, piece)
		sess.PieceStates[piece] = PieceDownloading
		sess.removeNeededLocked(piece)
	}
	sess.mu.Unlock()

	want := []int{0, 1, 4, 5}
	if !slices.Equal(got, want) {
		t.Fatalf("sequential picks mismatch: got %v want %v", got, want)
	}
}

func TestSetSequentialModeFalsePreservesReaderWindows(t *testing.T) {
	pieces := [][]byte{
		[]byte("piece-000-block"),
		[]byte("piece-001-block"),
		[]byte("piece-002-block"),
		[]byte("piece-003-block"),
		[]byte("piece-004-block"),
		[]byte("piece-005-block"),
	}
	sess := newPieceTestSession(t, int64(len(pieces[0])), pieces)

	reader, err := sess.NewReader(ReaderOptions{
		Offset:          0,
		Length:          int64(2 * len(pieces[0])),
		ReadaheadPieces: 2,
	})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()

	sess.mu.Lock()
	for i := range sess.PieceStates {
		sess.setPieceAvailabilityLocked(i, 10)
	}
	sess.setPieceAvailabilityLocked(5, 1)
	sess.mu.Unlock()

	reader.prioritize(0, int64(len(pieces[0])))
	sess.SetSequentialMode(false, 0, 0)

	sess.mu.Lock()
	got := sess.selectNeededPieceLocked(func(int64) bool { return true })
	sess.mu.Unlock()
	if got != 0 {
		t.Fatalf("expected reader window to survive SetSequentialMode(false) and pick 0, got %d", got)
	}
}

func TestSequentialModeReportsManualModeOnly(t *testing.T) {
	pieces := threePieces()
	sess := newPieceTestSession(t, int64(len(pieces[0])), pieces)

	reader, err := sess.NewReader(ReaderOptions{ReadaheadPieces: 2})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()

	reader.prioritize(0, int64(len(pieces[0])))
	if enabled, _, _ := sess.SequentialMode(); enabled {
		t.Fatal("expected SequentialMode to ignore reader-private windows")
	}

	sess.SetSequentialMode(true, 1, 3)
	enabled, startPiece, readaheadPieces := sess.SequentialMode()
	if !enabled || startPiece != 1 || readaheadPieces != 3 {
		t.Fatalf("manual SequentialMode mismatch: enabled=%v start=%d readahead=%d", enabled, startPiece, readaheadPieces)
	}
}

func TestReaderSequentialWindowFallsBackToRarestFirst(t *testing.T) {
	pieces := [][]byte{
		[]byte("piece-000-block"),
		[]byte("piece-001-block"),
		[]byte("piece-002-block"),
		[]byte("piece-003-block"),
		[]byte("piece-004-block"),
		[]byte("piece-005-block"),
	}
	sess := newPieceTestSession(t, int64(len(pieces[0])), pieces)

	reader, err := sess.NewReader(ReaderOptions{
		Offset:          0,
		Length:          int64(2 * len(pieces[0])),
		ReadaheadPieces: 2,
	})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()

	sess.mu.Lock()
	for i := range sess.PieceStates {
		sess.setPieceAvailabilityLocked(i, 10)
	}
	sess.setPieceAvailabilityLocked(5, 1)
	sess.mu.Unlock()

	reader.prioritize(0, int64(len(pieces[0])))

	sess.mu.Lock()
	got := sess.selectNeededPieceLocked(func(idx int64) bool { return idx >= 2 })
	sess.mu.Unlock()
	if got != 5 {
		t.Fatalf("expected reader window to fall back to rarest piece 5, got %d", got)
	}
}

func TestCompletedReaderWindowDoesNotAdvanceBeyondRange(t *testing.T) {
	pieces := [][]byte{
		[]byte("piece-000-block"),
		[]byte("piece-001-block"),
		[]byte("piece-002-block"),
		[]byte("piece-003-block"),
		[]byte("piece-004-block"),
		[]byte("piece-005-block"),
	}
	sess := newPieceTestSession(t, int64(len(pieces[0])), pieces)

	reader, err := sess.NewReader(ReaderOptions{
		Offset:          0,
		Length:          int64(2 * len(pieces[0])),
		ReadaheadPieces: 2,
	})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()

	reader.prioritize(0, int64(len(pieces[0])))
	for i := 0; i < 2; i++ {
		sess.processCompletedPiece(pieceWriteJob{
			index: int64(i),
			hash:  sha1.Sum(pieces[i]),
			data:  pieces[i],
		})
	}
	sess.mu.Lock()
	for i := range sess.PieceStates {
		sess.setPieceAvailabilityLocked(i, 10)
	}
	sess.setPieceAvailabilityLocked(5, 1)
	got := sess.selectNeededPieceLocked(func(int64) bool { return true })
	sess.mu.Unlock()
	if got != 5 {
		t.Fatalf("expected completed reader window to fall back to rarest piece 5, got %d", got)
	}
}

func TestClosingReaderRemovesOnlyItsSequentialWindow(t *testing.T) {
	pieces := [][]byte{
		[]byte("piece-000-block"),
		[]byte("piece-001-block"),
		[]byte("piece-002-block"),
		[]byte("piece-003-block"),
		[]byte("piece-004-block"),
		[]byte("piece-005-block"),
	}
	sess := newPieceTestSession(t, int64(len(pieces[0])), pieces)

	first, err := sess.NewReader(ReaderOptions{
		Offset:          0,
		Length:          int64(2 * len(pieces[0])),
		ReadaheadPieces: 2,
	})
	if err != nil {
		t.Fatalf("NewReader first: %v", err)
	}
	defer first.Close()
	second, err := sess.NewReader(ReaderOptions{
		Offset:          int64(4 * len(pieces[0])),
		Length:          int64(2 * len(pieces[0])),
		ReadaheadPieces: 2,
	})
	if err != nil {
		t.Fatalf("NewReader second: %v", err)
	}

	first.prioritize(0, int64(len(pieces[0])))
	second.prioritize(int64(4*len(pieces[0])), int64(len(pieces[0])))
	if err := second.Close(); err != nil {
		t.Fatalf("Close second: %v", err)
	}

	sess.mu.Lock()
	got := sess.selectNeededPieceLocked(func(int64) bool { return true })
	sess.mu.Unlock()
	if got != 0 {
		t.Fatalf("expected remaining reader window to pick piece 0, got %d", got)
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

func TestPieceCompletionWakesOnlyWaitersForCompletedPiece(t *testing.T) {
	pieces := threePieces()
	pieceLen := int64(len(pieces[0]))
	sess := newPieceTestSession(t, pieceLen, pieces)

	reader0, err := sess.NewReader(ReaderOptions{Offset: 0, Length: pieceLen})
	if err != nil {
		t.Fatalf("NewReader piece 0: %v", err)
	}
	defer reader0.Close()

	reader1, err := sess.NewReader(ReaderOptions{Offset: pieceLen, Length: pieceLen})
	if err != nil {
		t.Fatalf("NewReader piece 1: %v", err)
	}
	defer reader1.Close()

	type readResult struct {
		data []byte
		err  error
	}
	done0 := make(chan readResult, 1)
	done1 := make(chan readResult, 1)
	go func() {
		buf := make([]byte, len(pieces[0]))
		_, err := io.ReadFull(reader0, buf)
		done0 <- readResult{data: buf, err: err}
	}()
	go func() {
		buf := make([]byte, len(pieces[1]))
		_, err := io.ReadFull(reader1, buf)
		done1 <- readResult{data: buf, err: err}
	}()

	ch0 := waitForPieceWaiter(t, sess, 0)
	ch1 := waitForPieceWaiter(t, sess, 1)

	sess.processCompletedPiece(pieceWriteJob{
		index: 0,
		hash:  sha1.Sum(pieces[0]),
		data:  pieces[0],
	})

	select {
	case <-ch0:
	case <-time.After(time.Second):
		t.Fatal("piece 0 waiter was not signaled")
	}
	select {
	case <-ch1:
		t.Fatal("piece 1 waiter was signaled by piece 0 completion")
	default:
	}

	select {
	case res := <-done0:
		if res.err != nil {
			t.Fatalf("piece 0 read failed: %v", res.err)
		}
		if !bytes.Equal(res.data, pieces[0]) {
			t.Fatalf("piece 0 data mismatch: got %q want %q", res.data, pieces[0])
		}
	case <-time.After(time.Second):
		t.Fatal("reader for completed piece 0 did not finish")
	}
	select {
	case res := <-done1:
		t.Fatalf("reader for incomplete piece 1 finished early: data=%q err=%v", res.data, res.err)
	default:
	}

	sess.processCompletedPiece(pieceWriteJob{
		index: 1,
		hash:  sha1.Sum(pieces[1]),
		data:  pieces[1],
	})
	select {
	case res := <-done1:
		if res.err != nil {
			t.Fatalf("piece 1 read failed: %v", res.err)
		}
		if !bytes.Equal(res.data, pieces[1]) {
			t.Fatalf("piece 1 data mismatch: got %q want %q", res.data, pieces[1])
		}
	case <-time.After(time.Second):
		t.Fatal("reader for completed piece 1 did not finish")
	}
}

func TestPieceCompletionWakesAllWaitersForSamePiece(t *testing.T) {
	pieces := threePieces()
	pieceLen := int64(len(pieces[0]))
	sess := newPieceTestSession(t, pieceLen, pieces)

	readerA, err := sess.NewReader(ReaderOptions{Offset: 0, Length: pieceLen})
	if err != nil {
		t.Fatalf("NewReader A: %v", err)
	}
	defer readerA.Close()

	readerB, err := sess.NewReader(ReaderOptions{Offset: 0, Length: pieceLen})
	if err != nil {
		t.Fatalf("NewReader B: %v", err)
	}
	defer readerB.Close()

	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(readerA)
		doneA <- err
	}()
	go func() {
		_, err := io.ReadAll(readerB)
		doneB <- err
	}()

	_ = waitForPieceWaiters(t, sess, 0, 2)
	sess.processCompletedPiece(pieceWriteJob{
		index: 0,
		hash:  sha1.Sum(pieces[0]),
		data:  pieces[0],
	})

	for name, done := range map[string]<-chan error{"reader A": doneA, "reader B": doneB} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s read failed: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s did not finish after piece completion", name)
		}
	}
}

// TestDeprioritizingFileWakesBlockedReader proves that setting a file to PrioritySkip
// wakes a reader already blocked on one of its pieces, so it returns the deprioritized
// error promptly instead of sleeping forever. Per-piece completion signals never fire
// for a piece that will no longer be downloaded, so the priority change must wake the
// reader itself (a regression guard for the per-piece wakeup scheme).
func TestDeprioritizingFileWakesBlockedReader(t *testing.T) {
	pieces := threePieces()
	pieceLen := int64(len(pieces[0]))
	sess := newPieceTestSession(t, pieceLen, pieces)

	reader, err := sess.NewReader(ReaderOptions{Offset: 0, Length: pieceLen})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, len(pieces[0]))
		_, err := io.ReadFull(reader, buf)
		done <- err
	}()

	// Block on piece 0's waiter channel before changing priority, so we exercise the
	// wakeup path rather than the subscribe-time fast-fail in waitForPiece.
	_ = waitForPieceWaiter(t, sess, 0)

	// The single test file spans every piece, so skipping it makes piece 0 unwanted.
	sess.SetFilePriority(0, PrioritySkip)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error after the file was deprioritized, got nil")
		}
		if !strings.Contains(err.Error(), "deprioritized") {
			t.Fatalf("expected a deprioritized error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader blocked on a deprioritized piece was not woken after SetFilePriority")
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

	sess.SetFilePriority(0, PrioritySkip)

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

// TestReaderWindowClearedAfterLastReaderCloses proves a transient stream does not
// keep biasing piece selection after the reader is closed.
func TestReaderWindowClearedAfterLastReaderCloses(t *testing.T) {
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
	sess.mu.Lock()
	readerWindows := len(sess.sequentialReaderWindows)
	sess.mu.Unlock()
	if readerWindows != 1 {
		t.Fatalf("expected one reader window after a streaming read, got %d", readerWindows)
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	sess.mu.Lock()
	readerWindows = len(sess.sequentialReaderWindows)
	sess.mu.Unlock()
	if readerWindows != 0 {
		t.Fatalf("expected reader windows cleared after close, got %d", readerWindows)
	}
}

func waitForPieceWaiter(t *testing.T, sess *Session, pieceIndex int64) <-chan struct{} {
	t.Helper()
	return waitForPieceWaiters(t, sess, pieceIndex, 1)
}

func waitForPieceWaiters(t *testing.T, sess *Session, pieceIndex int64, want int) <-chan struct{} {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		sess.mu.Lock()
		waiter := sess.pieceWaiters[pieceIndex]
		if waiter != nil && waiter.waiters >= want {
			ch := waiter.ch
			sess.mu.Unlock()
			return ch
		}
		sess.mu.Unlock()

		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d waiter(s) on piece %d", want, pieceIndex)
		case <-ticker.C:
		}
	}
}

package downloader

import (
	"context"
	"crypto/sha1"
	"fmt"
	"sync"
	"testing"
	"time"

	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// gatedVerifyStorage reports every VerifyPiece entry on entered and holds the call
// until the test releases it, so tests can observe which sessions are hashing at once.
type gatedVerifyStorage struct {
	storage.Storage
	name    string
	entered chan string
	release chan struct{}
	delay   time.Duration
}

func (g *gatedVerifyStorage) VerifyPiece(idx int64, hash [20]byte) (bool, error) {
	if g.entered != nil {
		g.entered <- g.name
	}
	if g.release != nil {
		<-g.release
	}
	if g.delay > 0 {
		time.Sleep(g.delay)
	}
	return true, nil
}

// recheckSession builds an unstarted session whose pieces are all claimed by a resume
// hint and still need hashing, the state a restored torrent is in before its recheck.
func recheckSession(t *testing.T, name string, pieces int, st *gatedVerifyStorage, paused bool) *Session {
	t.Helper()
	hashes := make([][20]byte, pieces)
	for i := range hashes {
		hashes[i] = sha1.Sum(fmt.Appendf(nil, "%s-%d", name, i))
	}
	tor := &torrent.Torrent{
		Name:        name,
		InfoHash:    sha1.Sum([]byte(name)),
		PieceLength: 4,
		PieceHashes: hashes,
		Files:       []torrent.File{{Length: int64(4 * pieces), Path: []string{name}}},
	}
	if st.Storage == nil {
		mem, err := storage.NewMemStorage(t.TempDir(), []storage.FileInfo{{Path: name, Length: int64(4 * pieces)}}, 4)
		if err != nil {
			t.Fatalf("mem storage: %v", err)
		}
		st.Storage = mem
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	t.Cleanup(sess.Close)
	sess.mu.Lock()
	for i := range sess.PieceStates {
		sess.PieceStates[i] = PieceUnverified
	}
	sess.recomputeNeededLocked()
	sess.recomputeStatsLocked()
	sess.verifying = true
	sess.verifyFullScan = false
	sess.verifyDone = make(chan struct{})
	sess.verifyTotalBytes = int64(4 * pieces)
	sess.paused = paused
	sess.mu.Unlock()
	return sess
}

func awaitEntry(t *testing.T, entered <-chan string, want string) {
	t.Helper()
	select {
	case got := <-entered:
		if got != want {
			t.Fatalf("session %q started hashing, want %q", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("session %q never started hashing", want)
	}
}

// awaitEntries collects one hasher entry per expected name, in any order.
func awaitEntries(t *testing.T, entered <-chan string, want ...string) {
	t.Helper()
	pending := map[string]int{}
	for _, name := range want {
		pending[name]++
	}
	for range want {
		select {
		case got := <-entered:
			if pending[got] == 0 {
				t.Fatalf("session %q started hashing, want one of %v", got, pending)
			}
			pending[got]--
		case <-time.After(3 * time.Second):
			t.Fatalf("still waiting for %v to start hashing", pending)
		}
	}
}

func assertNoEntry(t *testing.T, entered <-chan string, wait time.Duration) {
	t.Helper()
	select {
	case got := <-entered:
		t.Fatalf("session %q started hashing while it should have been queued", got)
	case <-time.After(wait):
	}
}

func TestPausedRechecksRunOneAtATimeBesideActiveOnes(t *testing.T) {
	entered := make(chan string)
	release := make(chan struct{})
	mk := func(name string, paused bool) (*Session, *gatedVerifyStorage) {
		st := &gatedVerifyStorage{name: name, entered: entered, release: release}
		return recheckSession(t, name, 2, st, paused), st
	}
	pausedA, _ := mk("paused-a", true)
	activeC, _ := mk("active-c", false)

	ctx := t.Context()
	var wg sync.WaitGroup
	run := func(s *Session) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runVerification(ctx)
		}()
	}
	run(pausedA)
	awaitEntry(t, entered, "paused-a")
	pausedB, _ := mk("paused-b", true)
	run(pausedB)
	run(activeC)

	// The active session hashes alongside the paused one; the second paused
	// session waits for the single paused slot and reports that it is queued.
	awaitEntry(t, entered, "active-c")
	assertNoEntry(t, entered, 400*time.Millisecond)
	pausedB.mu.RLock()
	status := pausedB.statusLocked()
	queued := pausedB.verifyQueued
	pausedB.mu.RUnlock()
	if status != "Queued" || !queued {
		t.Fatalf("second paused session status = %q (queued=%v), want Queued", status, queued)
	}
	if snap := pausedB.Snapshot(); !snap.Verification.Queued || !snap.Verification.Active {
		t.Fatalf("snapshot = %+v, want Active and Queued", snap.Verification)
	}

	// Releasing the first pieces lets both hashing sessions move to their second
	// piece; the second paused session only gets the slot once the first is done.
	release <- struct{}{}
	release <- struct{}{}
	awaitEntries(t, entered, "paused-a", "active-c")
	assertNoEntry(t, entered, 200*time.Millisecond)
	release <- struct{}{}
	release <- struct{}{}
	awaitEntry(t, entered, "paused-b")
	release <- struct{}{}
	awaitEntry(t, entered, "paused-b")
	release <- struct{}{}
	wg.Wait()

	for _, s := range []*Session{pausedA, pausedB, activeC} {
		s.mu.RLock()
		queued, states := s.verifyQueued, append([]PieceState(nil), s.PieceStates...)
		s.mu.RUnlock()
		if queued {
			t.Errorf("%s still marked queued after its recheck", s.Torrent.Name)
		}
		for i, st := range states {
			if st != PieceCompleted {
				t.Errorf("%s piece %d = %v, want completed", s.Torrent.Name, i, st)
			}
		}
	}
}

func TestResumedSessionLeavesThePausedQueue(t *testing.T) {
	entered := make(chan string)
	release := make(chan struct{})
	holder := recheckSession(t, "holder", 1, &gatedVerifyStorage{name: "holder", entered: entered, release: release}, true)
	waiter := recheckSession(t, "waiter", 1, &gatedVerifyStorage{name: "waiter", entered: entered, release: release}, true)

	ctx := t.Context()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); holder.runVerification(ctx) }()
	awaitEntry(t, entered, "holder")
	go func() { defer wg.Done(); waiter.runVerification(ctx) }()
	assertNoEntry(t, entered, 400*time.Millisecond)

	// Resuming the queued session moves it onto the active gate without waiting
	// for the paused slot to free up.
	waiter.mu.Lock()
	waiter.paused = false
	waiter.mu.Unlock()
	awaitEntry(t, entered, "waiter")
	release <- struct{}{}
	release <- struct{}{}
	wg.Wait()
}

func TestPausedRecheckYieldsOnlyToLiveTransfers(t *testing.T) {
	var mu sync.Mutex
	var yields []time.Duration
	orig := pausedRecheckYield
	pausedRecheckYield = func(ctx context.Context, d time.Duration) {
		mu.Lock()
		yields = append(yields, d)
		mu.Unlock()
	}
	t.Cleanup(func() { pausedRecheckYield = orig })
	const pieceDelay = 5 * time.Millisecond

	run := func(name string, paused bool, live bool) []time.Duration {
		mu.Lock()
		yields = nil
		mu.Unlock()
		sess := recheckSession(t, name, 3, &gatedVerifyStorage{name: name, delay: pieceDelay}, paused)
		if live {
			liveTransfers.Add(1)
			defer liveTransfers.Add(-1)
		}
		if !sess.runVerification(context.Background()) {
			t.Fatalf("%s: verification did not finish", name)
		}
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Duration(nil), yields...)
	}

	if got := run("paused-idle-disk", true, false); len(got) != 0 {
		t.Fatalf("paused recheck yielded %v with no live transfer", got)
	}
	if got := run("active-busy-disk", false, true); len(got) != 0 {
		t.Fatalf("active session's check yielded %v; only paused rechecks yield", got)
	}
	got := run("paused-busy-disk", true, true)
	if len(got) != 3 {
		t.Fatalf("paused recheck yielded %d times during a live transfer, want once per piece", len(got))
	}
	for i, d := range got {
		if d < 2*pieceDelay {
			t.Errorf("yield %d = %v, want at least twice the %v piece time", i, d, pieceDelay)
		}
	}
}

func TestLiveTransferGaugeFollowsSessionSpeed(t *testing.T) {
	sess := recheckSession(t, "gauge", 1, &gatedVerifyStorage{name: "gauge"}, false)
	before := liveTransfers.Load()
	sess.mu.Lock()
	sess.setLiveTransferLocked(true)
	sess.setLiveTransferLocked(true)
	sess.mu.Unlock()
	if got := liveTransfers.Load(); got != before+1 {
		t.Fatalf("gauge = %d after marking live twice, want %d", got, before+1)
	}
	sess.mu.Lock()
	sess.setLiveTransferLocked(false)
	sess.setLiveTransferLocked(false)
	sess.mu.Unlock()
	if got := liveTransfers.Load(); got != before {
		t.Fatalf("gauge = %d after clearing, want %d", got, before)
	}
}

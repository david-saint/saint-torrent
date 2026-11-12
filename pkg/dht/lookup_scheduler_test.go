package dht

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeLookupCall is one lookup the scheduler started.
type fakeLookupCall struct {
	infoHash [20]byte
	peerPort uint16
	opts     LookupOptions
	at       time.Time
}

// fakeLookups stands in for runLookup: each lookup blocks until the test
// releases its info-hash or the DHT closes.
type fakeLookups struct {
	mu         sync.Mutex
	calls      []fakeLookupCall
	running    int
	maxRunning int
	gates      map[[20]byte]chan struct{}
	started    chan fakeLookupCall
}

// installFakeLookups replaces runLookup and the start interval until the test
// ends. Call it before newFakeDHT, so the DHT is closed before they are
// restored.
func installFakeLookups(t *testing.T, interval time.Duration) *fakeLookups {
	t.Helper()
	f := &fakeLookups{
		gates:   make(map[[20]byte]chan struct{}),
		started: make(chan fakeLookupCall, 2*dhtMaxQueuedLookups),
	}
	oldRun, oldInterval := runLookup, dhtLookupStartInterval
	runLookup = func(d *DHT, ctx context.Context, infoHash [20]byte, peerPort uint16, opts LookupOptions) {
		call := fakeLookupCall{infoHash: infoHash, peerPort: peerPort, opts: opts, at: time.Now()}
		f.mu.Lock()
		f.calls = append(f.calls, call)
		f.running++
		f.maxRunning = max(f.maxRunning, f.running)
		gate := f.gateLocked(infoHash)
		f.mu.Unlock()
		f.started <- call
		select {
		case <-gate:
		case <-ctx.Done():
		}
		f.mu.Lock()
		f.running--
		f.mu.Unlock()
	}
	dhtLookupStartInterval = interval
	t.Cleanup(func() {
		runLookup, dhtLookupStartInterval = oldRun, oldInterval
	})
	return f
}

func (f *fakeLookups) gateLocked(infoHash [20]byte) chan struct{} {
	g, ok := f.gates[infoHash]
	if !ok {
		g = make(chan struct{})
		f.gates[infoHash] = g
	}
	return g
}

// release lets the running (or next) lookup of infoHash return.
func (f *fakeLookups) release(infoHash [20]byte) {
	f.mu.Lock()
	g := f.gateLocked(infoHash)
	delete(f.gates, infoHash)
	f.mu.Unlock()
	close(g)
}

// next waits for the scheduler to start a lookup.
func (f *fakeLookups) next(t *testing.T) fakeLookupCall {
	t.Helper()
	select {
	case c := <-f.started:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("no lookup was started")
		return fakeLookupCall{}
	}
}

// expectNoStart fails if a lookup starts within d.
func (f *fakeLookups) expectNoStart(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case c := <-f.started:
		t.Fatalf("lookup for %x started unexpectedly", c.infoHash[:4])
	case <-time.After(d):
	}
}

func (f *fakeLookups) callsFor(infoHash [20]byte) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.infoHash == infoHash {
			n++
		}
	}
	return n
}

func schedHash(i int) [20]byte {
	var h [20]byte
	copy(h[:], "scheduled-lookup----")
	h[16], h[17], h[18], h[19] = byte(i>>24), byte(i>>16), byte(i>>8), byte(i)
	return h
}

// awaitSchedIdle waits until no lookup is running.
func awaitSchedIdle(t *testing.T, d *DHT) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		d.sched.mu.Lock()
		n := len(d.sched.running)
		d.sched.mu.Unlock()
		if n == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("%d lookups still running", n)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// saturate starts dhtMaxConcurrentLookups blocking lookups, numbered from
// base, and returns their info-hashes.
func saturate(t *testing.T, d *DHT, f *fakeLookups, base int) [][20]byte {
	t.Helper()
	var hashes [][20]byte
	for i := 0; i < dhtMaxConcurrentLookups; i++ {
		h := schedHash(base + i)
		hashes = append(hashes, h)
		d.LookupWithOptions(h, 6881, LookupOptions{})
	}
	for range hashes {
		f.next(t)
	}
	return hashes
}

func pendingFor(d *DHT, infoHash [20]byte) (pendingLookup, bool) {
	d.sched.mu.Lock()
	defer d.sched.mu.Unlock()
	p, ok := d.sched.pending[infoHash]
	if !ok {
		return pendingLookup{}, false
	}
	return *p, true
}

// TestLookupSchedulerReturnsImmediately verifies LookupWithOptions never waits
// for the lookup it asks for.
func TestLookupSchedulerReturnsImmediately(t *testing.T) {
	f := installFakeLookups(t, time.Hour)
	d, _ := newFakeDHT(t)

	start := time.Now()
	for i := 0; i < 100; i++ {
		d.LookupWithOptions(schedHash(i), 6881, LookupOptions{})
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("100 lookup requests took %v", elapsed)
	}
	// The first start after an idle spell is immediate, the next waits an
	// hour. Which request it is depends on when the dispatcher first runs.
	c := f.next(t)
	requested := false
	for i := 0; i < 100 && !requested; i++ {
		requested = c.infoHash == schedHash(i)
	}
	if !requested {
		t.Fatalf("first lookup started was %x, not one requested", c.infoHash)
	}
	f.expectNoStart(t, 50*time.Millisecond)
}

// TestLookupSchedulerNewestFirstLookupFirst verifies first-time lookups are
// started newest first, so a torrent added just after a large restore does not
// queue behind every restored torrent's first lookup. Repeats stay FIFO.
func TestLookupSchedulerNewestFirstLookupFirst(t *testing.T) {
	f := installFakeLookups(t, 0)
	d, _ := newFakeDHT(t)

	repeats := []int{700, 701, 702}
	for _, i := range repeats {
		d.LookupWithOptions(schedHash(i), 6881, LookupOptions{})
		f.next(t)
		f.release(schedHash(i))
	}
	awaitSchedIdle(t, d)

	running := saturate(t, d, f, 0)
	for _, i := range repeats {
		d.LookupWithOptions(schedHash(i), 6881, LookupOptions{})
	}
	restored := []int{100, 101, 102, 103}
	for _, i := range restored {
		d.LookupWithOptions(schedHash(i), 6881, LookupOptions{})
	}
	added := schedHash(200)
	d.LookupWithOptions(added, 6881, LookupOptions{})

	want := []int{200, 103, 102, 101, 100, 700, 701, 702}
	for i, w := range want {
		f.release(running[i])
		if c := f.next(t); c.infoHash != schedHash(w) {
			t.Fatalf("start %d was %x, want %x", i, c.infoHash, schedHash(w))
		}
	}
}

// TestLookupSchedulerCoalesces verifies a request for a queued info-hash
// merges into the queued lookup (announce is kept, the latest port wins) and
// a request for a running one is dropped.
func TestLookupSchedulerCoalesces(t *testing.T) {
	f := installFakeLookups(t, 0)
	d, _ := newFakeDHT(t)

	running := saturate(t, d, f, 0)

	// Running: dropped, not queued behind itself.
	d.LookupWithOptions(running[0], 6881, LookupOptions{Announce: true})
	if _, queued := pendingFor(d, running[0]); queued {
		t.Fatal("a request for a running lookup was queued")
	}

	// Queued: merged.
	h := schedHash(100)
	d.LookupWithOptions(h, 1000, LookupOptions{Announce: false})
	d.LookupWithOptions(h, 2000, LookupOptions{Announce: true})
	d.LookupWithOptions(h, 0, LookupOptions{Announce: false})
	d.sched.mu.Lock()
	queued := len(d.sched.pending)
	d.sched.mu.Unlock()
	if queued != 1 {
		t.Fatalf("three requests for one info-hash queued %d lookups", queued)
	}

	f.release(running[0])
	c := f.next(t)
	if c.infoHash != h {
		t.Fatalf("the freed slot started %x, want the queued lookup", c.infoHash)
	}
	if !c.opts.Announce || c.peerPort != 2000 {
		t.Fatalf("merged lookup ran with announce=%v port=%d, want announce=true port=2000", c.opts.Announce, c.peerPort)
	}
	f.expectNoStart(t, 50*time.Millisecond)
	if got := f.callsFor(running[0]); got != 1 {
		t.Fatalf("the running info-hash was looked up %d times, want 1", got)
	}
	if got := f.callsFor(h); got != 1 {
		t.Fatalf("the merged info-hash was looked up %d times, want 1", got)
	}
}

// TestLookupSchedulerCapsConcurrency verifies at most dhtMaxConcurrentLookups
// run at once and a finished lookup frees exactly one slot.
func TestLookupSchedulerCapsConcurrency(t *testing.T) {
	f := installFakeLookups(t, 0)
	d, _ := newFakeDHT(t)

	const total = 3 * dhtMaxConcurrentLookups
	for i := 0; i < total; i++ {
		d.LookupWithOptions(schedHash(i), 6881, LookupOptions{})
	}
	var started []fakeLookupCall
	for i := 0; i < dhtMaxConcurrentLookups; i++ {
		started = append(started, f.next(t))
	}
	f.expectNoStart(t, 50*time.Millisecond)

	f.release(started[0].infoHash)
	started = append(started, f.next(t))
	f.expectNoStart(t, 50*time.Millisecond)

	for i := 1; i < total; i++ {
		if i >= len(started) {
			started = append(started, f.next(t))
		}
		f.release(started[i].infoHash)
	}
	awaitSchedIdle(t, d)
	f.mu.Lock()
	peak, calls := f.maxRunning, len(f.calls)
	f.mu.Unlock()
	if peak > dhtMaxConcurrentLookups {
		t.Fatalf("%d lookups ran at once, cap %d", peak, dhtMaxConcurrentLookups)
	}
	if calls != total {
		t.Fatalf("%d of %d queued lookups ran", calls, total)
	}
}

// TestLookupSchedulerPacesStarts verifies starts are spaced by the start
// interval, while the first start after an idle spell is immediate.
func TestLookupSchedulerPacesStarts(t *testing.T) {
	const interval = 250 * time.Millisecond
	f := installFakeLookups(t, interval)
	d, _ := newFakeDHT(t)

	requested := time.Now()
	for i := 0; i < 3; i++ {
		d.LookupWithOptions(schedHash(i), 6881, LookupOptions{})
	}
	first, second, third := f.next(t), f.next(t), f.next(t)
	if wait := first.at.Sub(requested); wait >= interval {
		t.Fatalf("the first lookup after an idle spell waited %v", wait)
	}
	if spread := third.at.Sub(requested); spread < 2*interval {
		t.Fatalf("three lookups started within %v, want them %v apart", spread, interval)
	}
	if second.at.Sub(requested) < interval {
		t.Fatalf("the second lookup started %v after the request, before the interval", second.at.Sub(requested))
	}

	// After an idle spell longer than the interval, the next start is
	// immediate again.
	time.Sleep(2 * interval)
	requested = time.Now()
	d.LookupWithOptions(schedHash(10), 6881, LookupOptions{})
	if wait := f.next(t).at.Sub(requested); wait >= interval {
		t.Fatalf("a lookup after an idle spell waited %v", wait)
	}
}

// TestLookupSchedulerTakePacing checks the pacing arithmetic without a clock.
func TestLookupSchedulerTakePacing(t *testing.T) {
	var s lookupScheduler
	const interval = 250 * time.Millisecond
	for i := 0; i < 3; i++ {
		s.enqueue(schedHash(i), 6881, LookupOptions{})
	}
	t0 := time.Unix(1000, 0)
	if p, _ := s.take(t0, interval); p == nil {
		t.Fatal("the first start was delayed")
	}
	p, wait := s.take(t0.Add(100*time.Millisecond), interval)
	if p != nil || wait != 150*time.Millisecond {
		t.Fatalf("take 100ms after a start = %v, wait %v; want nothing for 150ms", p, wait)
	}
	if p, _ := s.take(t0.Add(interval), interval); p == nil {
		t.Fatal("no start once the interval had passed")
	}
	if p, _ := s.take(t0.Add(time.Hour), interval); p == nil {
		t.Fatal("no immediate start after an idle spell")
	}
	if p, wait := s.take(t0.Add(2*time.Hour), interval); p != nil || wait != 0 {
		t.Fatalf("take on an empty queue = %v, wait %v; want nothing until woken", p, wait)
	}
}

// TestLookupSchedulerUrgentOvertakesRepeats verifies an info-hash looked up
// for the first time since the DHT started runs ahead of queued repeats.
func TestLookupSchedulerUrgentOvertakesRepeats(t *testing.T) {
	f := installFakeLookups(t, 0)
	d, _ := newFakeDHT(t)

	repeat := schedHash(500)
	d.LookupWithOptions(repeat, 6881, LookupOptions{})
	f.next(t)
	f.release(repeat)
	awaitSchedIdle(t, d)

	running := saturate(t, d, f, 0)
	d.LookupWithOptions(repeat, 6881, LookupOptions{})
	fresh := schedHash(600)
	d.LookupWithOptions(fresh, 6881, LookupOptions{})

	f.release(running[0])
	if c := f.next(t); c.infoHash != fresh {
		t.Fatalf("a repeat lookup ran ahead of a first-time one (%x)", c.infoHash)
	}
	f.release(running[1])
	if c := f.next(t); c.infoHash != repeat {
		t.Fatalf("the repeat lookup did not follow (%x)", c.infoHash)
	}
}

// TestLookupSchedulerFullSeenSetQueuesRoutine verifies that once the set of
// remembered info-hashes is full, new ones are queued as routine rather than
// growing it.
func TestLookupSchedulerFullSeenSetQueuesRoutine(t *testing.T) {
	var s lookupScheduler
	s.enqueue(schedHash(0), 6881, LookupOptions{})
	s.mu.Lock()
	for i := 1; len(s.seen) < dhtMaxRememberedLookups; i++ {
		s.seen[schedHash(1_000_000+i)] = struct{}{}
	}
	s.mu.Unlock()

	s.enqueue(schedHash(1), 6881, LookupOptions{})
	q := &s.levels[LookupPriorityLow]
	if len(q.urgent) != 1 || len(q.routine) != 1 || q.routine[0].infoHash != schedHash(1) {
		t.Fatalf("with the seen set full a new info-hash was queued urgent (urgent %d, routine %d)", len(q.urgent), len(q.routine))
	}
	for p, _ := s.take(time.Now(), 0); p != nil; p, _ = s.take(time.Now(), 0) {
		s.finish(p.infoHash)
	}
	if len(s.seen) != dhtMaxRememberedLookups {
		t.Fatalf("the seen set grew to %d, bound %d", len(s.seen), dhtMaxRememberedLookups)
	}
}

// TestLookupSchedulerQueueBound verifies requests beyond dhtMaxQueuedLookups
// distinct info-hashes are dropped.
func TestLookupSchedulerQueueBound(t *testing.T) {
	f := installFakeLookups(t, 0)
	d, _ := newFakeDHT(t)
	saturate(t, d, f, 0)

	for i := 0; i < dhtMaxQueuedLookups; i++ {
		d.LookupWithOptions(schedHash(10_000+i), 6881, LookupOptions{})
	}
	over := schedHash(99_999)
	d.LookupWithOptions(over, 6881, LookupOptions{})

	d.sched.mu.Lock()
	queued := len(d.sched.pending)
	queues := 0
	for _, lq := range d.sched.levels {
		queues += len(lq.urgent) + len(lq.routine)
	}
	d.sched.mu.Unlock()
	if queued != dhtMaxQueuedLookups || queues != dhtMaxQueuedLookups {
		t.Fatalf("queue holds %d (%d in FIFOs), bound %d", queued, queues, dhtMaxQueuedLookups)
	}
	if _, ok := pendingFor(d, over); ok {
		t.Fatal("a request past the queue bound was queued")
	}
	// A request merging into an entry already queued is still accepted.
	d.LookupWithOptions(schedHash(10_000), 6881, LookupOptions{Announce: true})
	if p, ok := pendingFor(d, schedHash(10_000)); !ok || !p.opts.Announce {
		t.Fatal("a full queue refused to merge a request for a queued info-hash")
	}
}

// TestLookupSchedulerCloseDiscardsPending verifies Close stops running
// lookups, never starts queued ones and ignores later requests.
func TestLookupSchedulerCloseDiscardsPending(t *testing.T) {
	f := installFakeLookups(t, 0)
	d, _ := newFakeDHT(t)
	saturate(t, d, f, 0)
	for i := 0; i < 5; i++ {
		d.LookupWithOptions(schedHash(100+i), 6881, LookupOptions{})
	}

	closed := make(chan struct{})
	go func() {
		d.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung on running lookups")
	}
	d.LookupWithOptions(schedHash(200), 6881, LookupOptions{})

	d.sched.mu.Lock()
	queued := len(d.sched.pending)
	for _, lq := range d.sched.levels {
		queued += len(lq.urgent) + len(lq.routine)
	}
	d.sched.mu.Unlock()
	if queued != 0 {
		t.Fatalf("%d lookups still queued after Close", queued)
	}
	f.mu.Lock()
	calls := len(f.calls)
	f.mu.Unlock()
	if calls != dhtMaxConcurrentLookups {
		t.Fatalf("%d lookups ran, want only the %d running at Close", calls, dhtMaxConcurrentLookups)
	}
}

// TestLookupSchedulerPriorityLevelsAndMagnetRetry verifies the three priority
// levels drain High -> Normal -> Low, keeping first-lookup LIFO and repeat FIFO
// within each level, and keeping a magnet retry at the top level ahead of
// downloads and seeds.
func TestLookupSchedulerPriorityLevelsAndMagnetRetry(t *testing.T) {
	f := installFakeLookups(t, 0)
	d, _ := newFakeDHT(t)

	// Run one lookup each for the magnet retry, a normal repeat, and a low
	// repeat so all three are in seen.
	magnetRetry := schedHash(900)
	normalRepeat := schedHash(800)
	lowRepeat := schedHash(700)
	for _, tc := range []struct {
		h [20]byte
		p LookupPriority
	}{
		{magnetRetry, LookupPriorityHigh},
		{normalRepeat, LookupPriorityNormal},
		{lowRepeat, LookupPriorityLow},
	} {
		d.LookupWithOptions(tc.h, 6881, LookupOptions{Priority: tc.p})
		f.next(t)
		f.release(tc.h)
	}
	awaitSchedIdle(t, d)

	running := saturate(t, d, f, 0)

	// Queue Low first (repeat + first-times), then Normal (repeat + first-times),
	// then High (magnet retry + first-times).
	d.LookupWithOptions(lowRepeat, 6881, LookupOptions{Priority: LookupPriorityLow})
	d.LookupWithOptions(schedHash(701), 6881, LookupOptions{Priority: LookupPriorityLow})
	d.LookupWithOptions(normalRepeat, 6881, LookupOptions{Priority: LookupPriorityNormal})
	d.LookupWithOptions(schedHash(801), 6881, LookupOptions{Priority: LookupPriorityNormal})
	d.LookupWithOptions(magnetRetry, 6881, LookupOptions{Priority: LookupPriorityHigh})
	d.LookupWithOptions(schedHash(901), 6881, LookupOptions{Priority: LookupPriorityHigh})

	// 6 items queued; all 6 start before the 8th start (anti-starvation), so
	// priority order governs all 6:
	// High urgent (901), High routine (900),
	// Normal urgent (801), Normal routine (800),
	// Low urgent (701), Low routine (700).
	want := []int{901, 900, 801, 800, 701, 700}
	for i, w := range want {
		f.release(running[i])
		if c := f.next(t); c.infoHash != schedHash(w) {
			t.Fatalf("start %d was %x, want %x", i, c.infoHash, schedHash(w))
		}
	}
}

// TestLookupSchedulerAntiStarvationOneInEight verifies one start in eight goes
// to the oldest queued request of any level so seeds are never starved by a
// continuous stream of higher-priority lookups.
func TestLookupSchedulerAntiStarvationOneInEight(t *testing.T) {
	var s lookupScheduler
	seed := schedHash(1)
	s.enqueue(seed, 6881, LookupOptions{Priority: LookupPriorityLow})
	for i := 0; i < 10; i++ {
		s.enqueue(schedHash(100+i), 6881, LookupOptions{Priority: LookupPriorityHigh})
	}

	now := time.Unix(1000, 0)
	for i := 0; i < 7; i++ {
		p, _ := s.take(now, 0)
		if p == nil || p.opts.Priority != LookupPriorityHigh {
			t.Fatalf("start %d picked %v, want High priority", i+1, p)
		}
		s.finish(p.infoHash)
	}
	// 8th start must pick the oldest queued request (the Low seed).
	p, _ := s.take(now, 0)
	if p == nil || p.infoHash != seed {
		t.Fatalf("8th start picked %v, want oldest queued request (%x)", p, seed)
	}
	s.finish(p.infoHash)
}

// TestLookupSchedulerCancelQueuedAndRunning verifies CancelLookup withdraws
// queued requests, stops running ones immediately, and lets a subsequent
// request for the same info-hash run once re-queued.
func TestLookupSchedulerCancelQueuedAndRunning(t *testing.T) {
	f := installFakeLookups(t, 0)
	d, _ := newFakeDHT(t)

	running := saturate(t, d, f, 0)
	queued := schedHash(500)
	kept := schedHash(501)
	d.LookupWithOptions(queued, 6881, LookupOptions{Priority: LookupPriorityHigh})
	d.LookupWithOptions(kept, 6881, LookupOptions{Priority: LookupPriorityNormal})

	// Cancel the queued lookup before any slot opens.
	d.CancelLookup(queued)
	if _, ok := pendingFor(d, queued); ok {
		t.Fatal("CancelLookup left queued lookup in pending")
	}

	// Cancel a running lookup; its ctx should unblock immediately and free its
	// slot for kept (never starting queued).
	d.CancelLookup(running[0])
	c := f.next(t)
	if c.infoHash != kept {
		t.Fatalf("freed slot started %x, want %x", c.infoHash, kept)
	}

	// Cancelling a running lookup and immediately re-enqueuing it must not be
	// dropped as already_running.
	d.CancelLookup(running[1])
	d.LookupWithOptions(running[1], 6881, LookupOptions{Priority: LookupPriorityHigh})
	c2 := f.next(t)
	if c2.infoHash != running[1] {
		t.Fatalf("re-queued lookup after cancel started %x, want %x", c2.infoHash, running[1])
	}
}

// TestLookupSchedulerMergePromotesPriority verifies merging a higher-priority
// request into an already-queued entry moves it to the higher level.
func TestLookupSchedulerMergePromotesPriority(t *testing.T) {
	var s lookupScheduler
	h1 := schedHash(1)
	h2 := schedHash(2)
	s.enqueue(h1, 6881, LookupOptions{Priority: LookupPriorityLow})
	s.enqueue(h2, 6881, LookupOptions{Priority: LookupPriorityNormal})
	// Promote h1 to High.
	res, _ := s.enqueue(h1, 6881, LookupOptions{Priority: LookupPriorityHigh, Announce: true})
	if res != lookupMerged {
		t.Fatalf("enqueue merge result = %v, want merged", res)
	}
	p, _ := s.take(time.Now(), 0)
	if p == nil || p.infoHash != h1 || !p.opts.Announce || p.opts.Priority != LookupPriorityHigh {
		t.Fatalf("first take = %+v, want promoted h1", p)
	}
}

package dht

import (
	"context"
	"fmt"
	"sync"
	"time"

	"sainttorrent/pkg/logging"
)

const (
	// dhtMaxConcurrentLookups bounds how many get_peers lookups run at once.
	// Each already keeps dhtLookupParallelism queries in flight, so this caps
	// the DHT's outbound query rate however many torrents ask at the same
	// moment (every restored session looks up within a second of startup and
	// then every 30 s in lockstep).
	dhtMaxConcurrentLookups = 8
	// dhtMaxQueuedLookups bounds the lookups waiting for a slot. Requests for
	// one info-hash coalesce, so this is a bound on distinct torrents.
	dhtMaxQueuedLookups = 4096
	// dhtMaxRememberedLookups bounds the set of info-hashes looked up since
	// this DHT started, which is what lets a torrent's first lookup overtake
	// the periodic repeats of others.
	dhtMaxRememberedLookups = 65536
	// dhtLookupAntiStarvationInterval is how many priority-ordered starts run
	// before one start goes to the oldest queued request of any level.
	dhtLookupAntiStarvationInterval = 7
)

// dhtLookupStartInterval spaces lookup starts, so a burst of requests turns
// into a steady trickle of queries instead of a spike that remote nodes'
// rate limits punish. The first start after an idle spell is immediate. It is
// a variable so tests can shorten it.
var dhtLookupStartInterval = 250 * time.Millisecond

// runLookup runs one lookup to completion. It is a variable so tests can
// substitute a fake that blocks or records its calls.
var runLookup = (*DHT).lookup

// pendingLookup is one queued lookup request.
type pendingLookup struct {
	infoHash [20]byte
	peerPort uint16
	opts     LookupOptions
	seq      uint64
	urgent   bool
}

type runningLookup struct {
	cancel    context.CancelFunc
	cancelled bool
}

type levelQueue struct {
	urgent  []*pendingLookup // first lookups, newest-first (LIFO)
	routine []*pendingLookup // repeats, in turn (FIFO)
}

// lookupScheduler queues lookups and starts them under the concurrency,
// pacing and priority limits above. Levels drain highest first, and within
// each level urgent (first lookups since this DHT started) drains newest-first
// ahead of routine (repeats, FIFO). One start in eight goes to the oldest
// queued request of any level so lower-priority torrents are never starved.
type lookupScheduler struct {
	mu          sync.Mutex
	pending     map[[20]byte]*pendingLookup // queued, by info-hash
	levels      [numLookupPriorities]levelQueue
	running     map[[20]byte]*runningLookup
	seen        map[[20]byte]struct{} // info-hashes started since this DHT started
	nextSeq     uint64
	prioStarts  int
	lastStart   time.Time
	dispatching bool
	closed      bool
	wake        chan struct{}
}

// lookupEnqueueResult says what became of a lookup request.
type lookupEnqueueResult uint8

const (
	lookupQueued lookupEnqueueResult = iota
	lookupMerged
	lookupAlreadyRunning
	lookupQueueFull
	lookupClosed
)

func (r lookupEnqueueResult) String() string {
	switch r {
	case lookupQueued:
		return "queued"
	case lookupMerged:
		return "merged"
	case lookupAlreadyRunning:
		return "already_running"
	case lookupQueueFull:
		return "queue_full"
	case lookupClosed:
		return "closed"
	}
	return "unknown"
}

func normalizePriority(p LookupPriority) LookupPriority {
	if p > LookupPriorityHigh {
		return LookupPriorityHigh
	}
	return p
}

func removeLookup(slice []*pendingLookup, target *pendingLookup) []*pendingLookup {
	for i, p := range slice {
		if p == target {
			copy(slice[i:], slice[i+1:])
			slice[len(slice)-1] = nil
			return slice[:len(slice)-1]
		}
	}
	return slice
}

func insertLookupBySeq(slice []*pendingLookup, p *pendingLookup) []*pendingLookup {
	i := len(slice)
	for i > 0 && slice[i-1].seq > p.seq {
		i--
	}
	slice = append(slice, nil)
	copy(slice[i+1:], slice[i:])
	slice[i] = p
	return slice
}

// enqueue queues a lookup request. A request for an info-hash already queued
// merges into it: announcing is kept if either asked for it, the latest
// non-zero port wins, and a higher priority promotes it. A request for an
// info-hash whose lookup is running (and not cancelled) is dropped, since
// callers repeat on their own cadence. startDispatcher reports that the caller
// must launch the dispatcher goroutine.
func (s *lookupScheduler) enqueue(infoHash [20]byte, peerPort uint16, opts LookupOptions) (res lookupEnqueueResult, startDispatcher bool) {
	opts.Priority = normalizePriority(opts.Priority)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return lookupClosed, false
	}
	if s.pending == nil {
		s.pending = make(map[[20]byte]*pendingLookup)
		s.running = make(map[[20]byte]*runningLookup)
		s.seen = make(map[[20]byte]struct{})
	}
	if p, ok := s.pending[infoHash]; ok {
		p.opts.Announce = p.opts.Announce || opts.Announce
		if peerPort != 0 {
			p.peerPort = peerPort
		}
		if opts.Priority > p.opts.Priority {
			oldQ := &s.levels[p.opts.Priority]
			newQ := &s.levels[opts.Priority]
			if p.urgent {
				oldQ.urgent = removeLookup(oldQ.urgent, p)
				newQ.urgent = insertLookupBySeq(newQ.urgent, p)
			} else {
				oldQ.routine = removeLookup(oldQ.routine, p)
				newQ.routine = insertLookupBySeq(newQ.routine, p)
			}
			p.opts.Priority = opts.Priority
		}
		return lookupMerged, false
	}
	if r, ok := s.running[infoHash]; ok && !r.cancelled {
		return lookupAlreadyRunning, false
	}
	if len(s.pending) >= dhtMaxQueuedLookups {
		return lookupQueueFull, false
	}
	s.nextSeq++
	_, seen := s.seen[infoHash]
	urgent := !seen && len(s.seen) < dhtMaxRememberedLookups
	p := &pendingLookup{
		infoHash: infoHash,
		peerPort: peerPort,
		opts:     opts,
		seq:      s.nextSeq,
		urgent:   urgent,
	}
	s.pending[infoHash] = p
	q := &s.levels[opts.Priority]
	if urgent {
		q.urgent = append(q.urgent, p)
	} else {
		q.routine = append(q.routine, p)
	}
	s.signalLocked()
	if !s.dispatching {
		s.dispatching = true
		return lookupQueued, true
	}
	return lookupQueued, false
}

// cancel removes any queued lookup for infoHash and cancels an active one if
// it is running.
func (s *lookupScheduler) cancel(infoHash [20]byte) {
	s.mu.Lock()
	if p, ok := s.pending[infoHash]; ok {
		delete(s.pending, infoHash)
		q := &s.levels[p.opts.Priority]
		if p.urgent {
			q.urgent = removeLookup(q.urgent, p)
		} else {
			q.routine = removeLookup(q.routine, p)
		}
	}
	var cancelFn context.CancelFunc
	if r, ok := s.running[infoHash]; ok && !r.cancelled {
		r.cancelled = true
		cancelFn = r.cancel
	}
	s.mu.Unlock()
	if cancelFn != nil {
		cancelFn()
	}
}

func (s *lookupScheduler) popByPriorityLocked() *pendingLookup {
	for lvl := int(LookupPriorityHigh); lvl >= int(LookupPriorityLow); lvl-- {
		q := &s.levels[lvl]
		for i := len(q.urgent) - 1; i >= 0; i-- {
			p := q.urgent[i]
			if _, busy := s.running[p.infoHash]; busy {
				continue
			}
			copy(q.urgent[i:], q.urgent[i+1:])
			q.urgent[len(q.urgent)-1] = nil
			q.urgent = q.urgent[:len(q.urgent)-1]
			return p
		}
		for i := 0; i < len(q.routine); i++ {
			p := q.routine[i]
			if _, busy := s.running[p.infoHash]; busy {
				continue
			}
			copy(q.routine[i:], q.routine[i+1:])
			q.routine[len(q.routine)-1] = nil
			q.routine = q.routine[:len(q.routine)-1]
			return p
		}
	}
	return nil
}

func (s *lookupScheduler) popOldestReadyLocked() *pendingLookup {
	var best *pendingLookup
	var bestSlice *[]*pendingLookup
	bestIdx := -1
	consider := func(slice *[]*pendingLookup) {
		for i, p := range *slice {
			if _, busy := s.running[p.infoHash]; busy {
				continue
			}
			if best == nil || p.seq < best.seq {
				best = p
				bestSlice = slice
				bestIdx = i
			}
			break // slices stay sorted by increasing seq
		}
	}
	for lvl := 0; lvl < numLookupPriorities; lvl++ {
		consider(&s.levels[lvl].urgent)
		consider(&s.levels[lvl].routine)
	}
	if best == nil {
		return nil
	}
	sl := *bestSlice
	copy(sl[bestIdx:], sl[bestIdx+1:])
	sl[len(sl)-1] = nil
	*bestSlice = sl[:len(sl)-1]
	return best
}

// take starts the next queued lookup if a slot is free and the pacing
// interval since the previous start has passed. With nothing to start it
// returns nil and how long to wait before asking again; zero means wait for a
// wake-up.
func (s *lookupScheduler) take(now time.Time, interval time.Duration) (*pendingLookup, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.running) >= dhtMaxConcurrentLookups || len(s.pending) == 0 {
		return nil, 0
	}
	if !s.lastStart.IsZero() {
		if elapsed := now.Sub(s.lastStart); elapsed >= 0 && elapsed < interval {
			return nil, interval - elapsed
		}
	}
	var p *pendingLookup
	if s.prioStarts >= dhtLookupAntiStarvationInterval {
		p = s.popOldestReadyLocked()
		if p != nil {
			s.prioStarts = 0
		}
	} else {
		p = s.popByPriorityLocked()
		if p != nil {
			if len(s.pending) <= 1 {
				s.prioStarts = 0
			} else {
				s.prioStarts++
			}
		}
	}
	if p == nil {
		return nil, 0
	}
	delete(s.pending, p.infoHash)
	s.running[p.infoHash] = &runningLookup{}
	if len(s.seen) < dhtMaxRememberedLookups {
		s.seen[p.infoHash] = struct{}{}
	}
	s.lastStart = now
	return p, 0
}

// attachCancel binds the per-lookup cancel func to the running entry, or
// invokes it immediately if CancelLookup raced between take and launch.
func (s *lookupScheduler) attachCancel(infoHash [20]byte, cancel context.CancelFunc) {
	s.mu.Lock()
	r, ok := s.running[infoHash]
	cancelled := ok && r.cancelled
	if ok && !cancelled {
		r.cancel = cancel
	}
	s.mu.Unlock()
	if cancelled {
		cancel()
	}
}

// finish records that the lookup for infoHash has returned and wakes the
// dispatcher, which may now start another.
func (s *lookupScheduler) finish(infoHash [20]byte) {
	s.mu.Lock()
	delete(s.running, infoHash)
	s.signalLocked()
	s.mu.Unlock()
}

// close discards every queued request and refuses new ones.
func (s *lookupScheduler) close() {
	s.mu.Lock()
	s.closed = true
	s.pending = nil
	s.levels = [numLookupPriorities]levelQueue{}
	s.mu.Unlock()
}

// signalLocked wakes the dispatcher without blocking; a wake-up already
// pending covers this one.
func (s *lookupScheduler) signalLocked() {
	if s.wake == nil {
		s.wake = make(chan struct{}, 1)
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// wakeChan returns the dispatcher's wake-up channel.
func (s *lookupScheduler) wakeChan() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wake == nil {
		s.wake = make(chan struct{}, 1)
	}
	return s.wake
}

// dispatchLookups is the scheduler's single dispatcher goroutine. It starts
// queued lookups, each on its own tracked goroutine, and sleeps until a slot
// frees, a request arrives or the pacing interval passes. It exits when the
// DHT closes.
func (d *DHT) dispatchLookups() {
	wake := d.sched.wakeChan()
	for {
		if d.ctx.Err() != nil {
			return
		}
		p, wait := d.sched.take(time.Now(), dhtLookupStartInterval)
		if p != nil {
			ctx, cancel := context.WithCancel(d.ctx)
			d.sched.attachCancel(p.infoHash, cancel)
			if !d.goTracked(func() {
				defer cancel()
				defer d.sched.finish(p.infoHash)
				runLookup(d, ctx, p.infoHash, p.peerPort, p.opts)
			}) {
				cancel()
				d.sched.finish(p.infoHash)
			}
			continue
		}
		var timer *time.Timer
		var due <-chan time.Time
		if wait > 0 {
			timer = time.NewTimer(wait)
			due = timer.C
		}
		select {
		case <-d.ctx.Done():
		case <-wake:
		case <-due:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// queueLookup hands a lookup request to the scheduler, launching its
// dispatcher on first use.
func (d *DHT) queueLookup(infoHash [20]byte, peerPort uint16, opts LookupOptions) {
	if d.ctx.Err() != nil {
		return
	}
	res, start := d.sched.enqueue(infoHash, peerPort, opts)
	if start {
		d.goTracked(d.dispatchLookups)
	}
	if (res == lookupQueueFull || res == lookupAlreadyRunning) && logging.Enabled() {
		logging.Debug("dht_lookup_skipped",
			logging.String("info_hash", fmt.Sprintf("%x", infoHash)),
			logging.String("reason", res.String()),
		)
	}
}

package dht

import (
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
}

// lookupScheduler queues lookups and starts them under the concurrency and
// pacing limits above. Two queues are drained urgent first: urgent holds
// info-hashes not looked up since this DHT started (a newly added torrent or
// magnet), routine holds the periodic repeats. Routine is FIFO, so repeats
// take turns. Urgent is drained newest first: at startup every restored
// torrent's first lookup lands there within a second or two, and a magnet the
// user adds just after (the magnet launcher starts the client with one) must
// not wait for hundreds of them, which at a few lookups a second is minutes.
type lookupScheduler struct {
	mu          sync.Mutex
	pending     map[[20]byte]*pendingLookup // queued, by info-hash
	urgent      []*pendingLookup
	routine     []*pendingLookup
	running     map[[20]byte]struct{}
	seen        map[[20]byte]struct{} // info-hashes started since this DHT started
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

// enqueue queues a lookup request. A request for an info-hash already queued
// merges into it: announcing is kept if either asked for it, and the latest
// non-zero port wins. A request for an info-hash whose lookup is running is
// dropped, since callers repeat on their own cadence. startDispatcher reports
// that the caller must launch the dispatcher goroutine.
func (s *lookupScheduler) enqueue(infoHash [20]byte, peerPort uint16, opts LookupOptions) (res lookupEnqueueResult, startDispatcher bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return lookupClosed, false
	}
	if s.pending == nil {
		s.pending = make(map[[20]byte]*pendingLookup)
		s.running = make(map[[20]byte]struct{})
		s.seen = make(map[[20]byte]struct{})
	}
	if p, ok := s.pending[infoHash]; ok {
		p.opts.Announce = p.opts.Announce || opts.Announce
		if peerPort != 0 {
			p.peerPort = peerPort
		}
		return lookupMerged, false
	}
	if _, ok := s.running[infoHash]; ok {
		return lookupAlreadyRunning, false
	}
	if len(s.pending) >= dhtMaxQueuedLookups {
		return lookupQueueFull, false
	}
	p := &pendingLookup{infoHash: infoHash, peerPort: peerPort, opts: opts}
	s.pending[infoHash] = p
	_, seen := s.seen[infoHash]
	if !seen && len(s.seen) < dhtMaxRememberedLookups {
		s.urgent = append(s.urgent, p)
	} else {
		s.routine = append(s.routine, p)
	}
	s.signalLocked()
	if !s.dispatching {
		s.dispatching = true
		return lookupQueued, true
	}
	return lookupQueued, false
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
	if n := len(s.urgent); n > 0 {
		p = s.urgent[n-1]
		s.urgent[n-1] = nil
		s.urgent = s.urgent[:n-1]
	} else {
		p = s.routine[0]
		s.routine[0] = nil
		s.routine = s.routine[1:]
	}
	delete(s.pending, p.infoHash)
	s.running[p.infoHash] = struct{}{}
	if len(s.seen) < dhtMaxRememberedLookups {
		s.seen[p.infoHash] = struct{}{}
	}
	s.lastStart = now
	return p, 0
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
	s.urgent = nil
	s.routine = nil
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
			d.goTracked(func() {
				defer d.sched.finish(p.infoHash)
				runLookup(d, p.infoHash, p.peerPort, p.opts)
			})
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

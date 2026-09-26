package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/tracker"
)

const trackerDefaultNumWant = 200

// maxTrackerResponse caps how many bytes of an HTTP tracker's announce response we
// buffer. Legitimate replies are a few KB even at numwant=200 (256 KiB holds ~40k
// compact peers); the cap bounds what a malicious or MITM'd tracker can make the
// generic bencode decoder allocate.
const maxTrackerResponse = 256 * 1024

const (
	// maxSessionTrackers caps how many distinct trackers one torrent announces to.
	// Real torrents list well under 100; the cap stops a crafted announce-list or
	// magnet from turning every cycle into thousands of requests.
	maxSessionTrackers = 200
	// maxTrackerScan bounds how many raw list entries are parsed to find them.
	maxTrackerScan = 16 * maxSessionTrackers
	// maxTrackersPerHost keeps one host from being listed under many paths, so
	// a torrent cannot aim its whole tracker list at a single victim server.
	maxTrackersPerHost = 2
	// trackerAnnounceWorkers bounds one session's concurrent tracker requests.
	trackerAnnounceWorkers = 16
	// maxConcurrentTrackerRequests bounds tracker requests (sockets) in flight
	// across every session. It is well above libtorrent's 50, which counts HTTP
	// announces only and meets a few active torrents: here every torrent
	// announces to all its trackers at once, and a dead tracker holds its slot
	// for the whole timeout, so a budget of 50 made a restart with a dozen
	// torrents queue their live trackers behind each other's dead ones.
	maxConcurrentTrackerRequests = 200
	// trackerLowPeers is the connection count below which a downloading session
	// re-announces early (trackerEarlyReannounce) instead of waiting out the
	// tracker's full interval.
	trackerLowPeers = 30
)

// Tracker timing. Vars so tests can shorten them; treat them as constants.
var (
	trackerAnnounceTimeout = 15 * time.Second
	// trackerMinInterval floors a tracker-supplied interval, so a hostile or
	// broken tracker answering interval=1 cannot make us re-announce to it every
	// few seconds. It only affects that tracker: each tracker keeps its own
	// schedule.
	trackerMinInterval = time.Minute
	// trackerMaxInterval caps a tracker-supplied interval (and min interval).
	trackerMaxInterval = time.Hour
	// trackerDefaultInterval applies when a tracker sends no usable interval.
	trackerDefaultInterval = 30 * time.Minute
	// trackerEarlyReannounce is how soon after a successful announce a tracker may
	// be asked again while the session is short of peers. A tracker's own "min
	// interval" is honoured when it is longer.
	trackerEarlyReannounce = 5 * time.Minute
	// trackerRetryBaseDelay and trackerRetryMaxDelay bound the per-tracker backoff
	// after failures (doubling), so a dead tracker is not retried every cycle.
	trackerRetryBaseDelay = 15 * time.Second
	trackerRetryMaxDelay  = 30 * time.Minute
	// trackerIdlePoll is the longest trackerLoop sleeps, so the short-of-peers
	// check is re-evaluated even when every tracker is far from due.
	trackerIdlePoll = time.Minute
	// trackerEventFlushDelay spaces back-to-back tracker events.
	trackerEventFlushDelay = 100 * time.Millisecond
)

// scrapeMinInterval throttles how often the best-effort scrape runs. Announce
// already supplies seeders/leechers every cycle; scrape's only net-new datum is
// the (slowly-changing, cosmetic) completed count, so a full extra tracker
// round-trip on every announce cycle isn't worth the traffic and added loop
// latency. Scraping on this slower cadence keeps the count fresh enough.
var scrapeMinInterval = 15 * time.Minute

// trackerRequestSlots is the process-wide semaphore behind
// maxConcurrentTrackerRequests, held only for the network round trip.
var trackerRequestSlots = make(chan struct{}, maxConcurrentTrackerRequests)

func acquireTrackerSlot(ctx context.Context) bool {
	select {
	case trackerRequestSlots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func releaseTrackerSlot() { <-trackerRequestSlots }

// runBounded calls fn(i) for i in [0, n) on at most workers goroutines. Workers
// stop taking new items once ctx is done. It does not wait for them.
func runBounded(ctx context.Context, n, workers int, fn func(i int)) {
	var next atomic.Int64
	for range min(workers, n) {
		go func() {
			for ctx.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				fn(i)
			}
		}()
	}
}

// trackerLoop handles tracker announces. Each tracker keeps its own schedule
// (see trackerSchedule); the loop sleeps until the next one is due, a queued
// event arrives on resumeCh, or the session closes.
func (s *Session) trackerLoop() {
	defer s.wg.Done()
	defer s.crashGuard("tracker")()

	// sched and lastScrape are loop-local state (only this goroutine touches
	// them), so they need no locking. The zero lastScrape forces a scrape on
	// the first cycle.
	sched := &trackerSchedule{}
	var lastScrape time.Time

	for {
		s.mu.RLock()
		paused := s.paused
		hasEvents := len(s.trackerEvents) > 0
		s.mu.RUnlock()

		if !paused || hasEvents {
			s.announceDue(sched)
			// Best-effort scrape to surface swarm health (seeders / leechers /
			// completed) for the TUI. The completed count is unavailable from
			// announce, so it can only come from scrape. Throttled to
			// scrapeMinInterval so it doesn't double tracker traffic every cycle.
			if !paused && time.Since(lastScrape) >= scrapeMinInterval {
				s.scrapeTargets(sched.healthyTargets())
				lastScrape = time.Now()
			}
		}
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		s.mu.RLock()
		paused = s.paused
		hasMoreEvents := len(s.trackerEvents) > 0
		needPeers := s.trackerNeedsPeersLocked()
		s.mu.RUnlock()

		timer := time.NewTimer(sched.sleep(time.Now(), paused, hasMoreEvents, needPeers))
		select {
		case <-timer.C:
		case <-s.resumeCh:
			// Event triggered — announce immediately
			timer.Stop()
		case <-s.ctx.Done():
			timer.Stop()
			return
		}
	}
}

// trackerNeedsPeersLocked reports whether the session is downloading with few
// enough connections that an early re-announce is worth the tracker load.
func (s *Session) trackerNeedsPeersLocked() bool {
	return !s.paused && !s.closed && !s.isCompletedLocked() && len(s.activePeers) < trackerLowPeers
}

// trackerTarget is one validated, deduplicated tracker from the torrent's list.
type trackerTarget struct {
	url string // as listed; used for requests
	id  string // scheme://host[:port], safe for logs
	udp bool
	// source is the tracker's own address when its URL names a literal IP or
	// "localhost", the zero Addr otherwise. netpolicy lets only a loopback or
	// LAN tracker hand out loopback or LAN peers.
	source netip.Addr
}

// sanitizeTrackers validates, deduplicates and caps a torrent's tracker list:
// only http, https and udp (any case), each URL once, at most
// maxTrackersPerHost per scheme+host+port and maxSessionTrackers in total.
func sanitizeTrackers(raw []string) []trackerTarget {
	targets := make([]trackerTarget, 0, min(len(raw), maxSessionTrackers))
	seen := make(map[string]struct{})
	perHost := make(map[string]int)
	for i, r := range raw {
		if len(targets) >= maxSessionTrackers || i >= maxTrackerScan {
			break
		}
		r = strings.TrimSpace(r)
		u, err := tracker.ParseAnnounceURL(r)
		if err != nil {
			continue
		}
		port := u.Port()
		switch {
		case port != "":
		case u.Scheme == "http":
			port = "80"
		case u.Scheme == "https":
			port = "443"
		}
		hostKey := u.Scheme + "://" + net.JoinHostPort(canonicalHost(u.Hostname()), port)
		key := hostKey + u.EscapedPath() + "?" + u.RawQuery
		if _, dup := seen[key]; dup || perHost[hostKey] >= maxTrackersPerHost {
			continue
		}
		seen[key] = struct{}{}
		perHost[hostKey]++
		targets = append(targets, trackerTarget{
			url:    r,
			id:     trackerLogID(r),
			udp:    u.Scheme == "udp",
			source: trackerSourceAddr(u.Hostname()),
		})
	}
	return targets
}

func trackerSourceAddr(host string) netip.Addr {
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") {
		return netip.AddrFrom4([4]byte{127, 0, 0, 1})
	}
	if a, err := netip.ParseAddr(host); err == nil {
		return a.Unmap()
	}
	return netip.Addr{}
}

// trackerSchedule is trackerLoop's per-tracker announce state. Only the
// goroutine running an announce round touches it, so it needs no locking and
// adds nothing to s.mu.
type trackerSchedule struct {
	built   bool
	targets []trackerTarget
	states  []trackerState // parallel to targets
}

type trackerState struct {
	next      time.Time // next regular announce, or retry after a failure
	early     time.Time // earliest re-announce while short of peers (zero: none)
	fails     int       // consecutive failures
	succeeded bool      // answered since its last failure
	// Swarm counts from its last successful announce.
	complete   int
	incomplete int
}

// build sanitizes the torrent's tracker list once. Torrent.Trackers never
// changes after the session is created.
func (sc *trackerSchedule) build(raw []string) {
	if sc.built {
		return
	}
	sc.built = true
	sc.targets = sanitizeTrackers(raw)
	sc.states = make([]trackerState, len(sc.targets))
}

// due returns the indexes of the trackers to announce to now. A tracker in
// failure backoff waits for its retry time even for events; a healthy one
// takes a pending event at once, and otherwise waits for its interval, or for
// its early re-announce time when the session is short of peers.
func (sc *trackerSchedule) due(now time.Time, event, needPeers bool) []int {
	var due []int
	for i := range sc.states {
		st := &sc.states[i]
		switch {
		case st.fails > 0:
			if now.Before(st.next) {
				continue
			}
		case event, !now.Before(st.next):
		case needPeers && !st.early.IsZero() && !now.Before(st.early):
		default:
			continue
		}
		due = append(due, i)
	}
	return due
}

// record updates tracker i's schedule from an announce result.
func (sc *trackerSchedule) record(i int, r trackerAnnounceResult, now time.Time) {
	st := &sc.states[i]
	if r.err != nil {
		st.fails++
		st.succeeded = false
		st.next = now.Add(trackerRetryDelay(st.fails))
		st.early = time.Time{}
		return
	}
	st.fails = 0
	st.succeeded = true
	st.complete = r.complete
	st.incomplete = r.incomplete
	minInterval := clampTrackerSeconds(r.minInterval, 0, trackerMaxInterval)
	interval := trackerDefaultInterval
	if r.interval > 0 {
		interval = clampTrackerSeconds(r.interval, trackerMinInterval, trackerMaxInterval)
	}
	st.next = now.Add(max(interval, minInterval))
	st.early = now.Add(max(trackerEarlyReannounce, minInterval))
}

// clampTrackerSeconds converts a tracker-supplied count of seconds to a
// Duration within [lo, hi], comparing in seconds first so no value can
// overflow the multiplication.
func clampTrackerSeconds(secs int, lo, hi time.Duration) time.Duration {
	if int64(secs) >= int64(hi/time.Second) {
		return hi
	}
	return max(time.Duration(secs)*time.Second, lo)
}

// trackerRetryDelay is the backoff after the given number of consecutive
// failures: trackerRetryBaseDelay doubling up to trackerRetryMaxDelay.
func trackerRetryDelay(fails int) time.Duration {
	d := trackerRetryBaseDelay
	for i := 1; i < fails && d < trackerRetryMaxDelay; i++ {
		d *= 2
	}
	return min(d, trackerRetryMaxDelay)
}

// sleep returns how long trackerLoop may wait before its next round.
func (sc *trackerSchedule) sleep(now time.Time, paused, events, needPeers bool) time.Duration {
	if paused && !events {
		return trackerIdlePoll // Resume wakes the loop through resumeCh
	}
	var wake time.Time
	for i := range sc.states {
		st := &sc.states[i]
		t := st.next
		if st.fails == 0 {
			switch {
			case events:
				t = now
			case needPeers && !st.early.IsZero() && st.early.Before(t):
				t = st.early
			}
		}
		if wake.IsZero() || t.Before(wake) {
			wake = t
		}
	}
	if wake.IsZero() {
		return trackerIdlePoll
	}
	return min(max(wake.Sub(now), trackerEventFlushDelay), trackerIdlePoll)
}

// swarmTotals returns the largest seeder and leecher counts among trackers
// that are currently answering, and whether any is.
func (sc *trackerSchedule) swarmTotals() (seeders, leechers int, healthy bool) {
	for i := range sc.states {
		st := &sc.states[i]
		if st.fails == 0 && st.succeeded {
			healthy = true
			seeders = max(seeders, st.complete)
			leechers = max(leechers, st.incomplete)
		}
	}
	return seeders, leechers, healthy
}

// healthyTargets returns the trackers whose last announce succeeded; scrape
// skips the rest rather than waiting out their timeouts.
func (sc *trackerSchedule) healthyTargets() []trackerTarget {
	var out []trackerTarget
	for i := range sc.states {
		if sc.states[i].fails == 0 && sc.states[i].succeeded {
			out = append(out, sc.targets[i])
		}
	}
	return out
}

type trackerAnnounceResult struct {
	tracker     string
	peers       []tracker.Peer
	interval    int
	minInterval int
	complete    int
	incomplete  int
	err         error
}

type trackerAnnounceParams struct {
	infoHash   [20]byte
	peerID     [20]byte
	port       uint16
	uploaded   int64
	downloaded int64
	left       int64
}

// trackerAnnounceParamsLocked snapshots what an announce reports. Callers hold s.mu.
func (s *Session) trackerAnnounceParamsLocked() trackerAnnounceParams {
	p := trackerAnnounceParams{
		infoHash: s.Torrent.InfoHash,
		peerID:   s.PeerID,
		port:     s.Port,
		uploaded: s.Uploaded.Load(),
	}
	if s.metadataMode || s.Storage == nil || len(s.PieceStates) == 0 {
		p.left = 1
	} else {
		stats := s.completionStatsLocked()
		p.downloaded = stats.completedTotalBytes
		p.left = max(stats.totalBytes-stats.completedTotalBytes, 0)
	}
	return p
}

func trackerLogID(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return "invalid"
	}
	scheme := strings.ToLower(u.Scheme)
	host := u.Hostname()
	if host == "" {
		return scheme + "://unknown"
	}
	if port := u.Port(); port != "" {
		host = net.JoinHostPort(host, port)
	}
	return scheme + "://" + host
}

func trackerLogErr(err error) error {
	if err == nil {
		return nil
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Errorf("tracker URL error: %v", urlErr.Err)
	}
	return err
}

func announceTracker(ctx context.Context, t trackerTarget, p trackerAnnounceParams, event string, timeout time.Duration) trackerAnnounceResult {
	announceCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var resp *tracker.TrackerResponse
	var err error
	if t.udp {
		resp, err = tracker.UDPAnnounce(announceCtx, t.url, p.infoHash, p.peerID, p.port, p.uploaded, p.downloaded, p.left, event, trackerDefaultNumWant)
	} else {
		resp, err = httpAnnounce(announceCtx, t.url, p, event)
	}
	if err != nil {
		return trackerAnnounceResult{tracker: t.id, err: trackerLogErr(err)}
	}
	return trackerAnnounceResult{
		tracker:     t.id,
		peers:       resp.Peers,
		interval:    resp.Interval,
		minInterval: resp.MinInterval,
		complete:    resp.Complete,
		incomplete:  resp.Incomplete,
	}
}

func httpAnnounce(ctx context.Context, rawURL string, p trackerAnnounceParams, event string) (*tracker.TrackerResponse, error) {
	u, err := tracker.BuildTrackerURL(rawURL, p.infoHash, p.peerID, p.port, p.uploaded, p.downloaded, p.left, true, event, trackerDefaultNumWant)
	if err != nil {
		return nil, err
	}
	req, err := tracker.NewRequest(ctx, tracker.PurposeAnnounce, u)
	if err != nil {
		return nil, err
	}
	resp, err := tracker.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Drain a little so the connection can be reused; error pages are not
		// tracker replies.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return nil, fmt.Errorf("tracker returned HTTP %d", resp.StatusCode)
	}
	// Bound how much we buffer (see maxTrackerResponse).
	data, err := tracker.ReadCappedBody(resp.Body, maxTrackerResponse)
	if err != nil {
		return nil, err
	}
	return tracker.ParseTrackerResponse(data)
}

// announceAndConnect announces to every tracker now, ignoring any schedule,
// and dials the peers they return.
func (s *Session) announceAndConnect() {
	s.announceDue(&trackerSchedule{})
}

// announceDue announces to the trackers in sched that are due (all of them for
// a pending event) through a bounded worker pool, and dials each tracker's
// peers as soon as its answer arrives, so one slow or dead tracker never delays
// the first connections.
func (s *Session) announceDue(sched *trackerSchedule) {
	s.mu.Lock()
	rawTrackers := s.Torrent.Trackers
	params := s.trackerAnnounceParamsLocked()
	var event string
	if len(s.trackerEvents) > 0 {
		event = s.trackerEvents[0]
		s.trackerEvents = s.trackerEvents[1:]
	}
	needPeers := s.trackerNeedsPeersLocked()
	s.mu.Unlock()

	// Parsing the list happens outside s.mu; Torrent.Trackers is immutable.
	sched.build(rawTrackers)
	if len(sched.targets) == 0 {
		// No usable trackers: every queued event counts as delivered.
		s.mu.Lock()
		s.finishTrackerEventLocked(event, true)
		for _, ev := range s.trackerEvents {
			s.finishTrackerEventLocked(ev, true)
		}
		s.trackerEvents = nil
		s.mu.Unlock()
		return
	}

	due := sched.due(time.Now(), event != "", needPeers)
	if len(due) == 0 {
		// Every tracker is backing off; keep the event for the next retry.
		s.mu.Lock()
		s.finishTrackerEventLocked(event, false)
		s.mu.Unlock()
		return
	}

	type roundResult struct {
		index int
		trackerAnnounceResult
	}
	// Buffered for every due tracker so workers never block on a consumer
	// that has returned early on shutdown.
	results := make(chan roundResult, len(due))
	runBounded(s.ctx, len(due), trackerAnnounceWorkers, func(n int) {
		defer s.crashGuard("tracker_announce")()
		if !acquireTrackerSlot(s.ctx) {
			return
		}
		r := announceTracker(s.ctx, sched.targets[due[n]], params, event, trackerAnnounceTimeout)
		releaseTrackerSlot()
		results <- roundResult{index: due[n], trackerAnnounceResult: r}
	})

	infoHashHex := ""
	if logging.Enabled() {
		infoHashHex = fmt.Sprintf("%x", params.infoHash)
	}
	seenPeers := make(map[netip.AddrPort]struct{})
	delivered := false
	var trackerErr error
	for range due {
		var r roundResult
		select {
		case r = <-results:
		case <-s.ctx.Done():
			return
		}
		sched.record(r.index, r.trackerAnnounceResult, time.Now())
		if r.err != nil {
			if logging.Enabled() {
				logging.Warn("tracker_announce_failed",
					logging.String("tracker", r.tracker),
					logging.String("event", event),
					logging.String("info_hash", infoHashHex),
					logging.Err(r.err),
				)
			}
			trackerErr = r.err
			continue
		}
		if logging.Enabled() {
			logging.Info("tracker_announce_succeeded",
				logging.String("tracker", r.tracker),
				logging.String("event", event),
				logging.String("info_hash", infoHashHex),
				logging.Int("peers", len(r.peers)),
				logging.Int("interval", r.interval),
				logging.Int("seeders", r.complete),
				logging.Int("leechers", r.incomplete),
			)
		}
		delivered = true
		s.connectTrackerPeers(freshTrackerPeers(r.peers, sched.targets[r.index].source, seenPeers))
	}

	seeders, leechers, healthy := sched.swarmTotals()
	s.mu.Lock()
	if healthy {
		s.lastTrackerErr = nil
		s.trackerSeeders = seeders
		s.trackerLeechers = leechers
	} else if trackerErr != nil {
		s.lastTrackerErr = trackerErr
	}
	s.finishTrackerEventLocked(event, delivered)
	s.mu.Unlock()
}

// finishTrackerEventLocked records the outcome of announcing event: a delivered
// event updates stoppedAnnounced; an undelivered one goes back to the head of
// the queue for the next retry.
func (s *Session) finishTrackerEventLocked(event string, delivered bool) {
	if event == "" {
		return
	}
	if !delivered {
		s.trackerEvents = append([]string{event}, s.trackerEvents...)
		if event == "stopped" {
			s.stoppedAnnounced = false
		}
		return
	}
	switch event {
	case "stopped":
		s.stoppedAnnounced = true
	case "started":
		s.stoppedAnnounced = false
	}
}

// freshTrackerPeers converts a tracker's peers to endpoints, dropping ones
// already seen this round and ones trackerPeerAllowed refuses for this tracker
// (port 0, non-unicast, and loopback or link-local addresses from a tracker
// that is not itself that local).
func freshTrackerPeers(peers []tracker.Peer, source netip.Addr, seen map[netip.AddrPort]struct{}) []netip.AddrPort {
	out := make([]netip.AddrPort, 0, len(peers))
	for _, p := range peers {
		addr, ok := netip.AddrFromSlice(p.IP)
		if !ok {
			continue
		}
		ap := netip.AddrPortFrom(addr.Unmap(), p.Port)
		if !trackerPeerAllowed(p, source) {
			continue
		}
		if _, dup := seen[ap]; dup {
			continue
		}
		seen[ap] = struct{}{}
		out = append(out, ap)
	}
	return out
}

// connectTrackerPeers starts dials to new tracker peers. The outbound semaphore in
// connectToPeer is the hard cap on concurrent connections; this loop additionally
// bounds how many new dials one tracker answer starts so a huge peer list can't
// spawn a goroutine storm. slotsHeld is snapshotted once (len() is a safe,
// lock-free read, 0 for nil test sessions) so goroutines that acquire a slot
// mid-loop are not double-counted against launched — double-counting previously
// throttled connection ramp-up under load. Peers past that bound (or all of them
// while the metadata cannot be used), and listen endpoints of peers connected to
// us (see noteListenPortLocked), are recorded undialed, with no LastAttempt, so
// maintenance dials them once that changes rather than waiting for the next
// announce, up to an hour away, to list them again.
func (s *Session) connectTrackerPeers(peers []netip.AddrPort) {
	if len(peers) == 0 {
		return
	}
	slotsHeld := len(s.outboundSlots)
	launched := 0
	for _, ap := range peers {
		// Same key form as the DHT and PEX paths ("[v6]:port" for IPv6).
		peerAddr := ap.String()
		s.mu.Lock()
		if s.closed || s.paused {
			s.mu.Unlock()
			break
		}
		now := time.Now()
		pState, exists := s.Peers[peerAddr]
		shouldDial := false
		if !exists {
			shouldDial = true
		} else {
			// A tracker response is authoritative evidence that this endpoint is
			// dialable, even if the same address was first seen as an inbound peer.
			pState.Dialable = true
			pState.markTrackerListed()
			pState.seenAt = now
			if !pState.Active && !pState.Dialing && now.Sub(pState.LastAttempt) > peerRedialBackoff {
				shouldDial = true
			}
		}
		if shouldDial && (slotsHeld+launched >= maxOutboundPeers || s.metadataStalledLocked() || s.heldByInboundLocked(peerAddr)) {
			shouldDial = false
			if !exists {
				s.prunePeersLocked()
				s.Peers[peerAddr] = &PeerState{
					IP:        ap.Addr().String(),
					Port:      ap.Port(),
					Choked:    true,
					AmChoking: true,
					Dialable:  true,
					Source:    PeerSourceTracker,
					seenAt:    now,
				}
			}
		}
		if shouldDial {
			if !exists {
				s.prunePeersLocked()
				s.Peers[peerAddr] = &PeerState{
					IP:          ap.Addr().String(),
					Port:        ap.Port(),
					Choked:      true,
					Active:      false,
					AmChoking:   true,
					LastAttempt: now,
					Dialable:    true,
					Dialing:     true,
					Source:      PeerSourceTracker,
					seenAt:      now,
				}
			} else {
				s.Peers[peerAddr].LastAttempt = now
				s.Peers[peerAddr].Dialing = true
			}
			s.wg.Add(1)
			launched++
			go func(tp tracker.Peer) {
				defer s.wg.Done()
				s.connectToPeer(tp)
			}(tracker.Peer{IP: net.IP(ap.Addr().AsSlice()), Port: ap.Port()})
		}
		s.mu.Unlock()
	}
}

// TrackerSwarmStats returns the largest seed/leecher/completed counts from the
// latest successful tracker announce or scrape cycle. The completed count
// (number of times the torrent has been downloaded to completion) is only
// populated by scrape responses.
func (s *Session) TrackerSwarmStats() (seeders, leechers, completed int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trackerSeeders, s.trackerLeechers, s.trackerCompleted
}

// scrapeTracker queries a single tracker's scrape endpoint for this torrent's
// info hash, returning the swarm-health counts. Trackers that don't advertise
// scrape (no "announce" path segment) or that fail are surfaced as errors for
// the caller to ignore best-effort.
func scrapeTracker(ctx context.Context, tr string, infoHash [20]byte, timeout time.Duration) (tracker.ScrapeStats, error) {
	scrapeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	stats, err := tracker.Scrape(scrapeCtx, tr, infoHash)
	if err != nil {
		return tracker.ScrapeStats{}, err
	}
	if st, ok := stats[infoHash]; ok {
		return st, nil
	}
	return tracker.ScrapeStats{}, fmt.Errorf("scrape response did not include requested info hash")
}

// scrapeTrackers scrapes every configured tracker; see scrapeTargets.
func (s *Session) scrapeTrackers() {
	s.mu.RLock()
	raw := s.Torrent.Trackers
	s.mu.RUnlock()
	s.scrapeTargets(sanitizeTrackers(raw))
}

// scrapeTargets queries the given trackers' scrape endpoints through the same
// bounded pool as announces and records the largest seeder/leecher/completed
// counts seen. It is best-effort: trackers that don't support scrape or that
// fail are ignored, and the stored stats are left untouched when no tracker
// responds.
func (s *Session) scrapeTargets(targets []trackerTarget) {
	if len(targets) == 0 {
		return
	}
	s.mu.RLock()
	infoHash := s.Torrent.InfoHash
	s.mu.RUnlock()

	type scrapeResult struct {
		stats tracker.ScrapeStats
		ok    bool
	}
	results := make(chan scrapeResult, len(targets))
	runBounded(s.ctx, len(targets), trackerAnnounceWorkers, func(i int) {
		defer s.crashGuard("tracker_announce")()
		if !acquireTrackerSlot(s.ctx) {
			return
		}
		st, err := scrapeTracker(s.ctx, targets[i].url, infoHash, trackerAnnounceTimeout)
		releaseTrackerSlot()
		results <- scrapeResult{stats: st, ok: err == nil}
	})

	var seeders, leechers, completed int
	any := false
	for range targets {
		var r scrapeResult
		select {
		case r = <-results:
		case <-s.ctx.Done():
			return
		}
		if !r.ok {
			continue
		}
		any = true
		seeders = max(seeders, r.stats.Complete)
		leechers = max(leechers, r.stats.Incomplete)
		completed = max(completed, r.stats.Downloaded)
	}
	if !any {
		return
	}

	s.mu.Lock()
	s.trackerSeeders = max(s.trackerSeeders, seeders)
	s.trackerLeechers = max(s.trackerLeechers, leechers)
	// completed is a cumulative tracker counter (times the torrent finished
	// downloading), so it should only ever climb; max() prevents a later cycle
	// where the highest-count tracker dropped out from regressing the display.
	s.trackerCompleted = max(s.trackerCompleted, completed)
	s.mu.Unlock()
}

func (s *Session) queueTrackerEventLocked(event string) {
	if len(s.trackerEvents) > 0 && s.trackerEvents[len(s.trackerEvents)-1] == event {
		return
	}
	s.trackerEvents = append(s.trackerEvents, event)
}

func (s *Session) announceStopped() {
	s.mu.Lock()
	if s.stoppedAnnounced {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	// Short timeout (2s) on a background context
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if s.announceWithEvent(ctx, "stopped") {
		s.mu.Lock()
		s.stoppedAnnounced = true
		s.mu.Unlock()
	}
}

// announceWithEvent sends event to every tracker in parallel (bounded like
// announces) within ctx, reporting whether any tracker accepted it. Shutdown
// and removal use it, so ctx carries a short budget.
func (s *Session) announceWithEvent(ctx context.Context, event string) bool {
	s.mu.RLock()
	raw := s.Torrent.Trackers
	params := s.trackerAnnounceParamsLocked()
	s.mu.RUnlock()

	targets := sanitizeTrackers(raw)
	if len(targets) == 0 {
		return true // No trackers configured, counts as success
	}

	var success atomic.Bool
	var wg sync.WaitGroup
	var next atomic.Int64
	for range min(trackerAnnounceWorkers, len(targets)) {
		wg.Go(func() {
			for ctx.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= len(targets) || !acquireTrackerSlot(ctx) {
					return
				}
				r := announceTracker(ctx, targets[i], params, event, trackerAnnounceTimeout)
				releaseTrackerSlot()
				if r.err == nil {
					success.Store(true)
				}
			}
		})
	}
	wg.Wait()
	return success.Load()
}

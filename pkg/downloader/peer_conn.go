package downloader

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"sainttorrent/pkg/dht"
	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/tracker"
	"sainttorrent/pkg/utp"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// peerStallTimeout bounds how long an outbound peer may hold its connection slot
// without delivering a single block of data we want. A connection's slot is held
// for the whole life of its read loop, and a peer that chokes us forever — or
// trickles only keep-alives/Have messages, or sends nothing at all — keeps its
// socket alive without ever giving us data, so without this it would occupy a
// slot indefinitely. The loop's liveness ticker runs the check for a silent peer.
// In a slow swarm those dead-weight connections accumulate until the (shared,
// manager-wide) outbound pool is full and NO session can dial a fresh peer, which
// flatlines every torrent at once until a restart clears the pools. Any single
// received block resets the timer, so a genuinely-slow-but-working peer survives;
// only one delivering < one block per peerStallTimeout (≈273 B/s at 60 s) is
// reaped, freeing the slot for a productive peer. Reaping never applies while we
// are seeding (we want no data) — see the reaper in runPeerMessageLoop.
// A var (not const) so tests can shorten it; treat it as a constant in production.
var peerStallTimeout = 60 * time.Second

// peerInactivityTimeout drops a connection, in either direction, on which neither
// side has been interested and no payload has moved for this long (libtorrent's
// inactivity_timeout): the idle drop. The stall reaper only covers outbound
// connections while we download, so without this an idle peer sending keep-alives
// could hold an inbound slot for good. A var so tests can shorten it; treat it as
// a constant.
var peerInactivityTimeout = 10 * time.Minute

// peerReadTimeout is how long a connection may go without receiving a byte before
// its read fails: the backstop for a dead socket, not the silent-peer timeout
// (the stall reaper and the inactivity drop are). BEP 3 peers send a keep-alive
// at least every two minutes, and libtorrent drops a peer after 120 s of silence,
// so 150 s leaves slack. The reader arms the deadline lazily, 1.5x this far out
// once less than this remains, so a silent peer is dropped 150-225 s after its
// last byte at a cost of one deadline update per 75 s, not one per message. A var
// so tests can shorten it; treat it as a constant.
var peerReadTimeout = 150 * time.Second

// peerKeepAliveInterval is how often a connection checks whether it wrote
// anything to the peer and sends a keep-alive if not, so the gap between two of
// our writes stays under twice this (plus one liveness tick), well within the
// 120 s after which libtorrent drops a silent peer. A write to a peer that stopped
// reading is bounded by the peer write timeout (pkg/peer). A var so tests can
// shorten it; treat it as a constant.
var peerKeepAliveInterval = 30 * time.Second

// peerIdleTickInterval is the period of a connection's liveness ticker while it
// has no piece in flight; with pieces in flight it ticks every pumpSweepInterval
// so request timeouts fire on time. Each tick runs the loop's top-of-loop checks
// (stall reaper, inactivity drop, request-timeout sweep) for a peer that sends
// nothing, and the keep-alive check. A var so tests can shorten it; treat it as a
// constant.
var peerIdleTickInterval = 30 * time.Second

// peerInterestScanInterval bounds how often an Interested from one connection may
// scan every known peer to decide whether to unchoke it at once. Between scans
// the peer is only marked interested and the next choke round (every 10 s)
// decides, so a peer flipping Interested and NotInterested cannot make us walk
// the peer map under the session write lock per 5-byte message. A var so tests
// can change it; treat it as a constant.
var peerInterestScanInterval = 10 * time.Second

// interestScanHook, when set, is called each time an Interested runs the unchoke
// scan. Tests use it to count scans.
var interestScanHook func()

// chokeTransitionHook, when set, is called each time a peer's choke state changes
// (a choke or unchoke that is not a repeat). Tests use it to count them.
var chokeTransitionHook func(choked bool)

// unsolicitedFloodSlack is how many bytes of piece data we did not ask for (or no
// longer wait for) a connection may send beyond the late-block allowance before
// it is dropped as a flood; see the MsgPiece handler.
const unsolicitedFloodSlack = 4 << 20

// peerMaintenanceInterval is how often peerMaintenanceLoop redials toward a full
// outbound connection set. New dials previously happened ONLY on a tracker
// announce (interval up to an hour) or a 30 s DHT lookup, so a slot freed by a
// dropped/reaped peer could sit idle for a long time even with known peers on
// hand. The maintenance tick refills from the known-peer set as soon as slots
// open, decoupling connection churn from the announce cadence.
var peerMaintenanceInterval = 5 * time.Second

// peerRedialBackoff is the minimum gap before peerMaintenanceLoop re-dials a known
// peer that is not currently connected, so a peer that just dropped (or that we
// just reaped) is not hammered in a tight loop.
var peerRedialBackoff = 60 * time.Second

// Large enough to avoid kernel socket buffers becoming the bottleneck on fast peers.
const peerSocketBufferSize = 4 * 1024 * 1024

// maxOutboundPeers bounds how many peers a single session dials concurrently. This is
// the download engine: it governs throughput on swarms made of many slow peers, so it
// is set generously (mainline/libtorrent use ~200 per torrent). These slots are only
// filled by peers we chose to dial, but those addresses come from trackers, DHT and
// PEX, so a hostile endpoint can still hold one: the stall reaper and the peer write
// timeout (pkg/peer) are what free it again.
const maxOutboundPeers = 200

// maxInboundPeers bounds how many incoming peer connections a session accepts at once.
// The listen port is public (announced to trackers/DHT), so this is the real abuse
// surface: the cap stops a flood of inbound connections from exhausting file descriptors.
// It is a SEPARATE budget from outbound, so an inbound flood can never starve downloads.
const maxInboundPeers = 100

// metadataRetryInterval is how often a metadata-phase connection whose peer
// offers ut_metadata checks whether a failed assembly started a new fetch round
// and, if so, re-asks the peer for the missing blocks. A var so tests can shorten
// it; treat it as a constant in production.
var metadataRetryInterval = 2 * time.Second

// metadataSizeStallTimeout is how long the ut_metadata accumulator may go without
// taking a block before a peer that advertised a different metadata_size may
// replace it. The first peer to advertise a size fixes it for everyone and peers
// advertising another size are not asked, so without this one peer advertising a
// bogus size and never answering would stall a magnet for good; honest peers,
// which all advertise the true size, deliver a block well within it. It is also
// how long an owner of a solo round may go without a block before another
// connection takes the round over, and how long a suspect host waits after a
// failed round before it may size or own the next. A var so tests can shorten
// it; treat it as a constant in production.
var metadataSizeStallTimeout = 20 * time.Second

// metadataMinBlockPeriod is the slowest average feed (one 16 KiB block per
// period, 8 KiB/s) a ut_metadata round may run at, past its first
// metadataSizeStallTimeout, before a peer advertising another size may replace
// it (see metadataFeedSlow). Without it a peer that sized the round with a bogus
// size could hold it for hours by answering one block just inside every stall
// timeout. A var so tests can change it; treat it as a constant.
var metadataMinBlockPeriod = 2 * time.Second

// metadataRejectRetryAfter is how long after a peer rejected a ut_metadata
// request (libtorrent does so while rate limiting) we ask it for that block
// again, as libtorrent waits a minute after a reject; metadataRequestTimeout is
// how long a request may go unanswered before it is asked again. Without them a
// single-source magnet whose peer dropped or refused one request stalled until
// the connection was replaced. Vars so tests can shorten them; treat them as
// constants.
var (
	metadataRejectRetryAfter = time.Minute
	metadataRequestTimeout   = 30 * time.Second
)

// metadataServeWindow and metadataServeRequestsPerBlock bound how many ut_metadata
// blocks one connection may have us serve: an honest fetcher asks for each block
// once, so twice the info dict's block count per minute leaves room for retries
// while stopping a peer from re-requesting blocks without end. Requests beyond the
// budget are rejected.
const (
	metadataServeWindow           = time.Minute
	metadataServeRequestsPerBlock = 2
)

// maxPeerDHTPortUpdates bounds how many BEP 5 PORT messages from one connection
// are fed to the DHT. An honest peer sends one; a few more allow for a port
// change, while a peer alternating ports cannot make us scan the routing table
// with every 7-byte message.
const maxPeerDHTPortUpdates = 4

// pickRetryInterval bounds how long a peer loop trusts an empty pick (see noPick
// in runPeerMessageLoop) when nothing it tracks has changed: a backstop for any
// way a piece becomes pickable that does not advance Session.pickGen.
const pickRetryInterval = 5 * time.Second

// selectNeededPiece is the peer loop's picker scan. A var so tests can count the
// scans.
var selectNeededPiece = (*Session).selectNeededPieceLocked

// addDHTNode feeds a peer-advertised DHT endpoint to the routing table. A var so
// tests can observe the calls.
var addDHTNode = (*dht.DHT).AddNode

// maxExtHandshakesPerConn bounds how many BEP 10 extension handshakes one
// connection may have decoded and acted on. Real clients send one, occasionally a
// second to update it; later ones are ignored.
const maxExtHandshakesPerConn = 4

// maxKnownPeers bounds the size of the Peers map so a tracker/DHT feeding an endless
// stream of unique addresses cannot grow it without limit. Active peers are retained;
// inactive entries are evicted oldest-first.
const maxKnownPeers = 2048

// blockRequest tracks an outstanding block request sent to a peer.
type blockRequest struct {
	pieceIndex          int64
	begin               int64
	length              int64
	requested           bool
	received            bool
	requestedAt         time.Time
	firstRequestedAt    time.Time
	retries             int
	controllerSeq       uint64
	pipelineBudgetBytes int64
}

// uploadRequest is a peer's pending block request awaiting upload bandwidth. It is
// queued (rather than served inline in the message loop) so the upload limiter is
// consulted non-blockingly and never stalls the download pump — see issue #59.
type uploadRequest struct {
	index  int64
	begin  int64
	length int64
}

type transportDialResult struct {
	transport string
	conn      net.Conn
	err       error
}

// minRetry returns the sooner of two limiter retry delays, treating 0 ("no retry
// needed") as the absence of a deadline. The peer message loop drives both a
// download request pump and an upload serve pump off a single retry timer, so it
// arms that timer for whichever pump wants to run again first.
func minRetry(a, b time.Duration) time.Duration {
	switch {
	case a <= 0:
		return b
	case b <= 0:
		return a
	case a < b:
		return a
	default:
		return b
	}
}

// laterTime returns the later of a and b.
func laterTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// prunePeersLocked evicts inactive known-peer entries when the Peers map grows past
// maxKnownPeers: first those past maxPeerFailCount, so a flood of dead addresses
// cannot push out peers that work, then the ones least recently tried or listed
// (the later of LastAttempt and seenAt), so a peer recorded undialed because
// every slot was busy is not the first to go. Active peers are never evicted.
// Caller holds s.mu.
func (s *Session) prunePeersLocked() {
	if len(s.Peers) <= maxKnownPeers {
		return
	}
	type agedPeer struct {
		addr   string
		at     time.Time
		failed bool
	}
	inactive := make([]agedPeer, 0, len(s.Peers))
	for addr, ps := range s.Peers {
		if ps.Active {
			continue
		}
		inactive = append(inactive, agedPeer{addr: addr, at: laterTime(ps.LastAttempt, ps.seenAt), failed: ps.FailCount >= maxPeerFailCount})
	}
	sort.Slice(inactive, func(i, j int) bool {
		if inactive[i].failed != inactive[j].failed {
			return inactive[i].failed
		}
		return inactive[i].at.Before(inactive[j].at)
	})
	// Evict down to ~75% of the cap so pruning isn't triggered on every insert.
	evict := len(s.Peers) - (maxKnownPeers * 3 / 4)
	for i := 0; i < evict && i < len(inactive); i++ {
		delete(s.Peers, inactive[i].addr)
	}
}

func tunePeerConn(conn net.Conn) {
	tcpConn := underlyingTCPConn(conn)
	if tcpConn == nil {
		return
	}
	_ = tcpConn.SetNoDelay(true)
	_ = tcpConn.SetReadBuffer(peerSocketBufferSize)
	_ = tcpConn.SetWriteBuffer(peerSocketBufferSize)
}

type underlyingConn interface {
	UnderlyingConn() net.Conn
}

func underlyingTCPConn(conn net.Conn) *net.TCPConn {
	for conn != nil {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			return tcpConn
		}
		wrapped, ok := conn.(underlyingConn)
		if !ok {
			return nil
		}
		conn = wrapped.UnderlyingConn()
	}
	return nil
}

// markPeerAttemptFailed records a failed dial to peerAddr. An inbound
// connection can hold the same key meanwhile: a uTP peer sends from its
// listen port, so a connection it opened while our dial to it was in flight
// is keyed like the dial. The failure then only stamps LastAttempt, leaving
// the live connection active (visible to the choker, stats and dial gating)
// and the peer's failure count alone.
func (s *Session) markPeerAttemptFailed(peerAddr string) {
	s.mu.Lock()
	if ps, ok := s.Peers[peerAddr]; ok {
		ps.LastAttempt = time.Now()
		if _, live := s.activePeers[peerAddr]; !live {
			ps.Active = false
			ps.noteDialFailed()
		}
	}
	s.mu.Unlock()
}

// broadcastHave queues a Have for piece index on every active connection. Each
// connection's own message loop sends it, batched with any other queued Haves, so
// completing a piece starts no goroutines and never waits on a peer's socket: a
// peer that reads slowly cannot pile up senders.
func (s *Session) broadcastHave(index uint32) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.ctx.Err() != nil {
		return
	}
	for _, client := range s.activePeers {
		client.QueueHave(index)
	}
}

// peerMaintenanceLoop periodically refills the outbound connection set from the
// known-peer map, so a slot freed by a dropped or reaped peer is reused promptly
// instead of waiting for the next tracker announce (up to an hour out) or DHT
// lookup. This is what keeps a slow swarm churning toward productive peers rather
// than wedging at zero once the initial connections go stale.
func (s *Session) peerMaintenanceLoop() {
	defer s.wg.Done()
	defer s.crashGuard("peer_maintenance")()
	ticker := time.NewTicker(peerMaintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.maintainPeerConnections()
		case <-s.ctx.Done():
			return
		}
	}
}

// maintainPeerConnections dials known-but-disconnected peers up to the outbound cap
// while we still need data (pieces or metadata). It mirrors the dial gating in
// announceAndConnect: it respects the per-session slot count and the per-peer redial
// backoff, and launches connectToPeer (which acquires the real per-session and
// manager-wide slots) in its own goroutine so the dial never blocks under s.mu.
func (s *Session) maintainPeerConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.paused || s.closed || !s.started {
		return
	}
	// Only maintain download connections while there is still something to fetch.
	// When seeding, inbound connections and the normal announce flow cover uploads;
	// isCompletedLocked is false in metadata mode, so metadata fetches still churn.
	// A magnet whose metadata cannot be used yet has no use for peers.
	if s.isCompletedLocked() || s.metadataStalledLocked() {
		return
	}

	slotsHeld := len(s.outboundSlots)
	// Also bound launches by the manager-wide pool's free room (a lock-free hint): if
	// the global pool is full, connectToPeer would acquire nothing and return without
	// dialing, yet the pre-set LastAttempt below would still suppress the peer for a
	// full backoff. Gating here avoids burning the backoff on dials that can't happen.
	globalRoom := maxOutboundPeers
	if s.globalOutboundSlots != nil {
		globalRoom = cap(s.globalOutboundSlots) - len(s.globalOutboundSlots)
	}

	launched := 0
	now := time.Now()
	for addr, ps := range s.Peers {
		if slotsHeld+launched >= maxOutboundPeers || launched >= globalRoom {
			break
		}
		// Skip connected peers, attempts already in flight, inbound-only source
		// endpoints whose ports were never advertised as listening ports, and
		// DHT/PEX peers of a torrent that turned out private.
		if ps.Active || ps.Dialing || !ps.Dialable || s.refusesDialLocked(addr, ps.IP) || s.privateRefusesDialLocked(ps) {
			continue
		}
		// Eligible to (re)dial once the backoff has elapsed. A zero LastAttempt means
		// "dial now" (e.g. Resume clears it on every inactive peer); the dedup against a
		// concurrent dial is the LastAttempt = now set below, under the lock.
		if !ps.LastAttempt.IsZero() && now.Sub(ps.LastAttempt) <= ps.redialBackoff() {
			continue
		}
		ip := net.ParseIP(ps.IP)
		if ip == nil || ip.IsUnspecified() || ps.Port == 0 {
			continue
		}
		ps.LastAttempt = now
		ps.Dialing = true
		launched++
		s.wg.Add(1)
		go func(tp tracker.Peer) {
			defer s.wg.Done()
			s.connectToPeer(tp)
		}(tracker.Peer{IP: ip, Port: ps.Port})
	}
}

// connectToPeer dials a peer and runs the message loop.
// P2 FIX: Uses DialContext for context-aware cancellation.
func (s *Session) connectToPeer(p tracker.Peer) {
	defer s.crashGuard("peer_dial")()
	// Keyed like addPeer, PEX, DHT and inbound connections (net.JoinHostPort), so
	// an IPv6 peer's entry is found here too.
	peerAddr := net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))
	s.mu.RLock()
	dialPauseEpoch := s.pauseEpoch
	refused := s.refusesDialLocked(peerAddr, p.IP.String()) || s.privateRefusesDialLocked(s.Peers[peerAddr])
	s.mu.RUnlock()
	acquiredSlots := false
	defer func() {
		s.mu.Lock()
		if ps, ok := s.Peers[peerAddr]; ok && ps.Dialing {
			ps.Dialing = false
			// A full per-session or manager-wide pool means no network attempt was
			// made. Keep the peer immediately eligible instead of burning a full
			// redial backoff because a lock-free capacity hint raced another session.
			// A refused address keeps its backoff.
			resumedDuringDial := s.pauseEpoch != dialPauseEpoch && !s.paused && !s.closed
			if (!acquiredSlots || resumedDuringDial) && !ps.Active && !refused {
				ps.LastAttempt = time.Time{}
			}
			s.forgetRefusedPeerLocked(peerAddr)
		}
		s.mu.Unlock()
	}()
	// Deferred after the cleanup above so a panic below reaches it first: that
	// cleanup takes s.mu, which the panicking code may still hold (see
	// crashGuard).
	defer s.crashGuard("peer_dial")()
	if refused {
		return
	}

	// Acquire an outbound slot so concurrent dials stay bounded (see maxOutboundPeers).
	// outboundSlots is nil only for sessions built outside NewSession (tests), which
	// stay unbounded. Bail without opening a socket when at capacity; the peer is
	// retried after its normal backoff.
	if s.outboundSlots != nil {
		select {
		case s.outboundSlots <- struct{}{}:
			defer func() { <-s.outboundSlots }()
		default:
			if logging.Enabled() {
				logging.Debug("peer_dial_skipped",
					logging.String("peer", peerAddr),
					logging.String("reason", "session_outbound_cap"),
				)
			}
			return
		}
	}
	// Also hold a manager-wide outbound slot so many torrents can't collectively
	// exhaust file descriptors. If the global budget is full, the per-session slot
	// above is released by its deferred receive when we return.
	if s.globalOutboundSlots != nil {
		select {
		case s.globalOutboundSlots <- struct{}{}:
			defer func() { <-s.globalOutboundSlots }()
		default:
			if logging.Enabled() {
				logging.Debug("peer_dial_skipped",
					logging.String("peer", peerAddr),
					logging.String("reason", "global_outbound_cap"),
				)
			}
			return
		}
	}
	acquiredSlots = true

	conn, transport, err := s.dialPeer(peerAddr)
	if err != nil {
		s.markPeerAttemptFailed(peerAddr)
		if logging.Enabled() {
			logging.Debug("peer_dial_failed",
				logging.String("peer", peerAddr),
				logging.Err(err),
			)
		}
		return
	}

	// Spawn context monitor before encryption negotiation so shutdown interrupts
	// both MSE handshakes and the later peer-wire message loop.
	connMonitor := &monitoredPeerConn{}
	connMonitor.set(conn)
	doneCh := make(chan struct{})
	monitorDone := make(chan struct{})
	defer func() {
		close(doneCh)
		<-monitorDone
	}()
	go func() {
		defer close(monitorDone)
		select {
		case <-s.ctx.Done():
			connMonitor.close()
		case <-doneCh:
		}
	}()

	out, step, err := s.handshakeOutgoingPeer(peerAddr, conn, transport, connMonitor, s.dialPeer)
	if err != nil && out.transport == "utp" && isTimeoutErr(err) && s.ctx.Err() == nil {
		// The uTP connection came up but the handshake over it timed out: most
		// often a peer running an older saintTorrent, whose uTP numbering
		// stalls against ours, or a path that passes uTP's SYN but not its
		// data. Retry once, straight away and over TCP only, in the slots
		// already held, instead of charging the peer a failed attempt.
		if tcpConn, _, tcpErr := s.dialPeerTCP(peerAddr); tcpErr != nil {
			err = errors.Join(err, tcpErr)
		} else {
			out, step, err = s.handshakeOutgoingPeer(peerAddr, tcpConn, "tcp", connMonitor, s.dialPeerTCP)
		}
	}
	if err != nil {
		s.markPeerAttemptFailed(peerAddr)
		if logging.Enabled() {
			event := "peer_handshake_failed"
			if step == "negotiation" {
				event = "peer_negotiation_failed"
			}
			logging.Debug(event,
				logging.String("peer", peerAddr),
				logging.String("transport", out.transport),
				logging.Err(err),
			)
		}
		return
	}
	conn = out.conn
	defer conn.Close()
	client, handshake := out.client, out.handshake

	if handshake.InfoHash != s.Torrent.InfoHash {
		s.markPeerAttemptFailed(peerAddr)
		if logging.Enabled() {
			logging.Warn("peer_handshake_rejected",
				logging.String("peer", peerAddr),
				logging.String("reason", "info_hash_mismatch"),
			)
		}
		return
	}

	s.mu.Lock()
	if ps, ok := s.Peers[peerAddr]; ok {
		ps.LastAttempt = time.Now()
		ps.Dialable = true
		ps.Dialing = false
		ps.FailCount = 0
	}
	s.mu.Unlock()

	s.runPeerMessageLoop(client, conn, peerAddr, p.IP.String(), p.Port, handshake.Reserved, true)
}

// outgoingPeer is an outbound connection that completed its handshake.
type outgoingPeer struct {
	conn      net.Conn
	transport string // "tcp" or "utp"; set on failure too
	client    *peer.Client
	handshake *peer.Handshake
}

// handshakeOutgoingPeer runs the encryption negotiation (per the session's
// policy) and the BitTorrent handshake on conn, a fresh connection to peerAddr
// over transport, each bounded by peerDialHandshakeTimeout. redial dials the
// peer again for a plaintext fallback after a failed MSE handshake. On failure
// the connection is closed, and step names the step that failed
// ("negotiation" or "handshake").
func (s *Session) handshakeOutgoingPeer(peerAddr string, conn net.Conn, transport string, monitor *monitoredPeerConn, redial func(string) (net.Conn, string, error)) (out outgoingPeer, step string, err error) {
	tunePeerConn(conn)
	monitor.set(conn)
	_ = conn.SetDeadline(time.Now().Add(peerDialHandshakeTimeout))
	conn, transport, err = s.negotiateOutgoingPeerConn(peerAddr, conn, transport, monitor, redial)
	if err != nil {
		return outgoingPeer{transport: transport}, "negotiation", err
	}
	monitor.set(conn)

	_ = conn.SetDeadline(time.Now().Add(peerDialHandshakeTimeout))
	client := peer.NewClient(conn, s.Torrent.InfoHash, s.PeerID)
	s.mu.RLock()
	client.DisableDHT = !s.allowsDecentralizedPeerDiscoveryLocked()
	s.mu.RUnlock()
	handshake, err := client.Handshake()
	if err != nil {
		_ = conn.Close()
		return outgoingPeer{transport: transport}, "handshake", err
	}
	_ = conn.SetDeadline(time.Time{}) // clear deadline
	return outgoingPeer{conn: conn, transport: transport, client: client, handshake: handshake}, "", nil
}

// isTimeoutErr reports whether err is, or wraps, an I/O timeout.
func isTimeoutErr(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// peerDialTimeout bounds one dial attempt, whatever the transport.
const peerDialTimeout = 5 * time.Second

// peerDialHandshakeTimeout bounds each of an outbound connection's encryption
// negotiation and BitTorrent handshake. A var so tests can shorten it; treat it
// as a constant.
var peerDialHandshakeTimeout = peerHandshakeTimeout

// dialTCPGraceMin and dialTCPGraceMax bound how long a uTP connection that came
// up first waits for the TCP dial to the same peer (see pickDialResult). Vars
// so tests can change them; treat them as constants.
var (
	dialTCPGraceMin = 250 * time.Millisecond
	dialTCPGraceMax = time.Second
)

// peerTCPDial dials addr over TCP. A var so tests can slow or fail it.
var peerTCPDial = func(ctx context.Context, addr string) (net.Conn, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, "tcp", addr)
}

// dialPeer connects to peerAddr and reports the transport it used. TCP is
// preferred: our uTP has no congestion control or fast retransmit yet, so it
// recovers from loss far more slowly, and it stalls against older
// saintTorrent peers. When uTP is available both are dialed at once, so a
// firewalled TCP path does not hold the bounded outbound slot for a whole dial
// timeout before uTP gets a chance; pickDialResult then keeps uTP only if TCP
// fails or does not connect soon after it.
func (s *Session) dialPeer(peerAddr string) (net.Conn, string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, peerDialTimeout)
	defer cancel()

	s.mu.RLock()
	udpSocket := s.utpSocket
	s.mu.RUnlock()

	if udpSocket == nil {
		conn, err := peerTCPDial(ctx, peerAddr)
		if err != nil {
			return nil, "", err
		}
		return conn, "tcp", nil
	}

	dialStart := time.Now()
	tcpCtx, cancelTCP := context.WithCancel(ctx)
	defer cancelTCP()
	utpCtx, cancelUTP := context.WithCancel(ctx)
	defer cancelUTP()
	results := make(chan transportDialResult, 2)
	go func() {
		defer s.crashGuard("peer_dial_transport")()
		conn, err := peerTCPDial(tcpCtx, peerAddr)
		results <- transportDialResult{transport: "tcp", conn: conn, err: err}
	}()
	go func() {
		defer s.crashGuard("peer_dial_transport")()
		conn, err := udpSocket.DialContext(utpCtx, peerAddr)
		results <- transportDialResult{transport: "utp", conn: conn, err: err}
	}()
	return pickDialResult(results, 2, dialStart, cancelTCP, cancelUTP)
}

// dialPeerTCP connects to peerAddr over TCP only.
func (s *Session) dialPeerTCP(peerAddr string) (net.Conn, string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, peerDialTimeout)
	defer cancel()
	conn, err := peerTCPDial(ctx, peerAddr)
	if err != nil {
		return nil, "", fmt.Errorf("tcp dial failed: %w", err)
	}
	return conn, "tcp", nil
}

// pickDialResult picks the connection dialPeer uses from the pending results of
// its TCP and uTP dials, started at dialStart. A TCP connection wins at once
// and cancels the uTP dial. A uTP connection that comes first waits up to
// dialTCPGrace for TCP: TCP still wins if it connects within that, and uTP is
// used if TCP fails or the grace runs out, which cancels the TCP dial. A
// failure waits for the other transport. Every connection that loses is
// closed, including ones that complete after the pick.
func pickDialResult(results <-chan transportDialResult, pending int, dialStart time.Time, cancelTCP, cancelUTP func()) (net.Conn, string, error) {
	var errs []error
	var utpConn net.Conn
	var grace *time.Timer
	var graceC <-chan time.Time
	defer func() {
		if grace != nil {
			grace.Stop()
		}
	}()
	for pending > 0 {
		var res transportDialResult
		select {
		case res = <-results:
			pending--
		case <-graceC:
			cancelTCP()
			go closeLateDialSuccesses(results, pending)
			return utpConn, "utp", nil
		}
		switch {
		case res.err != nil:
			errs = append(errs, fmt.Errorf("%s dial failed: %w", res.transport, res.err))
			if utpConn != nil {
				return utpConn, "utp", nil // TCP failed within the grace
			}
		case res.transport == "tcp":
			cancelUTP()
			if utpConn != nil {
				_ = utpConn.Close()
			}
			if pending > 0 {
				go closeLateDialSuccesses(results, pending)
			}
			return res.conn, "tcp", nil
		case pending == 0:
			return res.conn, "utp", nil // TCP already failed
		default:
			utpConn = res.conn
			grace = time.NewTimer(dialTCPGrace(time.Since(dialStart)))
			graceC = grace.C
		}
	}
	return nil, "", errors.Join(errs...)
}

// dialTCPGrace is how long a uTP connection that took utpConnect to come up
// waits for the TCP dial: twice that (TCP's handshake needs about the same
// round trip), within [dialTCPGraceMin, dialTCPGraceMax].
func dialTCPGrace(utpConnect time.Duration) time.Duration {
	return min(max(2*utpConnect, dialTCPGraceMin), dialTCPGraceMax)
}

func closeLateDialSuccesses(results <-chan transportDialResult, remaining int) {
	for i := 0; i < remaining; i++ {
		res := <-results
		if res.err == nil && res.conn != nil {
			_ = res.conn.Close()
		}
	}
}

// inboundListenerLoop accepts incoming peer connections on the already-bound listener.
//
// Only a standalone session (tests, or a Session used without a
// TorrentManager) owns a listener. The CLI and TUI serve every torrent from the
// manager's shared listener, which reads the handshake under a bounded
// pre-handshake budget (inboundHandshakeSlots, with a per-source share) before
// any session slot is taken; see TorrentManager.handleRoutedIncomingConnection.
// This path has no such budget: handleIncomingConnection takes the session's
// and the global inbound slots before the handshake is read, so connections
// that never send one can fill them for up to peerHandshakeTimeout.
func (s *Session) inboundListenerLoop() {
	defer s.wg.Done()
	defer s.crashGuard("inbound_listener")()

	s.mu.RLock()
	listener := s.listener
	s.mu.RUnlock()

	if listener == nil {
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		s.mu.Lock()
		if s.closed {
			conn.Close()
			s.mu.Unlock()
			return
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleIncomingConnection(c)
		}(conn)
		s.mu.Unlock()
	}
}

// handleIncomingConnection serves a connection from a standalone session's own
// listener (see inboundListenerLoop). It holds an inbound slot from before the
// handshake is read, unlike the manager's shared listener, which the CLI uses
// and which budgets pre-handshake connections separately.
func (s *Session) handleIncomingConnection(conn net.Conn) {
	defer s.crashGuard("peer_inbound")()
	defer conn.Close()

	// Bound concurrent inbound connections (see maxInboundPeers); drop new ones once
	// we're at capacity. This is a separate budget from outbound dials, so an inbound
	// flood can never starve our own downloads.
	if s.inboundSlots != nil {
		select {
		case s.inboundSlots <- struct{}{}:
			defer func() { <-s.inboundSlots }()
		default:
			return
		}
	}
	// Also hold a manager-wide inbound slot (released by the per-session deferred
	// receive if the global budget is full).
	if s.globalInboundSlots != nil {
		select {
		case s.globalInboundSlots <- struct{}{}:
			defer func() { <-s.globalInboundSlots }()
		default:
			return
		}
	}

	s.serveIncomingConnection(conn, nil)
}

// handleRoutedIncomingConnection serves a connection whose handshake was parsed
// by the manager's shared listener. The manager already holds the global inbound
// slot, so only the per-session budget is acquired here.
func (s *Session) handleRoutedIncomingConnection(conn net.Conn, handshake *peer.Handshake) {
	defer s.crashGuard("peer_inbound")()
	if s.inboundSlots != nil {
		select {
		case s.inboundSlots <- struct{}{}:
			defer func() { <-s.inboundSlots }()
		default:
			return
		}
	}
	s.serveIncomingConnection(conn, handshake)
}

func (s *Session) serveIncomingConnection(conn net.Conn, handshake *peer.Handshake) {
	defer s.crashGuard("peer_inbound")()
	tunePeerConn(conn)

	s.mu.RLock()
	paused := s.paused
	closed := s.closed
	banned := s.refusesIncomingLocked(conn.RemoteAddr())
	stalled := s.metadataStalledLocked()
	s.mu.RUnlock()
	if paused || closed || banned || stalled {
		return
	}

	// Spawn context monitor to interrupt immediately on shutdown
	connMonitor := &monitoredPeerConn{}
	connMonitor.set(conn)
	doneCh := make(chan struct{})
	monitorDone := make(chan struct{})
	defer func() {
		close(doneCh)
		<-monitorDone
	}()
	go func() {
		defer close(monitorDone)
		select {
		case <-s.ctx.Done():
			connMonitor.close()
		case <-doneCh:
		}
	}()

	if handshake == nil {
		_ = conn.SetDeadline(time.Now().Add(peerHandshakeTimeout))
		var err error
		conn, handshake, err = s.parseIncomingHandshake(conn)
		if err != nil {
			return
		}
		connMonitor.set(conn)
	}

	if handshake.InfoHash != s.Torrent.InfoHash {
		return
	}

	s.mu.RLock()
	allowDHT := s.allowsDecentralizedPeerDiscoveryLocked()
	s.mu.RUnlock()

	respHs := &peer.Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: s.Torrent.InfoHash,
		PeerID:   s.PeerID,
	}
	respHs.Reserved[5] = 0x10 // Support extension protocol (BEP 10)
	if allowDHT {
		respHs.Reserved[7] |= 0x01 // Support DHT (BEP 5)
	}
	peer.EnableFastExtension(&respHs.Reserved)
	_ = conn.SetDeadline(time.Now().Add(peerHandshakeTimeout))
	if _, err := conn.Write(respHs.Serialize()); err != nil {
		return
	}

	_ = conn.SetDeadline(time.Time{})

	client := peer.NewClient(conn, s.Torrent.InfoHash, s.PeerID)
	client.RemotePeerID = handshake.PeerID
	peerAddr := conn.RemoteAddr().String()
	host, portStr, err := net.SplitHostPort(peerAddr)
	if err != nil {
		return
	}
	var portVal int
	_, _ = fmt.Sscanf(portStr, "%d", &portVal)
	if portVal <= 0 || portVal > 65535 {
		return
	}

	s.runPeerMessageLoop(client, conn, peerAddr, host, uint16(portVal), handshake.Reserved, false)
}

func (s *Session) runPeerMessageLoop(client *peer.Client, conn net.Conn, peerAddr string, ip string, port uint16, peerReserved [8]byte, outbound bool) {
	defer s.crashGuard("peer_loop")()
	fastEnabled := peer.SupportsFastExtension(peerReserved)
	direction := "inbound"
	if outbound {
		direction = "outbound"
	}

	hostKey, loopback := peerHostKey(ip)
	remoteID := client.RemotePeerID
	// A piece from this connection that fails its hash check is charged to its
	// host; loopback peers are exempt, as from the per-host cap.
	source := &pieceSource{host: hostKey}
	if loopback {
		source.host = ""
	}
	s.mu.Lock()
	if s.paused || s.closed {
		s.mu.Unlock()
		return
	}
	// A dial started before metadata showed the torrent is private is refused
	// here, under the same lock that registers the connection, so none slips
	// past purgeDiscoveryPeersLocked.
	var reason string
	if s.metadataStalledLocked() {
		reason = "metadata_error"
	} else if outbound && s.privateRefusesDialLocked(s.Peers[peerAddr]) {
		reason = "private_discovery_peer"
		s.forgetRefusedPeerLocked(peerAddr)
	} else {
		reason = s.admitPeerLocked(peerAddr, hostKey, loopback, remoteID, outbound)
	}
	if reason != "" {
		switch reason {
		case "self_connection":
			if ps, ok := s.Peers[peerAddr]; ok {
				ps.Dialable = false
			}
		case "per_ip_limit", "duplicate_peer_id":
			// The dial worked but the connection cannot be used. connectToPeer
			// cleared the failure count once the handshake succeeded, so count the
			// refusal here, or the address would be redialled (TCP, encryption and
			// handshake) every peerRedialBackoff for as long as the refusal holds.
			if ps, ok := s.Peers[peerAddr]; ok && outbound {
				ps.noteDialFailed()
				ps.LastAttempt = time.Now()
			}
		}
		s.mu.Unlock()
		if logging.Enabled() {
			logging.Debug("peer_rejected",
				logging.String("peer", peerAddr),
				logging.String("direction", direction),
				logging.String("reason", reason),
			)
		}
		return
	}
	connectionPauseEpoch := s.pauseEpoch
	logEnabled := logging.Enabled()
	var logInfoHash, logName string
	pState, ok := s.Peers[peerAddr]
	if !ok {
		pState = &PeerState{
			IP:          ip,
			Port:        port,
			Choked:      true,
			Active:      false,
			AmChoking:   true,
			LastAttempt: time.Now(),
		}
		s.Peers[peerAddr] = pState
		// Inserting into the map is the only reliable eviction trigger available on
		// the inbound path: reconnect churn keys on an ephemeral source port, so it
		// never hits the discovery-path !exists branches (addPeer, tracker dials)
		// that otherwise drive prunePeersLocked, and a seeding/private session can
		// get no other prunes at all.
		s.prunePeersLocked()
	}
	// An outbound connection confirms this is a listening endpoint. An inbound
	// connection does not erase prior tracker/DHT evidence for the same endpoint.
	if outbound {
		pState.Dialable = true
	} else {
		pState.Source |= PeerSourceIncoming
	}
	pState.Active = true
	// Choke and interest start over on every connection (BEP 3); an entry kept
	// from an earlier connection to this address still holds that one's state.
	pState.AmChoking = true
	pState.Choked = true
	pState.Interested = false
	s.activePeers[peerAddr] = client
	if logEnabled {
		logInfoHash, logName = s.logIdentityLocked()
	}
	registeredPieces := len(s.PieceStates)
	s.mu.Unlock()
	// Size the largest bitfield the reader accepts to this torrent before the
	// reader starts. Before metadata the client's default (the largest bitfield a
	// magnet can need) applies until the piece count is known.
	if registeredPieces > 0 {
		client.SetBitfieldLimit(registeredPieces)
	}
	if logEnabled {
		logging.Info("peer_connected",
			logging.String("info_hash", logInfoHash),
			logging.String("name", logName),
			logging.String("peer", peerAddr),
			logging.String("direction", direction),
			logging.Bool("fast_extension", fastEnabled),
		)
	}

	disconnectReason := "ended"
	var disconnectErr error
	defer func() {
		s.mu.Lock()
		reconnectAfterResume := false
		if s.metadataOwner == client {
			s.metadataOwner = nil // the next connection to ask takes the solo round over
		}
		if activeClient, active := s.activePeers[peerAddr]; active && activeClient == client {
			s.releasePeerLocked(peerAddr, hostKey, remoteID)
			if ps, ok := s.Peers[peerAddr]; ok {
				ps.Active = false
				ps.Choked = true
				if s.pauseEpoch != connectionPauseEpoch && !s.paused && !s.closed {
					ps.LastAttempt = time.Time{}
					reconnectAfterResume = true
				} else {
					ps.LastAttempt = time.Now()
				}
				// Inbound-only entries (never confirmed dialable by tracker/DHT
				// discovery or a successful outbound dial) are keyed by an ephemeral
				// source port: they're worthless as redial candidates, so retaining
				// them past disconnect only feeds unbounded growth from reconnect
				// churn. Drop them outright rather than leaving them inactive forever.
				// So are DHT/PEX peers of a torrent that turned out private.
				if s.privateRefusesDialLocked(ps) {
					reconnectAfterResume = false
					delete(s.Peers, peerAddr)
				} else if !ps.Dialable && !reconnectAfterResume {
					delete(s.Peers, peerAddr)
				}
			}
			delete(s.activePeers, peerAddr)
		}
		s.mu.Unlock()

		if reconnectAfterResume {
			// Re-dial an already-known peer rather than perform decentralized
			// discovery, so private torrents (which use trackers only) can still
			// re-establish dropped connections after a resume.
			s.addPeer(peerAddr, false)
		}
		if logging.Enabled() {
			fields := []logging.Field{
				logging.String("info_hash", logInfoHash),
				logging.String("name", logName),
				logging.String("peer", peerAddr),
				logging.String("direction", direction),
				logging.String("reason", disconnectReason),
			}
			if disconnectErr != nil {
				fields = append(fields, logging.Err(disconnectErr))
			}
			logging.Info("peer_disconnected", fields...)
		}
	}()
	// The disconnect handler above takes s.mu: a guard deferred after it
	// handles a panic below before that handler could block on a lock the
	// panicking code still holds (see crashGuard).
	defer s.crashGuard("peer_loop")()

	s.mu.RLock()
	inMeta := s.metadataMode
	numPieces := len(s.PieceStates)
	s.mu.RUnlock()

	var initializedPeersAndBitfield bool = false
	allowedFastForPeer := make(map[int64]struct{})
	peerAllowedFast := make(map[int64]struct{})
	peerRejectedPieces := make(map[int64]struct{})

	// localAllowedFast is this peer's deterministic allowed-fast set (BEP 6),
	// computed once after the piece count is known; allowedFastFullyAdvertised
	// short-circuits the re-check once every index in it has been offered.
	var localAllowedFast []int
	allowedFastFullyAdvertised := false
	// allowedFastServed counts blocks of each allowed-fast piece queued for this
	// peer while we choke it (keys are limited to allowedFastForPeer).
	allowedFastServed := make(map[int64]int64)
	// Fast messages that arrive before we have metadata reference piece indices we
	// cannot validate yet; remember them and replay once the piece count is known
	// (a seed sends have_all exactly once, right after the handshake — well before
	// a magnet transfer has finished fetching metadata).
	peerHaveAllPending := false
	// pendingAllowedFast buffers allowed_fast offers that arrive before metadata,
	// deduped and capped at pendingAllowedFastCap. The cap sits far above any real
	// client's allowed-fast set so legitimate offers all replay once metadata lands
	// (the post-metadata path applies the same cap to peerAllowedFast),
	// while still preventing a peer from growing our memory at wire rate by spamming
	// distinct indices we cannot yet validate. The map is sized for the common case
	// (~allowedFastSetSize offers); it grows on its own if a peer sends more.
	var pendingAllowedFast []int64
	pendingAllowedFastSet := make(map[int64]struct{}, allowedFastSetSize)

	// maybeAdvertiseAllowedFast offers (once each) the allowed-fast pieces we have
	// completed. It is re-run as we complete more pieces so a client that finishes
	// an allowed-fast piece after connecting still grants it, and stops scanning
	// once the whole set has been advertised. SendAllowedFast is issued outside the
	// lock so a blocked socket write never stalls s.mu.
	maybeAdvertiseAllowedFast := func() {
		if !fastEnabled || allowedFastFullyAdvertised || len(localAllowedFast) == 0 {
			return
		}
		var toSend []int
		s.mu.RLock()
		n := len(s.PieceStates)
		for _, idx := range localAllowedFast {
			if idx < 0 || idx >= n || s.PieceStates[idx] != PieceCompleted {
				continue
			}
			if _, sent := allowedFastForPeer[int64(idx)]; !sent {
				toSend = append(toSend, idx)
			}
		}
		s.mu.RUnlock()
		for _, idx := range toSend {
			allowedFastForPeer[int64(idx)] = struct{}{}
			_ = client.SendAllowedFast(uint32(idx))
		}
		if len(allowedFastForPeer) >= len(localAllowedFast) {
			allowedFastFullyAdvertised = true
		}
	}

	sendInitialPeerState := func() {
		s.mu.RLock()
		numPieces := len(s.PieceStates)
		bf, hasAny, hasAll := completedPieceBitfield(s.PieceStates)
		isComplete := s.isCompletedLocked()
		s.mu.RUnlock()

		if fastEnabled && localAllowedFast == nil && numPieces > 0 {
			localAllowedFast = allowedFastSet(s.Torrent.InfoHash, ip, numPieces, allowedFastSetSize)
			if localAllowedFast == nil {
				localAllowedFast = []int{} // mark computed (e.g. an IPv6 peer has no set)
			}
		}

		switch {
		case fastEnabled && hasAll:
			_ = client.SendHaveAll()
		case fastEnabled && !hasAny:
			_ = client.SendHaveNone()
		case hasAny:
			_ = client.SendBitfield(bf)
		}
		if isComplete {
			_ = client.SendNotInterested()
		} else {
			_ = client.SendInterested()
		}
		maybeAdvertiseAllowedFast()
	}

	if !inMeta {
		sendInitialPeerState()
		initializedPeersAndBitfield = true
	}

	// Send extension handshake if peer supports extensions (BEP 10)
	if peerReserved[5]&0x10 != 0 {
		s.mu.RLock()
		extensions := s.extensionHandshakeMapLocked()
		infoLen := 0
		if _, ok := extensions[peer.ExtNameMetadata]; ok {
			infoLen = len(s.Torrent.InfoBytes)
		}
		s.mu.RUnlock()
		// reqq tells the peer how many requests we queue (maxUploadQueue), so it
		// does not pipeline requests we would have to reject.
		_ = client.SendExtensionHandshake(&peer.ExtensionHandshake{
			Extensions:   extensions,
			MetadataSize: infoLen,
			RequestQueue: maxUploadQueue,
		})
	}

	// Advertise our DHT UDP port to DHT-capable peers (BEP 5 PORT message). This
	// lets live peers add us to their routing tables and is a counterpart to
	// ingesting their PORT messages below.
	if peerReserved[7]&0x01 != 0 {
		s.mu.RLock()
		d := s.DHT
		allowDHT := s.allowsDecentralizedPeerDiscoveryLocked()
		s.mu.RUnlock()
		if allowDHT && d != nil {
			if dhtPort := d.Port(); dhtPort != 0 {
				_ = client.SendPort(dhtPort)
			}
		}
	}

	// Read peer wire loop
	var peerBitfield []byte
	if !inMeta {
		peerBitfield = make([]byte, (numPieces+7)/8)
	}
	// peerIsSeed marks a peer that announced every piece (have_all or a full
	// bitfield). Seeds are left out of pieceAvailability: a seed raises every
	// piece's count by one, which never changes rarest-first order, so skipping it
	// keeps a seed's connect and disconnect O(1) under s.mu instead of O(pieces).
	peerIsSeed := false
	// Drop this peer's contribution to swarm piece availability on exit. peerBitfield
	// accumulates exactly the pieces we counted (bitfield delta + Haves), so the
	// closure reads its final value here. (#7, rarest-first.)
	defer func() {
		if !peerIsSeed {
			s.removePeerAvailability(peerBitfield)
		}
	}()
	defer s.crashGuard("peer_loop")() // after the s.mu-taking cleanup above
	// availabilityReceived is set by the peer's first bitfield, have_all or
	// have_none. BEP 3/6 allow exactly one of them, right after the handshake, and
	// each one rewrites the peer's whole contribution to availability under s.mu,
	// so later ones are ignored: a peer flipping have_all/have_none would otherwise
	// hold the session write lock for O(pieces) per 5-byte message.
	availabilityReceived := false
	// pendingBitfield buffers a bitfield that arrives before metadata, when its
	// length cannot be checked yet; it is replayed once the piece count is known,
	// like peerHaveAllPending.
	var pendingBitfield []byte

	// A peer downloads several pieces at once (activeDownloads, filled in slice
	// order so earlier pieces complete first). The dynamic pipeline window spans
	// all of them instead of being capped by a single piece's block count.
	type activeDownload struct {
		pieceIndex     int64
		hash           [20]byte
		length         int64
		numBlocks      int64
		blocks         [][]byte                // received block data, nil until received
		blockMsgs      []*peer.Message         // pooled wire buffers backing blocks; released after assembly
		pending        map[int64]*blockRequest // begin offset -> request
		blocksReceived int64
		nextBlock      int64   // index of the next never-requested block (cursor)
		retry          []int64 // begin offsets of timed-out requests awaiting re-send
		// endgame is set when this is a redundant copy of a piece another peer already
		// holds open (#8). The piece's PieceDownloading state is owned by that other
		// peer, so this copy never returns the piece to the pool on release.
		endgame bool
	}
	var activeDownloads []*activeDownload
	pipeline := newPeerPipelineController(defaultPeerPipelineConfig())
	// noPick records that the last pick for this peer found nothing it can serve,
	// at session pickGen noPickGen and time noPickAt. pump then skips the picker,
	// whose scan of the needed pieces under the session write lock costs O(needed)
	// when the peer has none of them, until pickGen moves, the peer's side changes
	// (a new piece, an allowed_fast grant, one of our pieces closing, or an unchoke
	// when noPickRestricted; each clears noPick), or pickRetryInterval passes as a
	// backstop. Without it a peer that unchoked us but has nothing we need paid
	// that scan for every message it sent, keep-alives and its own block requests
	// included. noPickRestricted marks an empty pick made while the peer choked us
	// (allowed-fast pieces only) or with pieces it rejected set aside: an unchoke
	// widens those, so only then does it clear noPick, and a peer flipping choke
	// and unchoke cannot force a scan per flip.
	noPick := false
	noPickRestricted := false
	var noPickGen uint64
	var noPickAt time.Time

	type requestFinishReason int
	const (
		requestFinishAccepted requestFinishReason = iota
		requestFinishTimeout
		requestFinishCancel
		requestFinishAbandon
	)

	// Unsolicited-data accounting. A peer that has pieces we want is never
	// inactive, and an inbound one is never stall-reaped, so without these it
	// could stream piece messages we never asked for at wire speed for good.
	// usefulBytes counts the block bytes we accepted; lateAllowance the bytes of
	// requests we stopped waiting for (timed out, cancelled, dropped on a choke),
	// which an honest peer may still deliver; unsolicitedBytes every block we
	// discarded. The peer is dropped once the discarded bytes pass both the
	// allowance plus unsolicitedFloodSlack and the useful bytes.
	var unsolicitedBytes, usefulBytes, lateAllowance int64

	findDownload := func(index int64) *activeDownload {
		for _, dl := range activeDownloads {
			if dl.pieceIndex == index {
				return dl
			}
		}
		return nil
	}
	// releaseDownloadBuffers returns any pooled wire buffers still held by an
	// abandoned (never-assembled) download to the inbound pool. The completion path
	// releases and nils them itself before assembly, so this only reclaims buffers
	// from pieces dropped on choke, reject, endgame, or disconnect.
	releaseDownloadBuffers := func(dl *activeDownload) {
		for i, m := range dl.blockMsgs {
			if m != nil {
				m.Release()
				dl.blockMsgs[i] = nil
			}
		}
	}
	removeDownload := func(index int64) {
		noPick = false // a freed piece slot or endgame copy may allow a new pick
		for i, dl := range activeDownloads {
			if dl.pieceIndex == index {
				releaseDownloadBuffers(dl)
				n := len(activeDownloads)
				activeDownloads = append(activeDownloads[:i], activeDownloads[i+1:]...)
				// Nil the now-vacated tail slot so the backing array drops its
				// reference to the removed *activeDownload (and the block buffers
				// it may still hold), instead of pinning it until overwritten by a
				// future append (#63). Index through the full-length reslice since
				// n-1 is beyond activeDownloads' new (shrunk) length but still
				// within its capacity.
				activeDownloads[:n][n-1] = nil
				return
			}
		}
	}
	lastPipelineSnapshot := time.Time{}
	publishPipelineSnapshot := func(now time.Time, force bool) {
		if !force && !lastPipelineSnapshot.IsZero() && now.Sub(lastPipelineSnapshot) < 250*time.Millisecond {
			return
		}
		snap := pipeline.Snapshot(now)
		s.mu.Lock()
		pState.WindowBlocks = snap.WindowBlocks
		pState.TargetWindowBlocks = snap.TargetWindowBlocks
		pState.OutstandingBlocks = snap.OutstandingBlocks
		pState.OutstandingBytes = snap.OutstandingBytes
		pState.PipelineQueueSeconds = snap.QueueSeconds
		pState.PipelineRTT = snap.RTT
		pState.PipelineRate = snap.Rate
		pState.TimeoutRate = snap.TimeoutRate
		pState.AppLimited = snap.AppLimited
		pState.BudgetLimited = snap.BudgetLimited
		pState.PieceCapLimited = snap.PieceCapLimited
		pState.WriterLimited = snap.WriterLimited
		s.mu.Unlock()
		lastPipelineSnapshot = now
	}
	releasePipelineBudget := func(req *blockRequest) {
		if req == nil || req.pipelineBudgetBytes <= 0 {
			return
		}
		s.pipelineBudget.release(req.pipelineBudgetBytes)
		req.pipelineBudgetBytes = 0
	}
	// finishRequest ends our wait for an outstanding request. It is the only place
	// a requested, not yet received block stops being outstanding: every path that
	// gives one up (a timeout, a cancel, a reject, a choke, another peer finishing
	// the piece) goes through it, which is what keeps lateAllowance complete.
	finishRequest := func(req *blockRequest, reason requestFinishReason, now time.Time) {
		if req == nil || !req.requested || req.received {
			return
		}
		releasePipelineBudget(req)
		switch reason {
		case requestFinishAccepted:
			pipeline.OnBlockAccepted(req, req.length, now)
		case requestFinishTimeout:
			pipeline.OnRequestTimeout(req, now)
			lateAllowance += req.length
		case requestFinishCancel, requestFinishAbandon:
			pipeline.OnCancel(req, now)
			lateAllowance += req.length
		}
		req.requested = false
	}
	releasePipelineReservations := func(dls []*activeDownload, now time.Time) {
		for _, dl := range dls {
			for _, req := range dl.pending {
				finishRequest(req, requestFinishAbandon, now)
			}
		}
	}
	// releaseDownloads returns still-in-progress pieces to PieceEmpty so other
	// peers can re-pick them (used on choke and on disconnect).
	releaseDownloads := func(dls []*activeDownload) {
		if len(dls) == 0 {
			return
		}
		s.mu.Lock()
		for _, dl := range dls {
			if dl.endgame {
				continue // redundant endgame copy; the owning peer holds the piece state
			}
			if dl.pieceIndex >= 0 && dl.pieceIndex < int64(len(s.PieceStates)) &&
				s.PieceStates[dl.pieceIndex] == PieceDownloading {
				s.setPieceStateLocked(int(dl.pieceIndex), PieceEmpty)
			}
		}
		s.mu.Unlock()
		// Reclaim pooled wire buffers outside the lock — none of this touches
		// s.mu-guarded state, and Put must stay off the critical section.
		for _, dl := range dls {
			releaseDownloadBuffers(dl)
		}
	}
	abandonRejectedDownload := func(dl *activeDownload, rejectedBegin int64, now time.Time) {
		if dl == nil {
			return
		}
		peerRejectedPieces[dl.pieceIndex] = struct{}{}
		for begin, req := range dl.pending {
			if req.requested && !req.received {
				if begin != rejectedBegin {
					_ = client.SendCancel(uint32(dl.pieceIndex), uint32(begin), uint32(req.length))
				}
				finishRequest(req, requestFinishCancel, now)
			}
		}
		if !dl.endgame {
			s.mu.Lock()
			if dl.pieceIndex >= 0 && dl.pieceIndex < int64(len(s.PieceStates)) &&
				s.PieceStates[dl.pieceIndex] == PieceDownloading {
				s.setPieceStateLocked(int(dl.pieceIndex), PieceEmpty)
			}
			s.mu.Unlock()
		}
		removeDownload(dl.pieceIndex)
		publishPipelineSnapshot(now, true)
	}
	isFastMessage := func(id peer.MessageID) bool {
		switch id {
		case peer.MsgSuggestPiece, peer.MsgHaveAll, peer.MsgHaveNone, peer.MsgRejectRequest, peer.MsgAllowedFast:
			return true
		default:
			return false
		}
	}

	var peerUtMetadataID int = -1
	var peerUtPexID int = -1
	extHandshakes := 0
	// The DHT port this peer last advertised with PORT (BEP 5) that we acted on.
	var peerDHTPort uint16
	peerDHTPortUpdates := 0
	pexAdvertised := make(map[string]struct{})
	// pexLimit rate-limits the ut_pex messages this peer sends us.
	var pexLimit pexRateLimiter
	var pexTicker *time.Ticker
	var pexTick <-chan time.Time
	sendPEXDelta := func() {
		if peerUtPexID == -1 || !s.pexEnabled() {
			return
		}
		pexMsg, nextAdvertised, ok := s.buildPEXDelta(peerAddr, pexAdvertised)
		pexAdvertised = nextAdvertised
		if !ok {
			return
		}
		if err := client.SendPEX(byte(peerUtPexID), pexMsg); err != nil {
			_ = conn.Close()
		}
	}
	startPEX := func() {
		if pexTicker == nil && pexInterval > 0 {
			pexTicker = time.NewTicker(pexInterval)
			pexTick = pexTicker.C
		}
		sendPEXDelta()
	}
	defer func() {
		if pexTicker != nil {
			pexTicker.Stop()
		}
	}()

	// Helper: check if peer has piece
	hasPiece := func(index int64) bool {
		byteIndex := index / 8
		bitIndex := index % 8
		if byteIndex >= int64(len(peerBitfield)) {
			return false
		}
		return (peerBitfield[byteIndex] & (1 << (7 - bitIndex))) != 0
	}

	// setPeerBitfield swaps in the peer's freshly advertised bitfield and folds the
	// delta into swarm availability. Shared by the bitfield, have_all, and have_none
	// handlers so the one on-the-wire bookkeeping lives in a single place.
	setPeerBitfield := func(newBF []byte) {
		noPick = false
		oldBF := append([]byte(nil), peerBitfield...)
		peerBitfield = newBF
		s.applyBitfieldAvailability(oldBF, peerBitfield)
	}
	// markPeerSeed records that the peer has every piece without touching per-piece
	// availability (see peerIsSeed). Haves counted before the announcement are
	// withdrawn first so the peer is not counted twice.
	markPeerSeed := func(numPieces int) {
		if bitfieldAny(peerBitfield) {
			s.removePeerAvailability(peerBitfield)
		}
		peerBitfield = fullPieceBitfield(numPieces)
		peerIsSeed = true
		noPick = false
	}
	// applyAnnouncedBitfield installs a length-checked bitfield announcement.
	applyAnnouncedBitfield := func(payload []byte, numPieces int) {
		if bitfieldComplete(payload, numPieces) {
			markPeerSeed(numPieces)
			return
		}
		setPeerBitfield(append([]byte(nil), payload...))
	}

	// hasAllowedFastWork reports whether any piece the peer granted us via
	// allowed_fast is still worth requesting (the peer has it, we don't, it isn't
	// rejected). Used to keep a choked peer's pump from running the full piece scan
	// once its allowed-fast pieces are all done.
	hasAllowedFastWork := func() bool {
		if !fastEnabled || len(peerAllowedFast) == 0 {
			return false
		}
		s.mu.RLock()
		defer s.mu.RUnlock()
		for idx := range peerAllowedFast {
			if _, rejected := peerRejectedPieces[idx]; rejected {
				continue
			}
			if !hasPiece(idx) || idx < 0 || idx >= int64(len(s.PieceStates)) {
				continue
			}
			state := s.PieceStates[idx]
			if (state == PieceEmpty || (state == PieceDownloading && s.endgameActiveLocked())) &&
				s.isPieceWanted(idx) {
				return true
			}
		}
		return false
	}

	// endgameCopyLimit is how many redundant endgame copies this connection may
	// hold: what its request window can fill, and at least
	// minEndgamePiecesPerPeer. pump sets it from the current window.
	endgameCopyLimit := minEndgamePiecesPerPeer

	// openNewPiece claims the highest-priority, rarest empty wanted piece this peer has
	// and marks it PieceDownloading. In endgame (no fresh pieces left to claim) it
	// instead returns a redundant copy of an in-progress piece this peer has, leaving
	// that piece's state owned by the original downloader. Returns nil when the peer
	// has nothing left for us.
	openNewPiece := func(canRequestPiece func(int64) bool) *activeDownload {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.paused || s.closed || s.Storage == nil {
			return nil
		}
		// Never claim a piece we could not request over the wire or assemble into
		// one buffer: opening it allocates per-block state up front, before any data
		// arrives. Only a malformed torrent has such pieces, so a normal torrent
		// pays a single comparison here and its picker callback is left untouched.
		if !pieceLengthAssemblable(s.Storage.PieceLengthValue()) {
			requestable := canRequestPiece
			canRequestPiece = func(index int64) bool {
				return requestable(index) && pieceLengthAssemblable(s.Storage.PieceLength(index))
			}
		}
		endgame := false
		bestIdx := selectNeededPiece(s, canRequestPiece)
		if bestIdx == -1 {
			if s.endgameActiveLocked() {
				owned := make(map[int64]bool, len(activeDownloads))
				copies := 0
				for _, dl := range activeDownloads {
					owned[dl.pieceIndex] = true
					if dl.endgame {
						copies++
					}
				}
				if copies < endgameCopyLimit {
					bestIdx = s.selectEndgamePieceLocked(canRequestPiece, owned)
					endgame = true
				}
			}
			if bestIdx == -1 {
				noPick, noPickGen, noPickAt = true, s.pickGen.Load(), time.Now()
				return nil
			}
		}
		if !endgame {
			s.setPieceStateLocked(bestIdx, PieceDownloading)
		}
		numBlocks := s.blocksInPiece(int64(bestIdx))
		return &activeDownload{
			pieceIndex: int64(bestIdx),
			hash:       s.Torrent.PieceHashes[bestIdx],
			length:     s.Storage.PieceLength(int64(bestIdx)),
			numBlocks:  numBlocks,
			blocks:     make([][]byte, numBlocks),
			blockMsgs:  make([]*peer.Message, numBlocks),
			pending:    make(map[int64]*blockRequest),
			endgame:    endgame,
		}
	}

	// nextBlockInPiece returns the begin offset of the next block to request from
	// dl (re-sending timed-out blocks first, then advancing the fresh cursor), or
	// -1 when the piece is fully requested. It does not mark the request as sent;
	// that happens only after limiter, pipeline budget, and socket queueing succeed.
	nextBlockInPiece := func(dl *activeDownload) int64 {
		for len(dl.retry) > 0 {
			begin := dl.retry[len(dl.retry)-1]
			dl.retry = dl.retry[:len(dl.retry)-1]
			if req, ok := dl.pending[begin]; ok && !req.requested && !req.received {
				return begin
			}
		}
		for dl.nextBlock < dl.numBlocks {
			b := dl.nextBlock
			dl.nextBlock++
			begin := b * BlockSize
			if _, exists := dl.pending[begin]; exists {
				continue
			}
			blockLen := int64(BlockSize)
			if begin+blockLen > dl.length {
				blockLen = dl.length - begin
			}
			dl.pending[begin] = &blockRequest{
				pieceIndex: dl.pieceIndex,
				begin:      begin,
				length:     blockLen,
			}
			return begin
		}
		return -1
	}
	requestable := func(dl *activeDownload) bool {
		return len(dl.retry) > 0 || dl.nextBlock < dl.numBlocks
	}
	anyRequestable := func() bool {
		for _, dl := range activeDownloads {
			if requestable(dl) {
				return true
			}
		}
		return false
	}
	endgameCopies := func() int {
		n := 0
		for _, dl := range activeDownloads {
			if dl.endgame {
				n++
			}
		}
		return n
	}
	// roomForPiece reports whether one more piece fits in this connection's open
	// piece bytes (peerOpenPieceBytesCap). The open bytes are summed here instead of
	// counted per block: a piece is only opened once every open one is fully
	// requested, and the block path stays free of accounting.
	var torrentPieceLen int64
	roomForPiece := func() bool {
		if len(activeDownloads) < minOpenPiecesPerPeer {
			return true
		}
		if torrentPieceLen <= 0 {
			s.mu.RLock()
			torrentPieceLen = s.Torrent.PieceLength
			s.mu.RUnlock()
		}
		var open int64
		for _, dl := range activeDownloads {
			open += dl.length
		}
		return open+torrentPieceLen <= peerOpenPieceBytesCap(torrentPieceLen)
	}
	avgBlocksPerPiece := func() int {
		if len(activeDownloads) > 0 {
			var total int64
			for _, dl := range activeDownloads {
				total += dl.numBlocks
			}
			return max(1, int((total+int64(len(activeDownloads))-1)/int64(len(activeDownloads))))
		}
		s.mu.RLock()
		pieceLength := s.Torrent.PieceLength
		s.mu.RUnlock()
		if pieceLength <= 0 {
			return 1
		}
		return max(1, int((pieceLength+BlockSize-1)/BlockSize))
	}
	requestableWorkAvailable := func(pieceCap int, canRequestPiece func(int64) bool) bool {
		if anyRequestable() {
			return true
		}
		if len(activeDownloads) >= pieceCap || !roomForPiece() {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.hasSelectableNeededPieceLocked(canRequestPiece) {
			return true
		}
		if s.endgameActiveLocked() && endgameCopies() < endgameCopyLimit {
			for i := range s.downloadingPieces {
				if canRequestPiece(int64(i)) && s.isPieceWanted(int64(i)) {
					return true
				}
			}
		}
		return false
	}
	markRequestSent := func(req *blockRequest, budgetBytes int64, sentAt time.Time) {
		req.requested = true
		req.requestedAt = sentAt
		if req.firstRequestedAt.IsZero() {
			req.firstRequestedAt = sentAt
		}
		req.pipelineBudgetBytes = budgetBytes
		pipeline.OnRequestSent(req, sentAt)
	}

	// These timestamps are owned by this peer goroutine. A request gives the peer a
	// fresh stall-timeout window even if rate limiting delayed issuing it; a received
	// block records actual forward progress.
	lastProgressAt := time.Now()
	lastRequestAt := time.Time{}
	waitingForBandwidth := false
	// lastActiveAt is when payload last moved either way or the peer's interest
	// changed; see peerInactivityTimeout.
	lastActiveAt := time.Now()
	// lastPumpAt is when pump last ran. The loop runs pump itself once it has not
	// run for pumpSweepInterval; see the check at the top of the loop.
	var lastPumpAt time.Time
	pumpSweepInterval := blockRequestTimeout / 4

	// uploadQueue holds this peer's block requests awaiting upload bandwidth. It is
	// owned by this peer goroutine and drained FIFO by uploadPump; never touched by
	// other goroutines, so it needs no lock.
	var uploadQueue []uploadRequest

	// uploadChoked is our AmChoking for this peer as last seen by this loop.
	// noteUploadChoke drops the queued requests when we start choking the peer:
	// BEP 3 says a choke discards pending requests, and BEP 6 wants a reject for
	// each one from a fast peer. Allowed-fast requests stay queued. Without this a
	// peer could fill the queue just before the choke round and still be served up
	// to maxUploadQueue blocks after it.
	uploadChoked := false
	noteUploadChoke := func(amChoking bool) {
		if !amChoking {
			uploadChoked = false
			return
		}
		if uploadChoked {
			return
		}
		uploadChoked = true
		kept := uploadQueue[:0]
		for _, r := range uploadQueue {
			if _, ok := allowedFastForPeer[r.index]; ok {
				kept = append(kept, r)
				continue
			}
			if fastEnabled {
				_ = client.SendRejectRequest(uint32(r.index), uint32(r.begin), uint32(r.length))
			}
		}
		uploadQueue = kept
	}
	// wireAmChoking is the choke state last sent to the peer; connections start
	// choked. The choker only updates pState.AmChoking and wakes this loop
	// (peer.Client.Notify). applyChoke runs wherever the loop reads AmChoking: it
	// sends a change in order with everything else the loop writes, ahead of the
	// rejects noteUploadChoke sends for the requests a choke drops.
	wireAmChoking := true
	applyChoke := func(amChoking bool) {
		if amChoking != wireAmChoking {
			wireAmChoking = amChoking
			if amChoking {
				_ = client.SendChoke()
			} else {
				_ = client.SendUnchoke()
			}
		}
		noteUploadChoke(amChoking)
	}
	syncChoke := func() {
		s.mu.RLock()
		amChoking := pState.AmChoking
		s.mu.RUnlock()
		applyChoke(amChoking)
	}

	// refreshUploadChoke reads AmChoking for paths that do not already hold s.mu;
	// it only takes the lock when there is a queue to drop.
	refreshUploadChoke := func() {
		if len(uploadQueue) == 0 {
			return
		}
		syncChoke()
	}

	// pump re-arms timed-out requests, then fills the request window across all
	// active pieces, opening new pieces as needed. Called after each inbound
	// message — INCLUDING keep-alives — so the pipeline stays full across piece
	// boundaries and, crucially, so the timeout sweep below still runs when a peer
	// that already took our requests goes quiet but keeps the socket warm with
	// keep-alives. For a peer that sends nothing at all, the liveness ticker runs
	// the sweep from the top of the loop.
	pump := func() time.Duration {
		// Block requests below are queued into the client's write buffer; flush the
		// whole burst in one syscall on the way out, regardless of which branch
		// returns. Flushing an empty buffer is a no-op, so this is cheap on the
		// paused/choked/keep-alive paths that write nothing. If flushing fails, close
		// the connection immediately to trigger teardown.
		defer func() {
			if err := client.Flush(); err != nil {
				_ = conn.Close()
			}
		}()

		s.mu.RLock()
		paused := s.paused
		choked := pState.Choked
		s.mu.RUnlock()
		now := time.Now()
		lastPumpAt = now
		canRequestPiece := func(index int64) bool {
			if !hasPiece(index) {
				return false
			}
			if _, rejected := peerRejectedPieces[index]; rejected {
				return false
			}
			if !choked {
				return true
			}
			if !fastEnabled {
				return false
			}
			_, ok := peerAllowedFast[index]
			return ok
		}
		if paused {
			waitingForBandwidth = false
			publishPipelineSnapshot(now, false)
			return 0
		}

		// Offer allowed-fast pieces we have completed since the last pass (a leecher
		// that becomes a partial seed mid-connection still grants its fast set).
		maybeAdvertiseAllowedFast()

		// Re-arm timed-out requests, or drop a peer that has stalled past its retry
		// budget, and count what is still outstanding. This sweep runs even when the
		// peer is choking us: a peer that unchoked us, took a window of requests, then
		// re-choked (or simply stopped responding) still has outstanding requests that
		// must be timed out so the connection is dropped instead of held forever.
		outstanding := 0
		for _, dl := range activeDownloads {
			for begin, req := range dl.pending {
				if !req.requested || req.received {
					continue
				}
				if now.Sub(req.requestedAt) >= blockRequestTimeout {
					if req.retries >= maxBlockRequestRetries {
						s.mu.Lock()
						s.lastErr = fmt.Errorf("timed out downloading piece %d", dl.pieceIndex)
						s.mu.Unlock()
						_ = conn.Close() // pieces are released by the disconnect cleanup
						return 0
					}
					finishRequest(req, requestFinishTimeout, now)
					req.retries++
					dl.retry = append(dl.retry, begin)
					continue
				}
				outstanding++
			}
		}

		// A choked peer won't fulfill new requests, so don't open pieces or send;
		// the timeout sweep above has already run, which is the part that matters
		// for not leaking a stalled connection. Fast peers may still serve pieces
		// they explicitly listed with allowed_fast — but only proceed when at least
		// one such piece is still worth fetching, so a choked connection whose fast
		// set is exhausted doesn't run the full piece scan on every message. The
		// allowed-fast pieces kept across the choke count too: a block of one that
		// timed out must be re-sent, or it never runs out of retries and the peer
		// could hold the piece (and its received blocks) forever.
		if choked && !anyRequestable() && !hasAllowedFastWork() {
			waitingForBandwidth = false
			publishPipelineSnapshot(now, false)
			return 0
		}

		avgBlocks := avgBlocksPerPiece()
		window := pipeline.WindowBlocks(now)
		if effective := pipeline.EffectiveWindowBlocks(avgBlocks); effective < window {
			window = effective
			pipeline.OnPieceCapLimited(now)
		}
		pieceCap := pipeline.ConcurrentPieceCap(avgBlocks, 0)
		endgameCopyLimit = max(minEndgamePiecesPerPeer, divCeil(window, avgBlocks))

		// Fill the window, opening pieces on demand.
		for outstanding < window {
			var chosen *activeDownload
			var begin int64 = -1
			for _, dl := range activeDownloads {
				if !requestable(dl) {
					continue
				}
				if b := nextBlockInPiece(dl); b != -1 {
					chosen, begin = dl, b
					break
				}
			}
			if chosen == nil {
				if len(activeDownloads) >= pieceCap || !roomForPiece() {
					pipeline.OnPieceCapLimited(now)
					break
				}
				if noPick && s.pickGen.Load() == noPickGen && now.Sub(noPickAt) < pickRetryInterval {
					break // nothing this peer can serve has appeared since the last pick
				}
				newDL := openNewPiece(canRequestPiece)
				if newDL == nil {
					noPickRestricted = choked || len(peerRejectedPieces) > 0
					break
				}
				activeDownloads = append(activeDownloads, newDL)
				continue
			}

			req := chosen.pending[begin]
			if !pipeline.CanReserve(req.length) {
				pipeline.OnBudgetLimited(now)
				chosen.retry = append(chosen.retry, begin)
				waitingForBandwidth = false
				publishPipelineSnapshot(now, true)
				return 100 * time.Millisecond
			}
			// Never wait for bandwidth in the peer event loop: even with an empty
			// request window, blocking here lets many rate-limited peers occupy every
			// manager-wide connection slot while none of their sockets are drained.
			// The event loop schedules another pump when the limiter says tokens should
			// be available, while the dedicated reader below remains responsive.
			reserved, retryAfter, refundBandwidth := s.reserveDownloadWithRefund(int(req.length))
			if !reserved {
				pipeline.OnAppLimited(now, retryAfter)
				chosen.retry = append(chosen.retry, begin)
				// With no request in flight, this idle period is intentional: the
				// limiter is accumulating enough tokens for one full block. If other
				// requests are outstanding, the peer still owes us data and remains
				// subject to the normal stall and request timeouts.
				waitingForBandwidth = outstanding == 0
				publishPipelineSnapshot(now, true)
				return retryAfter
			}
			budgetBytes := req.length
			if !s.pipelineBudget.tryReserve(budgetBytes) {
				if refundBandwidth != nil {
					refundBandwidth()
				}
				pipeline.OnBudgetLimited(now)
				chosen.retry = append(chosen.retry, begin)
				waitingForBandwidth = false
				publishPipelineSnapshot(now, true)
				return 100 * time.Millisecond
			}
			if err := client.WriteRequest(uint32(chosen.pieceIndex), uint32(begin), uint32(req.length)); err != nil {
				s.pipelineBudget.release(budgetBytes)
				if refundBandwidth != nil {
					refundBandwidth()
				}
				_ = conn.Close()
				return 0 // dead connection; cleanup releases the pieces
			}
			sentAt := time.Now()
			markRequestSent(req, budgetBytes, sentAt)
			lastRequestAt = sentAt
			waitingForBandwidth = false
			outstanding++
		}
		if outstanding >= window && requestableWorkAvailable(pieceCap, canRequestPiece) {
			pipeline.OnWindowLimited(now)
		}
		waitingForBandwidth = false
		publishPipelineSnapshot(now, false)
		return 0
	}

	// uploadPump serves this peer's queued block requests as fast as the upload
	// limiters allow, WITHOUT ever blocking: a request is served only when its bytes
	// can be reserved without waiting, so this goroutine keeps running pump() and
	// draining the socket instead of stalling on a limiter Wait (issue #59 — the
	// download side was already converted to this non-blocking discipline). It returns
	// the delay after which it should run again (0 when the queue is empty or fully
	// drained) so the caller can arm the shared rate-retry timer alongside pump().
	uploadPump := func() time.Duration {
		served := false
		defer func() {
			if served {
				lastActiveAt = time.Now()
			}
		}()
		for len(uploadQueue) > 0 {
			r := uploadQueue[0]
			reserved, retryAfter, refund := s.reserveUploadWithRefund(int(r.length))
			if !reserved {
				// Not enough tokens yet: leave this request (and the rest) queued and
				// ask the loop to retry once the limiter says they should be available.
				return retryAfter
			}
			// Borrow a pooled block-sized buffer for the disk read instead of
			// allocating one per served block. SendPiece flushes synchronously, so
			// the buffer is fully on the wire by the time it returns and can be
			// recycled — the seed hot path allocates nothing (issue #55).
			bufPtr := s.getUploadBlockBuf()
			buf := (*bufPtr)[:r.length]
			if _, err := s.Storage.ReadBlock(r.index, r.begin, buf); err != nil {
				// Shouldn't happen for a completed piece, but if the read fails don't
				// leak the reserved tokens; tell a fast peer the request is dead so its
				// per-request accounting stays consistent, then drop it.
				s.putUploadBlockBuf(bufPtr)
				if refund != nil {
					refund()
				}
				if fastEnabled {
					_ = client.SendRejectRequest(uint32(r.index), uint32(r.begin), uint32(r.length))
				}
				uploadQueue = uploadQueue[1:]
				continue
			}
			if err := client.SendPiece(uint32(r.index), uint32(r.begin), buf); err != nil {
				// Dead socket: refund the reservation and let teardown handle the rest.
				s.putUploadBlockBuf(bufPtr)
				if refund != nil {
					refund()
				}
				_ = conn.Close()
				return 0
			}
			s.putUploadBlockBuf(bufPtr)
			served = true
			// Lock-free counter update (see the download hot path above).
			s.Uploaded.Add(r.length)
			atomic.AddInt64(&pState.Uploaded, r.length)
			uploadQueue = uploadQueue[1:]
		}
		return 0
	}

	// eitherSideInterested reports whether the peer wants data from us or has a
	// piece we still want. It scans the picker, so it only runs once the
	// connection has been idle for peerInactivityTimeout.
	eitherSideInterested := func() bool {
		if len(activeDownloads) > 0 {
			return true
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if pState.Interested {
			return true
		}
		// While fetching metadata we cannot tell yet what the peer has that we
		// need, but a peer that cannot serve the metadata is of no use until we
		// have it; without this an inbound one would never be dropped.
		if s.metadataMode {
			return peerUtMetadataID != -1
		}
		// While rechecking we cannot tell yet what we still need.
		if s.verifying {
			return true
		}
		if s.isCompletedLocked() {
			return false
		}
		peerHas := func(index int64) bool { return hasPiece(index) }
		if s.hasSelectableNeededPieceLocked(peerHas) {
			return true
		}
		if s.endgameActiveLocked() {
			for i := range s.downloadingPieces {
				if hasPiece(int64(i)) && s.isPieceWanted(int64(i)) {
					return true
				}
			}
		}
		return false
	}

	// dropCompletedElsewhere is the endgame "cancel on receipt" path: it drops any
	// in-progress piece that another peer has finished (so its state is no longer
	// PieceDownloading) and sends a Cancel for each of our still-outstanding blocks so
	// the peer stops feeding us data the swarm no longer needs. Bounded by
	// the adaptive per-peer piece cap, so it is cheap to run every message: it
	// takes only the read lock unless it has something to drop (pump publishes
	// the pipeline snapshot, throttled, after every message anyway).
	dropCompletedElsewhere := func() {
		if len(activeDownloads) == 0 {
			return
		}
		var finished []int64
		s.mu.RLock()
		for _, dl := range activeDownloads {
			if dl.pieceIndex < 0 || dl.pieceIndex >= int64(len(s.PieceStates)) ||
				s.PieceStates[dl.pieceIndex] != PieceDownloading {
				finished = append(finished, dl.pieceIndex)
			}
		}
		s.mu.RUnlock()
		if len(finished) == 0 {
			return
		}
		now := time.Now()
		for _, idx := range finished {
			dl := findDownload(idx)
			if dl == nil {
				continue
			}
			for begin, req := range dl.pending {
				if req.requested && !req.received {
					_ = client.SendCancel(uint32(idx), uint32(begin), uint32(req.length))
					finishRequest(req, requestFinishCancel, now)
				}
			}
			removeDownload(idx)
		}
		publishPipelineSnapshot(now, true)
	}

	// ut_metadata fetch state (BEP 9) for this connection. peerMetadataSize is the
	// size this peer advertised. metadataRequested marks the blocks asked of this
	// peer in fetch round metadataRound (the session's metadataEpoch, which moves on
	// whenever a failed assembly discards the accumulator). A block is accepted from
	// this peer only if it was asked for in the current round, so a peer cannot slip
	// unsolicited blocks into an assembly other peers are feeding. metadataAskedAt
	// is when each block was last asked for, or when the peer rejected it
	// (metadataRejected), so the retry tick can ask again for a block the peer
	// dropped or refused.
	peerMetadataSize := 0
	var metadataRequested []bool
	var metadataAskedAt []time.Time
	var metadataRejected []bool
	var metadataRound uint64
	var metadataRetryTicker *time.Ticker
	var metadataRetryTick <-chan time.Time
	defer func() {
		if metadataRetryTicker != nil {
			metadataRetryTicker.Stop()
		}
	}()

	// requestMetadataBlocks asks this peer for every block the shared accumulator
	// still lacks and that it has not been asked for this round. The session state
	// is read in a single locked pass that re-checks the fetch is still running and
	// sized as this peer advertised, and the requests go out after unlocking, so a
	// concurrent reset can never leave us indexing a stale block list. newRound
	// marks a call made because a new fetch round began; those are skipped while
	// the session has a blocking error (storage could not be set up), so a
	// persistent failure does not turn into an endless re-download of metadata.
	//
	// After failed assemblies (see noteMetadataRoundFailedLocked) a host that
	// sized or fed one waits metadataSizeStallTimeout before it may size or own a
	// new round, so a peer that has not failed us gets the round first, and new
	// rounds back off after failures in a row; in a solo round only the owner is
	// asked. Each is re-checked from the retry tick.
	requestMetadataBlocks := func(newRound bool) {
		if peerUtMetadataID == -1 || peerMetadataSize <= 0 {
			return
		}
		var missing []int
		now := time.Now()
		s.mu.Lock()
		if !s.metadataMode || s.metadataCompleted {
			s.mu.Unlock()
			return
		}
		if newRound && s.statusErr != nil {
			metadataRound = s.metadataEpoch
			s.mu.Unlock()
			return
		}
		suspect := s.metadataSuspectLocked(source.host, now)
		suspectWaiting := suspect && now.Sub(s.metadataResetAt) < metadataSizeStallTimeout
		stalled := now.Sub(s.metadataProgressAt) >= metadataSizeStallTimeout
		resize := false
		switch {
		case s.metadataSize == 0:
			// The first sized peer, or the first after a reset.
			resize = !now.Before(s.metadataNextRoundAt) && !suspectWaiting
		case s.metadataSize != peerMetadataSize && s.statusErr == nil &&
			((stalled && !suspectWaiting) ||
				(!suspect && metadataFeedSlow(now, s.metadataSizedAt, s.metadataProgressAt, s.metadataRoundBlocks))):
			// Whoever sized the accumulator delivers nothing, or too little to be
			// an honest peer: start a new round at this peer's size, and suspect
			// the old sizer. Peers on the old size are no longer asked, and their
			// late blocks fail the round check. A suspect may replace only a
			// round that takes no blocks at all, never one that is merely slow,
			// so it cannot keep taking a slow honest peer's round away, while a
			// peer made a suspect by a mixed round can still replace a silent
			// sizer.
			resize = true
			s.metadataEpoch++
			s.addMetadataSuspectLocked(s.metadataSizedBy, now)
		}
		if resize {
			numBlocks := (peerMetadataSize + peer.MetadataBlockSize - 1) / peer.MetadataBlockSize
			s.metadataSize = peerMetadataSize
			s.metadataBuf = make([]byte, peerMetadataSize)
			s.metadataPieces = make([]bool, numBlocks)
			s.metadataFrom = make([]string, numBlocks)
			s.metadataProgressAt = now
			s.metadataSizedBy = source.host
			s.metadataSizedAt = now
			s.metadataRoundBlocks = 0
			s.metadataOwner = nil
		}
		ask := s.metadataSize == peerMetadataSize
		if ask && s.metadataSolo && s.metadataOwner != client {
			// A solo round is fed by its owner alone. Another connection takes it
			// over once the owner is gone or has taken no block for
			// metadataSizeStallTimeout, dropping what the owner supplied, so the
			// round keeps a single supplier to blame. A slow owner keeps the
			// round: taking it away would restart a slow swarm's fetch for good.
			ownerGone := s.metadataOwner == nil ||
				now.Sub(laterTime(s.metadataOwnedAt, s.metadataProgressAt)) >= metadataSizeStallTimeout
			if ownerGone && !suspectWaiting {
				if slices.Contains(s.metadataPieces, true) {
					clear(s.metadataPieces)
					clear(s.metadataFrom)
					s.metadataEpoch++ // every connection's requests start over
				}
				s.metadataOwner = client
				s.metadataOwnedAt = now
			} else {
				ask = false
			}
		}
		if s.metadataEpoch != metadataRound || len(metadataRequested) != len(s.metadataPieces) {
			metadataRound = s.metadataEpoch
			metadataRequested = make([]bool, len(s.metadataPieces))
			metadataAskedAt = make([]time.Time, len(s.metadataPieces))
			metadataRejected = make([]bool, len(s.metadataPieces))
		}
		if ask {
			for i, have := range s.metadataPieces {
				if !have && !metadataRequested[i] {
					metadataRequested[i] = true
					metadataAskedAt[i] = now
					metadataRejected[i] = false
					missing = append(missing, i)
				}
			}
		}
		s.mu.Unlock()
		for _, i := range missing {
			if err := client.SendMetadataRequest(byte(peerUtMetadataID), i); err != nil {
				_ = conn.Close()
				return
			}
		}
	}

	// acceptMetadataBlock stores a ut_metadata data block this peer was asked for
	// and, once every block is in, hands the assembled info dict to
	// onMetadataDownloaded (which discards the accumulator and advances
	// metadataEpoch if it fails the infohash check). The block is recorded
	// against this connection's host, so a failed assembly can be blamed; in a
	// solo round only the owner's blocks count.
	acceptMetadataBlock := func(metaMsg *peer.MetadataMessage) {
		piece := metaMsg.Piece
		if piece < 0 || piece >= len(metadataRequested) || !metadataRequested[piece] {
			return // unsolicited, or asked for in an earlier round
		}
		metadataRequested[piece] = false
		metadataRejected[piece] = false
		s.mu.Lock()
		if s.metadataEpoch != metadataRound || !s.metadataMode || s.metadataCompleted ||
			s.metadataSize == 0 || piece >= len(s.metadataPieces) || s.metadataPieces[piece] ||
			(metaMsg.TotalSize > 0 && metaMsg.TotalSize != s.metadataSize) ||
			(s.metadataSolo && s.metadataOwner != client) {
			s.mu.Unlock()
			return
		}
		offset := piece * peer.MetadataBlockSize
		expectedLen := min(peer.MetadataBlockSize, s.metadataSize-offset)
		if expectedLen <= 0 || len(metaMsg.Data) != expectedLen || offset+expectedLen > len(s.metadataBuf) {
			s.mu.Unlock()
			return
		}
		copy(s.metadataBuf[offset:], metaMsg.Data)
		s.metadataPieces[piece] = true
		if piece < len(s.metadataFrom) {
			s.metadataFrom[piece] = source.host
		}
		s.metadataRoundBlocks++
		lastProgressAt = time.Now() // metadata progress; keeps the stall reaper off
		lastActiveAt = lastProgressAt
		s.metadataProgressAt = lastProgressAt
		for _, done := range s.metadataPieces {
			if !done {
				s.mu.Unlock()
				return
			}
		}
		bufCopy := append([]byte(nil), s.metadataBuf...)
		s.mu.Unlock()
		if err := s.onMetadataDownloaded(bufCopy); err != nil {
			s.mu.Lock()
			s.lastErr = err
			s.mu.Unlock()
		}
	}

	// Per-connection budget for serving ut_metadata blocks (see metadataServeWindow).
	var metadataServeWindowStart time.Time
	metadataServed := 0

	// serveMetadataRequest answers a peer's ut_metadata request from our info dict.
	// Replies are charged to the upload limiters without waiting (a shortfall is
	// answered with a reject, never a stall of this loop), counted in the upload
	// stats, and capped per connection, so 30-byte requests cannot be turned into an
	// unmetered 16 KiB-per-request stream.
	serveMetadataRequest := func(metaMsg *peer.MetadataMessage) {
		if peerUtMetadataID == -1 {
			return // the peer never told us which id to answer on
		}
		s.mu.RLock()
		// Nothing to serve while fetching; once known, a private torrent's info dict
		// stays within its tracker's swarm (BEP 27), even on connections that were
		// set up while the private flag was still unknown.
		serve := !s.metadataMode && !s.Torrent.Private
		infoBytes := s.Torrent.InfoBytes
		s.mu.RUnlock()

		offset := int64(metaMsg.Piece) * peer.MetadataBlockSize
		if !serve || len(infoBytes) == 0 || offset < 0 || offset >= int64(len(infoBytes)) {
			_ = client.SendMetadataReject(byte(peerUtMetadataID), metaMsg.Piece)
			return
		}
		now := time.Now()
		if now.Sub(metadataServeWindowStart) >= metadataServeWindow {
			metadataServeWindowStart = now
			metadataServed = 0
		}
		numBlocks := (len(infoBytes) + peer.MetadataBlockSize - 1) / peer.MetadataBlockSize
		if metadataServed >= metadataServeRequestsPerBlock*numBlocks {
			_ = client.SendMetadataReject(byte(peerUtMetadataID), metaMsg.Piece)
			return
		}
		blockLen := min(int64(peer.MetadataBlockSize), int64(len(infoBytes))-offset)
		reserved, _, refund := s.reserveUploadWithRefund(int(blockLen))
		if !reserved {
			_ = client.SendMetadataReject(byte(peerUtMetadataID), metaMsg.Piece)
			return
		}
		if err := client.SendMetadataData(byte(peerUtMetadataID), metaMsg.Piece, len(infoBytes), infoBytes[offset:offset+blockLen]); err != nil {
			if refund != nil {
				refund()
			}
			_ = conn.Close()
			return
		}
		metadataServed++
		lastActiveAt = now
		s.Uploaded.Add(blockLen)
		atomic.AddInt64(&pState.Uploaded, blockLen)
	}

	// handleExtendedMessage processes one BEP 10 message and returns a non-empty
	// disconnect reason when the peer must be dropped.
	handleExtendedMessage := func(payload []byte) string {
		if len(payload) < 2 {
			return ""
		}
		extMsgID := payload[0]
		payloadBytes := payload[1:]

		switch {
		case extMsgID == peer.ExtHandshake:
			// Size gates run before any bencode decode: see the caps in pkg/peer.
			if len(payloadBytes) > peer.MaxExtHandshakeSize {
				return "oversized_extension"
			}
			// BEP 10 lets a peer re-send its handshake to update it, but each one
			// is decoded and can restart the metadata requests, so only the first
			// few per connection are honoured.
			extHandshakes++
			if extHandshakes > maxExtHandshakesPerConn {
				return ""
			}
			hs, err := peer.ParseExtensionHandshake(payloadBytes)
			if err != nil {
				return ""
			}
			// Keep our request window within the queue the peer says it has.
			pipeline.LimitWindowBlocks(hs.RequestQueue)
			if utPexID, ok := hs.Extensions[peer.ExtNamePEX]; ok && s.pexEnabled() {
				peerUtPexID = utPexID
				startPEX()
			}
			utID, ok := hs.Extensions[peer.ExtNameMetadata]
			if !ok {
				return ""
			}
			peerUtMetadataID = utID
			s.mu.RLock()
			fetching := s.metadataMode && !s.metadataCompleted
			s.mu.RUnlock()
			if !fetching {
				return ""
			}
			if hs.MetadataSize <= 0 || hs.MetadataSize > peer.MaxMetadataSize {
				s.mu.Lock()
				s.lastErr = fmt.Errorf("invalid metadata size from peer: %d", hs.MetadataSize)
				s.mu.Unlock()
				return ""
			}
			peerMetadataSize = hs.MetadataSize
			requestMetadataBlocks(false)
			// Keep checking for a new fetch round while this peer idles, so an
			// assembly reset re-asks it instead of waiting for fresh connections.
			if metadataRetryTicker == nil && metadataRetryInterval > 0 {
				metadataRetryTicker = time.NewTicker(metadataRetryInterval)
				metadataRetryTick = metadataRetryTicker.C
			}

		case extMsgID == peer.LocalMetadataExtID:
			if len(payloadBytes) > peer.MaxMetadataMessageSize {
				return "oversized_extension"
			}
			metaMsg, err := peer.ParseMetadataMessage(payloadBytes)
			if err != nil {
				return ""
			}
			switch metaMsg.MsgType {
			case peer.MetadataRequest:
				serveMetadataRequest(metaMsg)
			case peer.MetadataData:
				acceptMetadataBlock(metaMsg)
			case peer.MetadataReject:
				// The block stays marked as asked of this peer, so other peers
				// supply it first; the retry tick asks this peer again after
				// metadataRejectRetryAfter.
				if p := metaMsg.Piece; p >= 0 && p < len(metadataRequested) && metadataRequested[p] && !metadataRejected[p] {
					metadataRejected[p] = true
					metadataAskedAt[p] = time.Now()
				}
			}

		case extMsgID == peer.LocalPEXExtID && s.pexEnabled():
			if len(payloadBytes) > peer.MaxPEXMessageSize {
				return "oversized_extension"
			}
			// BEP 11 peers send ut_pex about once a minute; see pexRateLimiter.
			use, flood := pexLimit.admit(time.Now())
			if flood {
				return "pex_flood"
			}
			if !use {
				return ""
			}
			if pexMsg, err := peer.ParsePEXMessage(payloadBytes); err == nil {
				s.handlePEXMessage(peerAddr, ip, pexMsg)
			}
		}
		return ""
	}

	type peerReadResult struct {
		msg *peer.Message
		err error
	}
	readCh := make(chan peerReadResult, 1)
	readDone := make(chan struct{})
	readStop := make(chan struct{})
	go func() {
		defer s.crashGuard("peer_reader")()
		defer close(readDone)
		// The read deadline is the dead-socket backstop (see peerReadTimeout). It
		// is moved only once less than peerReadTimeout remains, so a busy
		// connection updates it once per peerReadTimeout/2 instead of per message.
		// time.Until on a deadline with a monotonic reading is cheaper than
		// time.Now.
		var readDeadline time.Time
		for {
			if time.Until(readDeadline) < peerReadTimeout {
				readDeadline = time.Now().Add(peerReadTimeout * 3 / 2)
				_ = conn.SetReadDeadline(readDeadline)
			}
			msg, err := client.ReadMessage()
			if err != nil {
				// Close before handing the error over: if the loop is blocked in a
				// write to this peer, that ends the write now instead of when the
				// write timeout fires.
				_ = conn.Close()
			}
			select {
			case readCh <- peerReadResult{msg: msg, err: err}:
			case <-s.ctx.Done():
				return
			case <-readStop:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		close(readStop)
		_ = conn.Close()
		<-readDone
	}()

	var rateTimer *time.Timer
	var rateRetry <-chan time.Time
	scheduleRateRetry := func(delay time.Duration) {
		if rateTimer != nil {
			if !rateTimer.Stop() {
				select {
				case <-rateTimer.C:
				default:
				}
			}
		}
		rateRetry = nil
		if delay <= 0 {
			return
		}
		if rateTimer == nil {
			rateTimer = time.NewTimer(delay)
		} else {
			rateTimer.Reset(delay)
		}
		rateRetry = rateTimer.C
	}
	defer func() {
		if rateTimer != nil {
			rateTimer.Stop()
		}
	}()

	// Read and scheduling event loop. Socket parsing stays in one dedicated goroutine,
	// while limiter retry timers can wake the request pump without interrupting a
	// partially-read peer-wire message.
	//
	// pooledMsg holds the message read in the previous iteration so its pooled wire
	// buffer is returned to the pool at the top of the next one, once we are fully
	// done with it. Piece blocks whose ownership passed to an activeDownload detach
	// themselves (pooledMsg = nil) and are released after piece assembly instead.
	var pooledMsg *peer.Message
	defer func() { pooledMsg.Release() }()

	// The liveness ticker wakes the loop when the peer sends nothing, so the checks
	// at the top of the loop (stall reaper, inactivity drop, request-timeout sweep)
	// still run for a silent peer, and drives our keep-alives. It ticks every
	// pumpSweepInterval while pieces are in flight, so their requests time out on
	// time, and every peerIdleTickInterval otherwise: one timer per connection,
	// reset only when the period changes.
	livenessInterval := func() time.Duration {
		if len(activeDownloads) > 0 && pumpSweepInterval > 0 {
			return pumpSweepInterval
		}
		return peerIdleTickInterval
	}
	tickInterval := livenessInterval()
	lastKeepAliveCheck := time.Now()
	livenessTicker := time.NewTicker(tickInterval)
	defer livenessTicker.Stop()
	// keepAliveDue reports whether a keep-alive check is due at now. Ticks are not
	// exact, so half a tick of slack keeps a tick that lands a hair early from
	// putting the check off by a whole tick: checks stay about
	// peerKeepAliveInterval apart, and our writes at most two of those.
	keepAliveDue := func(now time.Time) bool {
		return now.Sub(lastKeepAliveCheck) >= peerKeepAliveInterval-tickInterval/2
	}

	// peerChoking mirrors pState.Choked, which only this loop changes once the
	// connection is registered (choked, as every connection starts), so a repeated
	// choke or unchoke is recognised without the session lock.
	peerChoking := true
	// lastInterestScanAt is when an Interested last ran the unchoke scan; see
	// peerInterestScanInterval.
	var lastInterestScanAt time.Time
	// initAfterMetadata moves a connection opened before metadata into the
	// download: it sends our bitfield and interest and replays the
	// availability and allowed-fast offers the peer sent before the piece count
	// was known. numPieces is the torrent's piece count.
	initAfterMetadata := func(numPieces int) {
		if numPieces > 0 {
			client.SetBitfieldLimit(numPieces)
		}
		sendInitialPeerState()

		// onMetadataDownloaded installs the piece table before it leaves
		// metadata mode, so a bitfield or have_all handled in between was
		// applied at its real length already; replacing it with an empty one
		// would hide the peer's pieces and leak their availability.
		if len(peerBitfield) != (numPieces+7)/8 {
			peerBitfield = make([]byte, (numPieces+7)/8)
		}
		initializedPeersAndBitfield = true

		// Replay any availability the peer announced before we had metadata
		// (have_none is the default zero bitfield, so nothing to do). A buffered
		// bitfield whose length does not fit the real piece count is dropped.
		if numPieces > 0 {
			switch {
			case peerHaveAllPending:
				markPeerSeed(numPieces)
			case pendingBitfield != nil && len(pendingBitfield) == (numPieces+7)/8:
				applyAnnouncedBitfield(pendingBitfield, numPieces)
			}
		}
		peerHaveAllPending = false
		pendingBitfield = nil
		for _, idx := range pendingAllowedFast {
			if idx >= 0 && idx < int64(numPieces) {
				peerAllowedFast[idx] = struct{}{}
			}
		}
		noPick = false
		pendingAllowedFast = nil
		pendingAllowedFastSet = nil
	}

	// The loop's own guard, deferred last so it is the first to see a panic in
	// the loop: it ends the process before any cleanup above runs, including
	// the ones that take s.mu, which the panicking code may still hold.
	defer s.crashGuard("peer_loop")()
peerLoop:
	for {
		pooledMsg.Release()
		pooledMsg = nil

		s.mu.RLock()
		paused := s.paused
		// A connection opened before metadata switches to the download as soon
		// as metadata is in, whatever woke the loop: a seed that already sent
		// have_all and the info dict says nothing more until we are interested,
		// and keep-alives, ticks and Notify (onMetadataDownloaded wakes every
		// connection) never reach the per-message switch below.
		leftMetadata := !initializedPeersAndBitfield && !s.metadataMode
		piecesAtSwitch := 0
		if leftMetadata {
			piecesAtSwitch = len(s.PieceStates)
		}
		s.mu.RUnlock()
		if paused {
			disconnectReason = "paused"
			break
		}
		if leftMetadata {
			initAfterMetadata(piecesAtSwitch)
			dropCompletedElsewhere()
			scheduleRateRetry(minRetry(pump(), uploadPump()))
		}

		// Reap an unproductive peer: drop a connection that hasn't delivered a block
		// within peerStallTimeout so its outbound slot can be reused for a peer that
		// will. Gated by cheap checks that keep the O(pieces) completion check off the
		// per-message hot path: only OUTBOUND connections (an inbound peer holds an
		// inbound slot, not an outbound one, and is keyed by an ephemeral port we can't
		// redial — reaping it just drops a productive uploader). A recently issued
		// request also grants a fresh timeout window, which prevents an intentionally
		// slow limiter wait from making the request look stale before it is sent.
		loopNow := time.Now()
		lastUsefulAt := lastProgressAt
		if lastRequestAt.After(lastUsefulAt) {
			lastUsefulAt = lastRequestAt
		}
		if outbound && !waitingForBandwidth && loopNow.Sub(lastUsefulAt) > peerStallTimeout {
			s.mu.RLock()
			seeding := s.isCompletedLocked()
			// Background resume verification can hold pieces PieceUnverified, so there
			// may be nothing to request yet through no fault of the peer; don't reap
			// while verifying.
			verifying := s.verifying
			s.mu.RUnlock()
			if !seeding && !verifying {
				disconnectReason = "stalled"
				if logging.Enabled() {
					logging.Warn("peer_reaped",
						logging.String("info_hash", logInfoHash),
						logging.String("name", logName),
						logging.String("peer", peerAddr),
						logging.String("direction", direction),
						logging.Duration("idle", loopNow.Sub(lastUsefulAt)),
					)
				}
				break
			}
		}

		// Drop a connection neither side has any use for: an idle peer that only
		// sends keep-alives would otherwise hold its slot for good.
		if loopNow.Sub(lastActiveAt) > peerInactivityTimeout {
			if eitherSideInterested() {
				lastActiveAt = time.Now()
			} else {
				disconnectReason = "inactive"
				break
			}
		}

		// The request timeout sweep lives in pump, which runs after every message we
		// act on, but the many messages we discard (bad lengths, unknown indices,
		// unsolicited blocks, requests we reject) skip it. Run it here once it is
		// overdue, or a peer that took our requests could stream only such messages,
		// never time out, and hold the requested pieces (and their received blocks)
		// for good; an inbound peer has no stall reaper to fall back on. The
		// liveness ticker brings a peer that sends nothing here too. Pieces another
		// peer finished are cancelled first, as after every message.
		if len(activeDownloads) > 0 && loopNow.Sub(lastPumpAt) >= pumpSweepInterval {
			dropCompletedElsewhere()
			scheduleRateRetry(minRetry(pump(), uploadPump()))
		}

		if want := livenessInterval(); want != tickInterval {
			tickInterval = want
			livenessTicker.Reset(want)
			// A switch to the longer idle period restarts the tick phase; check
			// now if a check is due, so the switch cannot stretch the gap between
			// two checks by a whole idle period.
			if keepAliveDue(loopNow) {
				lastKeepAliveCheck = loopNow
				if err := client.SendKeepAliveIfIdle(); err != nil {
					disconnectReason = "write_error"
					disconnectErr = err
					break
				}
			}
		}

		var msg *peer.Message
		select {
		case result := <-readCh:
			if result.err != nil {
				disconnectReason = "read_error"
				if errors.Is(result.err, peer.ErrInvalidMessageLength) {
					disconnectReason = "invalid_message_length"
				}
				disconnectErr = result.err
				break peerLoop
			}
			msg = result.msg
			pooledMsg = msg // release its pooled buffer at the top of the next iteration
		case <-client.Notified():
			// The choker changed our choke state or pieces completed.
			syncChoke()
			if initializedPeersAndBitfield {
				_ = client.SendQueuedHaves()
			} else {
				// Nothing may precede the bitfield, which is sent at the top of
				// the loop once metadata is in and already covers these pieces.
				client.DropQueuedHaves()
			}
			continue
		case now := <-livenessTicker.C:
			if keepAliveDue(now) {
				lastKeepAliveCheck = now
				if err := client.SendKeepAliveIfIdle(); err != nil {
					disconnectReason = "write_error"
					disconnectErr = err
					break peerLoop
				}
			}
			// Back to the top of the loop, which runs the reaper, the inactivity
			// check and any overdue request-timeout sweep.
			continue
		case <-pexTick:
			sendPEXDelta()
			continue
		case now := <-metadataRetryTick:
			s.mu.RLock()
			fetching := s.metadataMode && !s.metadataCompleted
			s.mu.RUnlock()
			if !fetching {
				metadataRetryTicker.Stop()
				metadataRetryTicker = nil
				metadataRetryTick = nil
				continue
			}
			// Ask again for blocks this peer rejected a while ago or never
			// answered, then re-run the round checks: a new round may have begun,
			// a stalled or too-slow round may be ours to replace or own, or a
			// suspect's wait may be over. Blocks already in are not re-asked.
			for i, asked := range metadataRequested {
				if !asked {
					continue
				}
				wait := metadataRequestTimeout
				if metadataRejected[i] {
					wait = metadataRejectRetryAfter
				}
				if now.Sub(metadataAskedAt[i]) >= wait {
					metadataRequested[i] = false
					metadataRejected[i] = false
				}
			}
			requestMetadataBlocks(false)
			continue
		case <-rateRetry:
			rateRetry = nil
			// The timer covers whichever pump was waiting on bandwidth: re-run both the
			// download request pump and the upload serve pump, then re-arm for the sooner.
			refreshUploadChoke()
			scheduleRateRetry(minRetry(pump(), uploadPump()))
			continue
		case <-s.ctx.Done():
			disconnectReason = "context_cancelled"
			disconnectErr = s.ctx.Err()
			break peerLoop
		}

		if msg == nil {
			// Keep alive: still run pump so outstanding requests to a peer that now
			// sends only keep-alives time out (and the peer is dropped after its
			// retry budget) instead of stalling forever. Also drain any queued
			// uploads that have since accrued bandwidth.
			refreshUploadChoke()
			scheduleRateRetry(minRetry(pump(), uploadPump()))
			continue
		}

		s.mu.RLock()
		inMetaNow := s.metadataMode
		numPiecesNow := len(s.PieceStates)
		metadataEpochNow := s.metadataEpoch
		amChokingNow := pState.AmChoking
		s.mu.RUnlock()
		applyChoke(amChokingNow)

		// A failed metadata assembly started a new fetch round: re-ask this peer
		// for the blocks that are still missing.
		if inMetaNow && peerMetadataSize > 0 && metadataEpochNow != metadataRound {
			requestMetadataBlocks(true)
		}

		if !inMetaNow && !initializedPeersAndBitfield {
			initAfterMetadata(numPiecesNow)
		}

		// A peer that never negotiated the fast extension shouldn't be sending its
		// messages; ignore them rather than tearing down an otherwise productive
		// connection (a stray suggest_piece must not cost us in-flight downloads).
		if !fastEnabled && isFastMessage(msg.ID) {
			continue
		}

		switch msg.ID {
		case peer.MsgExtended:
			if reason := handleExtendedMessage(msg.Payload); reason != "" {
				disconnectReason = reason
				break peerLoop
			}

		case peer.MsgChoke:
			// A repeat changes nothing: skip the session lock, the release pass and
			// the forced snapshot, which a peer re-sending chokes would otherwise
			// cost us per 5-byte message. Like any message it still runs pump below.
			if peerChoking {
				break
			}
			peerChoking = true
			if chokeTransitionHook != nil {
				chokeTransitionHook(true)
			}
			now := time.Now()
			s.mu.Lock()
			pState.Choked = true
			s.mu.Unlock()
			// A choked peer won't fulfill our requests; return the in-progress
			// pieces so other peers can grab them. We re-pick on unchoke. Pieces in
			// the peer's allowed_fast set are the exception: BEP 6 lets us keep
			// fetching them while choked, so retain those downloads (and their
			// already-received blocks) instead of restarting them from scratch.
			pipeline.OnChoke(now)
			var retained, released []*activeDownload
			for _, dl := range activeDownloads {
				if fastEnabled && !dl.endgame {
					if _, ok := peerAllowedFast[dl.pieceIndex]; ok {
						retained = append(retained, dl)
						continue
					}
				}
				released = append(released, dl)
			}
			releasePipelineReservations(released, now)
			releaseDownloads(released)
			activeDownloads = retained
			publishPipelineSnapshot(now, true)

		case peer.MsgUnchoke:
			if !peerChoking {
				break // a repeat changes nothing; see MsgChoke
			}
			peerChoking = false
			if chokeTransitionHook != nil {
				chokeTransitionHook(false)
			}
			now := time.Now()
			s.mu.Lock()
			pState.Choked = false
			s.mu.Unlock()
			// A fresh unchoke means the peer is willing to serve again, so clear any
			// pieces it previously rejected — a reject is "not this request now", not
			// a permanent refusal. Without this a single (often transient) reject
			// would bar the piece from this peer for the whole connection.
			clear(peerRejectedPieces)
			if noPickRestricted {
				noPick = false
			}
			pipeline.OnUnchoke(now)
			publishPipelineSnapshot(now, true)

		case peer.MsgInterested:
			// A repeat changes nothing, so skip the write lock and the upload-slot
			// scan of s.Peers: a peer could otherwise hold s.mu for a full map walk
			// with every 5-byte message.
			s.mu.RLock()
			alreadyInterested := pState.Interested
			s.mu.RUnlock()
			if alreadyInterested {
				break
			}
			now := time.Now()
			lastActiveAt = now
			// Unchoking a newly interested peer at once, when fewer than four
			// interested peers are unchoked, takes a walk over every known peer. A
			// peer flipping its interest gets that walk at most once per
			// peerInterestScanInterval; in between it is only marked interested
			// and the choke round decides.
			scan := lastInterestScanAt.IsZero() || now.Sub(lastInterestScanAt) >= peerInterestScanInterval
			s.mu.Lock()
			pState.Interested = true
			if scan && pState.AmChoking {
				lastInterestScanAt = now
				if interestScanHook != nil {
					interestScanHook()
				}
				unchokedInterested := 0
				for _, candidate := range s.Peers {
					if candidate.Active && candidate.Interested && !candidate.AmChoking {
						unchokedInterested++
					}
				}
				if unchokedInterested < 4 {
					pState.AmChoking = false
				}
			}
			amChoking := pState.AmChoking
			s.mu.Unlock()
			applyChoke(amChoking)

		case peer.MsgNotInterested:
			// Only a change takes the write lock (and counts as activity:
			// repeating not-interested must not keep an idle connection alive).
			s.mu.RLock()
			wasInterested := pState.Interested
			s.mu.RUnlock()
			if !wasInterested {
				break
			}
			lastActiveAt = time.Now()
			s.mu.Lock()
			pState.Interested = false
			s.mu.Unlock()

		case peer.MsgHave:
			if len(msg.Payload) == 4 {
				index := binary.BigEndian.Uint32(msg.Payload)
				if numPiecesNow < 0 || uint64(numPiecesNow) > uint64(^uint32(0)) {
					continue
				}
				if index >= uint32(numPiecesNow) {
					continue
				}
				i := int(index)
				if i/8 >= len(peerBitfield) {
					continue
				}
				if !bitfieldHas(peerBitfield, i) {
					setBit(peerBitfield, i)
					s.addPieceAvailability(i)
					noPick = false
				}
			}

		case peer.MsgHaveAll:
			if len(msg.Payload) != 0 {
				continue
			}
			if availabilityReceived {
				break // only the first announcement counts; see availabilityReceived
			}
			availabilityReceived = true
			if numPiecesNow == 0 {
				// Before metadata: remember it and replay once the count is known.
				peerHaveAllPending = true
				continue
			}
			markPeerSeed(numPiecesNow)

		case peer.MsgHaveNone:
			if len(msg.Payload) != 0 {
				continue
			}
			if availabilityReceived {
				break
			}
			availabilityReceived = true
			if numPiecesNow == 0 {
				// The default zeroed bitfield already represents have_none.
				continue
			}
			if bitfieldAny(peerBitfield) {
				// Only reachable if Haves arrived before the announcement.
				setPeerBitfield(make([]byte, (numPiecesNow+7)/8))
			}

		case peer.MsgBitfield:
			if availabilityReceived {
				break
			}
			if numPiecesNow == 0 {
				// Before metadata the length cannot be checked yet. Buffer it (no
				// valid torrent needs more than maxPendingBitfieldLen bytes) and
				// replay it once the piece count is known, as with have_all.
				if len(msg.Payload) == 0 || len(msg.Payload) > maxPendingBitfieldLen {
					continue
				}
				availabilityReceived = true
				pendingBitfield = append([]byte(nil), msg.Payload...)
				continue
			}
			if len(msg.Payload) != (numPiecesNow+7)/8 {
				continue
			}
			availabilityReceived = true
			applyAnnouncedBitfield(msg.Payload, numPiecesNow)

		case peer.MsgSuggestPiece:
			// Advisory only. We still require Have/Bitfield/HaveAll before requesting.
			if len(msg.Payload) != 4 {
				continue
			}

		case peer.MsgAllowedFast:
			if len(msg.Payload) != 4 {
				continue
			}
			index := int64(binary.BigEndian.Uint32(msg.Payload))
			if numPiecesNow == 0 {
				// Before metadata: buffer and validate once the count is known.
				// Deduped and capped at pendingAllowedFastCap (generously above any
				// real allowed-fast set, so legitimate offers survive to replay) so a
				// malicious peer can't grow memory at wire rate before we know how
				// many pieces exist.
				if _, dup := pendingAllowedFastSet[index]; !dup && len(pendingAllowedFast) < pendingAllowedFastCap {
					pendingAllowedFast = append(pendingAllowedFast, index)
					pendingAllowedFastSet[index] = struct{}{}
				}
				continue
			}
			// The same cap applies once metadata is known: the set is scanned by
			// hasAllowedFastWork on every pump while we are choked, so it must not
			// grow to the piece count.
			if index >= 0 && index < int64(numPiecesNow) && len(peerAllowedFast) < pendingAllowedFastCap {
				peerAllowedFast[index] = struct{}{}
				noPick = false
			}

		case peer.MsgRejectRequest:
			if len(msg.Payload) != 12 {
				continue
			}
			index := int64(binary.BigEndian.Uint32(msg.Payload[0:4]))
			begin := int64(binary.BigEndian.Uint32(msg.Payload[4:8]))
			length := int64(binary.BigEndian.Uint32(msg.Payload[8:12]))
			dl := findDownload(index)
			if dl == nil {
				continue
			}
			req, exists := dl.pending[begin]
			if !exists || req.length != length || !req.requested || req.received {
				continue
			}
			abandonRejectedDownload(dl, begin, time.Now())

		case peer.MsgPiece:
			if len(msg.Payload) < 8 {
				continue
			}
			index := int64(binary.BigEndian.Uint32(msg.Payload[0:4]))
			begin := int64(binary.BigEndian.Uint32(msg.Payload[4:8]))
			blockData := msg.Payload[8:]
			now := time.Now()

			// Validate against our outstanding requests: the block must belong to a
			// piece we are downloading from this peer, start on a block boundary, be
			// requested and not yet received, and have the requested length.
			var req *blockRequest
			var blockIndex int64
			valid, duplicate := false, false
			dl := findDownload(index)
			if dl != nil && begin%BlockSize == 0 {
				r, exists := dl.pending[begin]
				switch {
				case exists && r.requested && !r.received:
					req, blockIndex = r, begin/BlockSize
					valid = int64(len(blockData)) == r.length && blockIndex < int64(len(dl.blocks))
				case exists && r.received:
					duplicate = true
				}
			}
			if !valid {
				// Discard it. A peer may still deliver blocks we stopped waiting
				// for (lateAllowance) and repeat a few, but one that keeps sending
				// data we never asked for, more of it than useful data, is a flood.
				if duplicate {
					pipeline.OnDuplicate(int64(len(blockData)), now)
				} else {
					pipeline.OnUnsolicited(int64(len(blockData)), now)
				}
				unsolicitedBytes += int64(len(blockData))
				if unsolicitedBytes > lateAllowance+unsolicitedFloodSlack && unsolicitedBytes > usefulBytes {
					disconnectReason = "unsolicited_flood"
					break peerLoop
				}
				continue
			}

			// Accept the block
			usefulBytes += int64(len(blockData))
			finishRequest(req, requestFinishAccepted, now)
			dl.blocks[blockIndex] = blockData
			// Ownership of the pooled wire buffer passes to this download until the
			// piece is assembled; detach it from the per-iteration release so it is
			// not recycled while blockData still aliases it.
			dl.blockMsgs[blockIndex] = msg
			pooledMsg = nil
			req.received = true
			dl.blocksReceived++
			lastProgressAt = now // forward progress; keeps the stall reaper off
			lastActiveAt = now

			// Counters are bumped lock-free on this hot path; s.mu would
			// otherwise be taken per 16 KB block by every peer goroutine.
			s.Downloaded.Add(int64(len(blockData)))
			atomic.AddInt64(&pState.Downloaded, int64(len(blockData)))

			if dl.blocksReceived != dl.numBlocks {
				break // piece not complete yet; pump tops up at the loop bottom
			}

			// Piece complete: assemble into a pooled buffer and hand it to the async
			// hash/write pool. The peer goroutine keeps draining the socket and
			// requesting instead of stalling on sha1 + WriteBlock + the fast-resume
			// persist. The pool verifies the hash, writes, persists state, returns the
			// piece buffer to the pool, and — on a hash failure — disconnects this peer
			// (via its conn) and returns the piece to the empty pool.
			pieceBuf := s.getPieceBuf(dl.length)
			pieceData := *pieceBuf
			var offset int64
			validPiece := true
			for b := int64(0); b < dl.numBlocks; b++ {
				block := dl.blocks[b]
				if block == nil || offset+int64(len(block)) > int64(len(pieceData)) {
					validPiece = false
					break
				}
				copy(pieceData[offset:], block)
				offset += int64(len(block))
			}
			// The blocks are now copied into pieceData (an independent buffer), so the
			// pooled wire buffers backing them can go back to the inbound pool.
			for b := int64(0); b < dl.numBlocks; b++ {
				dl.blockMsgs[b].Release()
				dl.blockMsgs[b] = nil
			}

			pieceIdx := dl.pieceIndex
			pieceHash := dl.hash
			// The block data has been copied into pieceData; drop the reference to
			// the (now empty) block slice so the removed activeDownload doesn't pin
			// it for the life of the connection (e.g. an idle seeding conn, #63).
			dl.blocks = nil
			removeDownload(dl.pieceIndex)

			if !validPiece || offset != int64(len(pieceData)) {
				// Assembly invariant violated (shouldn't happen): return both buffers.
				s.putPieceBuf(pieceBuf)
				s.mu.Lock()
				if pieceIdx >= 0 && pieceIdx < int64(len(s.PieceStates)) && s.PieceStates[pieceIdx] == PieceDownloading {
					s.setPieceStateLocked(int(pieceIdx), PieceEmpty)
				}
				s.mu.Unlock()
				break
			}

			s.ensurePieceWritePool()
			writeQueueStarted := time.Now()
			select {
			case s.pieceWriteCh <- pieceWriteJob{index: pieceIdx, hash: pieceHash, data: pieceData, pieceBuf: pieceBuf, conn: conn, source: source}:
				if blocked := time.Since(writeQueueStarted); blocked > 10*time.Millisecond {
					pipeline.OnWriterLimited(time.Now())
					publishPipelineSnapshot(time.Now(), true)
				}
			case <-s.ctx.Done():
				s.putPieceBuf(pieceBuf)
				return
			}

		case peer.MsgRequest:
			if len(msg.Payload) == 12 {
				index := int64(binary.BigEndian.Uint32(msg.Payload[0:4]))
				begin := int64(binary.BigEndian.Uint32(msg.Payload[4:8]))
				length := int64(binary.BigEndian.Uint32(msg.Payload[8:12]))

				s.mu.RLock()
				paused := s.paused
				numPieces := len(s.PieceStates)
				amChoking := pState.AmChoking
				var isCompleted bool
				var pieceLen int64
				if index >= 0 && index < int64(numPieces) {
					isCompleted = s.PieceStates[index] == PieceCompleted
					pieceLen = s.Storage.PieceLength(index)
				}
				s.mu.RUnlock()
				applyChoke(amChoking)
				_, requestAllowedFast := allowedFastForPeer[index]

				if paused || (amChoking && !requestAllowedFast) {
					if fastEnabled && length > 0 {
						_ = client.SendRejectRequest(uint32(index), uint32(begin), uint32(length))
					}
					continue
				}

				if isCompleted && length > 0 && length <= BlockSize && begin >= 0 && begin+length <= pieceLen {
					// While we choke the peer, an allowed-fast piece may be fetched only a
					// few times over (libtorrent's mitigation): otherwise a choked peer
					// could re-download its fast set forever, bypassing the choker.
					if amChoking {
						if allowedFastServed[index] >= allowedFastServeRounds*((pieceLen+BlockSize-1)/BlockSize) {
							_ = client.SendRejectRequest(uint32(index), uint32(begin), uint32(length))
							continue
						}
						if len(uploadQueue) < maxUploadQueue {
							allowedFastServed[index]++
						}
					}
					// Queue the block for upload rather than blocking on the limiter here:
					// waiting for upload tokens inside the message loop would stop this
					// goroutine running pump(), stalling the download side (issue #59).
					// uploadPump (below, after every message) serves the queue as tokens
					// accrue. A full queue means the peer is asking faster than the upload
					// limit allows; reject (fast-extension) or drop as backpressure so a
					// greedy peer can't grow the queue without bound.
					if len(uploadQueue) < maxUploadQueue {
						uploadQueue = append(uploadQueue, uploadRequest{index: index, begin: begin, length: length})
					} else if fastEnabled {
						_ = client.SendRejectRequest(uint32(index), uint32(begin), uint32(length))
					}
				} else if fastEnabled && length > 0 {
					_ = client.SendRejectRequest(uint32(index), uint32(begin), uint32(length))
				}
			}

		case peer.MsgCancel:
			// Drop the cancelled block from the upload queue so it is not read from
			// disk and sent anyway. Under BEP 6 a fast peer gets a reject for it.
			if len(msg.Payload) != 12 {
				continue
			}
			index := int64(binary.BigEndian.Uint32(msg.Payload[0:4]))
			begin := int64(binary.BigEndian.Uint32(msg.Payload[4:8]))
			length := int64(binary.BigEndian.Uint32(msg.Payload[8:12]))
			for i, r := range uploadQueue {
				if r.index == index && r.begin == begin && r.length == length {
					uploadQueue = append(uploadQueue[:i], uploadQueue[i+1:]...)
					if fastEnabled {
						_ = client.SendRejectRequest(uint32(index), uint32(begin), uint32(length))
					}
					break
				}
			}

		case peer.MsgPort:
			// BEP 5: the peer advertises its DHT UDP port. Combine it with the
			// peer's source IP and feed it into the routing table so live peers
			// grow our DHT beyond bootstrap nodes and lookups.
			//
			// Each AddNode scans the routing table under the DHT lock, so only act on
			// the first PORT and on real changes, a few times per connection at most;
			// repeats cost nothing.
			if len(msg.Payload) != 2 {
				break
			}
			dhtPort := binary.BigEndian.Uint16(msg.Payload)
			if dhtPort == 0 || dhtPort == peerDHTPort || peerDHTPortUpdates >= maxPeerDHTPortUpdates {
				break
			}
			s.mu.RLock()
			d := s.DHT
			allowDHT := s.allowsDecentralizedPeerDiscoveryLocked()
			s.mu.RUnlock()
			if allowDHT && d != nil {
				if pip := net.ParseIP(ip); pip != nil {
					peerDHTPort = dhtPort
					peerDHTPortUpdates++
					addDHTNode(d, pip, dhtPort)
				}
			}
		}

		// Cancel and drop pieces another peer finished (endgame), then keep the
		// request pipeline full across all active pieces, opening new pieces as
		// needed (pump no-ops when paused, choked, or seeding). Also serve any block
		// requests this peer queued (e.g. a MsgRequest just handled above), draining
		// them without blocking so upload limiting never stalls the download pump.
		dropCompletedElsewhere()
		scheduleRateRetry(minRetry(pump(), uploadPump()))
	}

	// If we disconnected while holding pieces, return them to empty so other
	// peers can fetch them.
	now := time.Now()
	releasePipelineReservations(activeDownloads, now)
	publishPipelineSnapshot(now, true)
	releaseDownloads(activeDownloads)
}

// GetActivePeers returns a slice of active peer states for TUI updates.
func (s *Session) GetActivePeers() []PeerState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var list []PeerState
	for _, p := range s.Peers {
		if p.Active && !p.WebSeed {
			// Build the snapshot field-by-field rather than copying *p: the
			// Downloaded/Uploaded counters are written lock-free on the peer hot
			// path, so a whole-struct copy would race them (the copy reads those
			// words non-atomically). Every other field is guarded by s.mu, held
			// here; the two counters are loaded atomically.
			list = append(list, PeerState{
				IP:            p.IP,
				Port:          p.Port,
				Choked:        p.Choked,
				Interested:    p.Interested,
				DownloadSpeed: p.DownloadSpeed,
				UploadSpeed:   p.UploadSpeed,
				Downloaded:    atomic.LoadInt64(&p.Downloaded),
				Uploaded:      atomic.LoadInt64(&p.Uploaded),
				Active:        p.Active,
				AmChoking:     p.AmChoking,
				LastAttempt:   p.LastAttempt,
				Dialable:      p.Dialable,
				Dialing:       p.Dialing,

				WindowBlocks:         p.WindowBlocks,
				TargetWindowBlocks:   p.TargetWindowBlocks,
				OutstandingBlocks:    p.OutstandingBlocks,
				OutstandingBytes:     p.OutstandingBytes,
				PipelineQueueSeconds: p.PipelineQueueSeconds,
				PipelineRTT:          p.PipelineRTT,
				PipelineRate:         p.PipelineRate,
				TimeoutRate:          p.TimeoutRate,
				AppLimited:           p.AppLimited,
				BudgetLimited:        p.BudgetLimited,
				PieceCapLimited:      p.PieceCapLimited,
				WriterLimited:        p.WriterLimited,
			})
		}
	}
	return list
}

// UploadPeerStats summarizes whether connected peers currently want data from us.
type UploadPeerStats struct {
	Connected  int
	Interested int
	Unchoked   int
}

func (s *Session) GetUploadPeerStats() UploadPeerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var stats UploadPeerStats
	for _, p := range s.Peers {
		if !p.Active || p.WebSeed {
			continue
		}
		stats.Connected++
		if p.Interested {
			stats.Interested++
			if !p.AmChoking {
				stats.Unchoked++
			}
		}
	}
	return stats
}

// DHT lookup cadence (see dhtLoop). Every torrent used to look up 1 s after
// start and every 30 s after that, seeding or not, so a restart with many
// torrents sent hundreds of lookups in the same second, again every 30 s in
// lockstep, and libtorrent nodes' DoS blockers began ignoring us. Vars so tests
// can shorten them; treat them as constants.
var (
	// dhtFirstLookupDelay is how soon a session first looks up its torrent. A
	// magnet fetching metadata does so right then; any other session takes the
	// next free first-lookup slot, dhtStartupSpreadStep after the one before,
	// at most dhtStartupSpreadMax away (see dhtFirstLookupAfter), so restored
	// torrents start, and stay, out of phase.
	dhtFirstLookupDelay  = time.Second
	dhtStartupSpreadStep = 250 * time.Millisecond
	dhtStartupSpreadMax  = time.Minute
	// Intervals between lookups, each jittered by ±20%: while fetching
	// metadata; while downloading with fewer than dhtWellConnectedPeers
	// connections; and while seeding or well connected, when the swarm keeps
	// finding us anyway (libtorrent's dht_announce_interval is 15 minutes).
	dhtMetadataLookupInterval = 30 * time.Second
	dhtDownloadLookupInterval = time.Minute
	dhtIdleLookupInterval     = 15 * time.Minute
	// dhtResumeLookupDelay is how soon after a resume the next lookup runs,
	// plus up to as long again at random.
	dhtResumeLookupDelay = time.Second
)

// dhtWellConnectedPeers is the connection count from which a downloading
// session looks up as rarely as a seed.
const dhtWellConnectedPeers = 50

// dhtFirstLookupSlots hands out first-lookup times (see dhtFirstLookupAfter):
// next is the earliest time the next session may take.
var dhtFirstLookupSlots struct {
	mu   sync.Mutex
	next time.Time
}

// startDHTLookup starts a get_peers lookup for infoHash, announcing peerPort
// when announce is set. A var so tests can observe lookups.
var startDHTLookup = func(d *dht.DHT, infoHash [20]byte, peerPort uint16, announce bool) {
	d.LookupWithOptions(infoHash, peerPort, dht.LookupOptions{Announce: announce})
}

// dhtLoop looks the torrent up on the DHT on the cadence dhtLookupIntervalLocked
// picks, and soon after a resume.
func (s *Session) dhtLoop() {
	defer s.wg.Done()
	defer s.crashGuard("dht")()

	s.mu.RLock()
	metadataMode := s.metadataMode
	s.mu.RUnlock()
	timer := time.NewTimer(dhtFirstLookupAfter(metadataMode, time.Now()))
	defer timer.Stop()
	for {
		_, pauseChanged := s.pauseStateSignal()
		select {
		case <-timer.C:
			timer.Reset(s.dhtLookupTick())
		case <-pauseChanged:
			// Lookups are skipped while paused; after a resume the next one runs
			// soon instead of up to a whole interval later.
			if paused, _ := s.pauseStateSignal(); !paused {
				timer.Reset(dhtResumeLookupDelay + randDuration(dhtResumeLookupDelay))
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// dhtLookupTick runs one lookup unless the session is paused or cannot use
// peers, and returns the delay until the next tick.
func (s *Session) dhtLookupTick() time.Duration {
	s.mu.RLock()
	interval, skip := s.dhtLookupIntervalLocked()
	d := s.DHT
	peerPort := s.Port
	hasInbound := s.hasInboundListenerLocked()
	hasTorrent := s.Torrent != nil
	allowAnnounce := s.allowsDHTAnnounceLocked()
	var infoHash [20]byte
	if hasTorrent {
		infoHash = s.Torrent.InfoHash
	}
	s.mu.RUnlock()

	if !skip && d != nil && hasTorrent && hasInbound {
		startDHTLookup(d, infoHash, peerPort, allowAnnounce)
	}
	return jitterDHTInterval(interval)
}

// dhtLookupIntervalLocked returns the time from one DHT lookup to the next,
// and whether to skip lookups for now (paused, or metadata-stalled), in which
// case it is when to check again. Caller holds s.mu (read or write).
func (s *Session) dhtLookupIntervalLocked() (interval time.Duration, skip bool) {
	switch {
	case s.paused || s.metadataStalledLocked():
		return dhtMetadataLookupInterval, true
	case s.metadataMode:
		return dhtMetadataLookupInterval, false
	case s.isCompletedLocked() || len(s.activePeers) >= dhtWellConnectedPeers:
		return dhtIdleLookupInterval, false
	default:
		return dhtDownloadLookupInterval, false
	}
}

// dhtFirstLookupAfter is the delay from now before a session's first DHT
// lookup. Sessions that start together (a restore) take consecutive slots
// dhtStartupSpreadStep apart, each at a random point within its slot, so their
// first lookups spread over dhtStartupSpreadStep per session; past
// dhtStartupSpreadMax a session picks a random point in that window instead. A
// session started on its own (a torrent added later) finds the slots drained
// and looks up after dhtFirstLookupDelay, however many sessions run: sizing
// the spread by the running sessions made a new download in a large library
// wait up to a minute for its first DHT peers.
func dhtFirstLookupAfter(metadataMode bool, now time.Time) time.Duration {
	if metadataMode {
		return dhtFirstLookupDelay
	}
	first := now.Add(dhtFirstLookupDelay)
	dhtFirstLookupSlots.mu.Lock()
	defer dhtFirstLookupSlots.mu.Unlock()
	slot := first
	if dhtFirstLookupSlots.next.After(slot) {
		slot = dhtFirstLookupSlots.next
	}
	if slot.Sub(first) >= dhtStartupSpreadMax {
		return dhtFirstLookupDelay + randDuration(dhtStartupSpreadMax)
	}
	dhtFirstLookupSlots.next = slot.Add(dhtStartupSpreadStep)
	return slot.Sub(now) + randDuration(dhtStartupSpreadStep)
}

// jitterDHTInterval spreads d uniformly over ±20%, so sessions that started
// together drift apart.
func jitterDHTInterval(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

// randDuration returns a uniformly random duration in [0, d), or 0 when d <= 0.
func randDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return rand.N(d)
}

// AddPeerFromDiscovery adds a peer learned via a decentralized discovery mechanism
// (DHT, PEX) and attempts to initiate a connection. Discovery peers are suppressed
// for private torrents (BEP 27), which must use trackers only.
func (s *Session) AddPeerFromDiscovery(peerAddr string) {
	s.addPeer(peerAddr, true)
}

// addPeer records peerAddr and, when eligible, dials it. fromDiscovery marks peers
// learned via decentralized discovery (DHT/PEX); those are rejected for private
// torrents. Reconnecting an already-known peer (e.g. after a resume) passes
// fromDiscovery=false, so a private torrent can still re-establish its
// tracker-sourced connections, but not ones DHT/PEX supplied before a magnet's
// metadata showed it is private.
func (s *Session) addPeer(peerAddr string, fromDiscovery bool) {
	host, portStr, err := net.SplitHostPort(peerAddr)
	if err != nil {
		return
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return
	}
	if port <= 0 || port > 65535 {
		return
	}

	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() {
		return
	}

	s.mu.Lock()
	if s.paused || s.closed || !s.started {
		s.mu.Unlock()
		return
	}
	if fromDiscovery && !s.allowsDecentralizedPeerDiscoveryLocked() {
		s.mu.Unlock()
		return
	}

	pState, exists := s.Peers[peerAddr]
	if exists && s.privateRefusesDialLocked(pState) {
		s.mu.Unlock()
		return
	}
	var source PeerSource
	if fromDiscovery {
		source = PeerSourceDiscovery
	}
	now := time.Now()
	var shouldDial bool
	if !exists {
		shouldDial = true
	} else {
		// Discovery supplies a listening endpoint, so an inbound-only entry with the
		// same address becomes eligible for maintenance retries.
		pState.Dialable = true
		pState.Source |= source
		if fromDiscovery {
			pState.seenAt = now
		}
		if !pState.Active && !pState.Dialing && now.Sub(pState.LastAttempt) > pState.redialBackoff() {
			shouldDial = true
		}
	}

	// Never dial a refused address. Don't exceed the outbound connection cap, and
	// dial nobody while the metadata cannot be used; a new peer is recorded then
	// (with no LastAttempt), so maintenance dials it once that changes instead of
	// losing it until DHT or PEX happen to list it again.
	if shouldDial && s.refusesDialLocked(peerAddr, host) {
		shouldDial = false
	} else if shouldDial && (len(s.outboundSlots) >= maxOutboundPeers || s.metadataStalledLocked()) {
		shouldDial = false
		if !exists {
			s.prunePeersLocked()
			s.Peers[peerAddr] = &PeerState{
				IP:        host,
				Port:      uint16(port),
				AmChoking: true,
				Choked:    true,
				Dialable:  true,
				Source:    source,
				seenAt:    now,
			}
		}
	}

	if shouldDial {
		if !exists {
			s.prunePeersLocked()
			s.Peers[peerAddr] = &PeerState{
				IP:          host,
				Port:        uint16(port),
				AmChoking:   true,
				Choked:      true,
				LastAttempt: now,
				Dialable:    true,
				Dialing:     true,
				Source:      source,
				seenAt:      now,
			}
		} else {
			s.Peers[peerAddr].LastAttempt = now
			s.Peers[peerAddr].Dialing = true
		}
		s.wg.Add(1)
		go func(tp tracker.Peer) {
			defer s.wg.Done()
			s.connectToPeer(tp)
		}(tracker.Peer{IP: ip, Port: uint16(port)})
	}
	logEnabled := fromDiscovery && logging.Enabled()
	var infoHash, name string
	if logEnabled {
		infoHash, name = s.logIdentityLocked()
	}
	s.mu.Unlock()
	if logEnabled {
		logging.Debug("peer_discovered",
			logging.String("info_hash", infoHash),
			logging.String("name", name),
			logging.String("peer", peerAddr),
			logging.Bool("dial_scheduled", shouldDial),
		)
	}
}

// AttachDHT dynamically associates a DHT client and starts the dhtLoop if the session is running.
func (s *Session) AttachDHT(d *dht.DHT) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	if s.closed || s.DHT != nil || d == nil || !s.allowsDecentralizedPeerDiscoveryLocked() {
		s.mu.Unlock()
		return
	}
	s.DHT = d
	shouldStart := s.started && !s.closed
	s.mu.Unlock()

	if shouldStart {
		s.wg.Add(1)
		go s.dhtLoop()
	}
}

func (s *Session) attachUTPSocket(socket *utp.Socket) {
	if socket == nil {
		return
	}
	s.mu.Lock()
	if !s.closed {
		s.utpSocket = socket
	}
	s.mu.Unlock()
}

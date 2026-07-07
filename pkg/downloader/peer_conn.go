package downloader

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sainttorrent/pkg/dht"
	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/tracker"
	"sainttorrent/pkg/utp"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

// peerStallTimeout bounds how long an outbound peer may hold its connection slot
// without delivering a single block of data we want. A connection's slot is held
// for the whole life of its read loop, and a peer that chokes us forever — or
// trickles only keep-alives/Have messages — resets the socket read deadline
// without ever giving us data, so without this it would occupy a slot indefinitely.
// In a slow swarm those dead-weight connections accumulate until the (shared,
// manager-wide) outbound pool is full and NO session can dial a fresh peer, which
// flatlines every torrent at once until a restart clears the pools. Any single
// received block resets the timer, so a genuinely-slow-but-working peer survives;
// only one delivering < one block per peerStallTimeout (≈273 B/s at 60 s) is
// reaped, freeing the slot for a productive peer. Reaping never applies while we
// are seeding (we want no data) — see the reaper in runPeerMessageLoop.
// A var (not const) so tests can shorten it; treat it as a constant in production.
var peerStallTimeout = 60 * time.Second

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
// is set generously (mainline/libtorrent use ~200 per torrent). An attacker cannot
// occupy these slots — they are only ever filled by peers we chose to connect to.
const maxOutboundPeers = 200

// maxInboundPeers bounds how many incoming peer connections a session accepts at once.
// The listen port is public (announced to trackers/DHT), so this is the real abuse
// surface: the cap stops a flood of inbound connections from exhausting file descriptors.
// It is a SEPARATE budget from outbound, so an inbound flood can never starve downloads.
const maxInboundPeers = 100

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

// prunePeersLocked evicts inactive known-peer entries when the Peers map grows past
// maxKnownPeers, oldest-attempt-first; active peers are never evicted. Caller holds s.mu.
func (s *Session) prunePeersLocked() {
	if len(s.Peers) <= maxKnownPeers {
		return
	}
	type agedPeer struct {
		addr string
		at   time.Time
	}
	inactive := make([]agedPeer, 0, len(s.Peers))
	for addr, ps := range s.Peers {
		if ps.Active {
			continue
		}
		inactive = append(inactive, agedPeer{addr: addr, at: ps.LastAttempt})
	}
	sort.Slice(inactive, func(i, j int) bool {
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

func (s *Session) markPeerAttemptFailed(peerAddr string) {
	s.mu.Lock()
	if ps, ok := s.Peers[peerAddr]; ok {
		ps.Active = false
		ps.LastAttempt = time.Now()
	}
	s.mu.Unlock()
}

func (s *Session) broadcastHave(index uint32) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.mu.RLock()
	if s.closed || s.ctx.Err() != nil {
		s.mu.RUnlock()
		return
	}
	var clients []*peer.Client
	for _, client := range s.activePeers {
		clients = append(clients, client)
	}
	s.mu.RUnlock()

	for _, client := range clients {
		s.wg.Add(1)
		go func(c *peer.Client) {
			defer s.wg.Done()
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			_ = c.SendHave(index)
		}(client)
	}
}

// peerMaintenanceLoop periodically refills the outbound connection set from the
// known-peer map, so a slot freed by a dropped or reaped peer is reused promptly
// instead of waiting for the next tracker announce (up to an hour out) or DHT
// lookup. This is what keeps a slow swarm churning toward productive peers rather
// than wedging at zero once the initial connections go stale.
func (s *Session) peerMaintenanceLoop() {
	defer s.wg.Done()
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
	if s.isCompletedLocked() {
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
	for _, ps := range s.Peers {
		if slotsHeld+launched >= maxOutboundPeers || launched >= globalRoom {
			break
		}
		// Skip connected peers, attempts already in flight, and inbound-only source
		// endpoints whose ports were never advertised as listening ports.
		if ps.Active || ps.Dialing || !ps.Dialable {
			continue
		}
		// Eligible to (re)dial once the backoff has elapsed. A zero LastAttempt means
		// "dial now" (e.g. Resume clears it on every inactive peer); the dedup against a
		// concurrent dial is the LastAttempt = now set below, under the lock.
		if !ps.LastAttempt.IsZero() && now.Sub(ps.LastAttempt) <= peerRedialBackoff {
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
	peerAddr := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)
	s.mu.RLock()
	dialPauseEpoch := s.pauseEpoch
	s.mu.RUnlock()
	acquiredSlots := false
	defer func() {
		s.mu.Lock()
		if ps, ok := s.Peers[peerAddr]; ok && ps.Dialing {
			ps.Dialing = false
			// A full per-session or manager-wide pool means no network attempt was
			// made. Keep the peer immediately eligible instead of burning a full
			// redial backoff because a lock-free capacity hint raced another session.
			resumedDuringDial := s.pauseEpoch != dialPauseEpoch && !s.paused && !s.closed
			if (!acquiredSlots || resumedDuringDial) && !ps.Active {
				ps.LastAttempt = time.Time{}
			}
		}
		s.mu.Unlock()
	}()

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

	conn, err := s.dialPeer(peerAddr)
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
	tunePeerConn(conn)

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

	_ = conn.SetDeadline(time.Now().Add(peerHandshakeTimeout))
	conn, err = s.negotiateOutgoingPeerConn(peerAddr, conn, connMonitor)
	if err != nil {
		s.markPeerAttemptFailed(peerAddr)
		if logging.Enabled() {
			logging.Debug("peer_negotiation_failed",
				logging.String("peer", peerAddr),
				logging.Err(err),
			)
		}
		return
	}
	connMonitor.set(conn)
	defer conn.Close()

	// Handshake with deadline
	_ = conn.SetDeadline(time.Now().Add(peerHandshakeTimeout))
	client := peer.NewClient(conn, s.Torrent.InfoHash, s.PeerID)
	s.mu.RLock()
	client.DisableDHT = !s.allowsDecentralizedPeerDiscoveryLocked()
	s.mu.RUnlock()
	handshake, err := client.Handshake()
	if err != nil {
		s.markPeerAttemptFailed(peerAddr)
		if logging.Enabled() {
			logging.Debug("peer_handshake_failed",
				logging.String("peer", peerAddr),
				logging.Err(err),
			)
		}
		return
	}
	_ = conn.SetDeadline(time.Time{}) // clear deadline

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
	}
	s.mu.Unlock()

	s.runPeerMessageLoop(client, conn, peerAddr, p.IP.String(), p.Port, handshake.Reserved, true)
}

func (s *Session) dialPeer(peerAddr string) (net.Conn, error) {
	// Transport policy: prefer TCP for existing swarm compatibility, then fall
	// back to uTP on the same endpoint. Race both when uTP is available so a
	// firewalled TCP path does not hold the bounded outbound slot for two full
	// dial timeouts before uTP gets a chance.
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()

	s.mu.RLock()
	udpSocket := s.utpSocket
	s.mu.RUnlock()

	dialCount := 1
	if udpSocket != nil {
		dialCount = 2
	}
	results := make(chan transportDialResult, dialCount)

	go func() {
		dialer := net.Dialer{}
		conn, err := dialer.DialContext(ctx, "tcp", peerAddr)
		results <- transportDialResult{transport: "tcp", conn: conn, err: err}
	}()

	if udpSocket == nil {
		res := <-results
		return res.conn, res.err
	}

	go func() {
		conn, err := udpSocket.DialContext(ctx, peerAddr)
		results <- transportDialResult{transport: "utp", conn: conn, err: err}
	}()

	var errs []error
	for i := 0; i < dialCount; i++ {
		res := <-results
		if res.err == nil {
			cancel()
			go closeLateDialSuccesses(results, dialCount-i-1)
			return res.conn, nil
		}
		errs = append(errs, fmt.Errorf("%s dial failed: %w", res.transport, res.err))
	}
	return nil, errors.Join(errs...)
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
func (s *Session) inboundListenerLoop() {
	defer s.wg.Done()

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

func (s *Session) handleIncomingConnection(conn net.Conn) {
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
	tunePeerConn(conn)

	s.mu.RLock()
	paused := s.paused
	closed := s.closed
	s.mu.RUnlock()
	if paused || closed {
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
	fastEnabled := peer.SupportsFastExtension(peerReserved)
	direction := "inbound"
	if outbound {
		direction = "outbound"
	}

	s.mu.Lock()
	if s.paused || s.closed {
		s.mu.Unlock()
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
	}
	// An outbound connection confirms this is a listening endpoint. An inbound
	// connection does not erase prior tracker/DHT evidence for the same endpoint.
	if outbound {
		pState.Dialable = true
	}
	pState.Active = true
	s.activePeers[peerAddr] = client
	if logEnabled {
		logInfoHash, logName = s.logIdentityLocked()
	}
	s.mu.Unlock()
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
		if activeClient, active := s.activePeers[peerAddr]; active && activeClient == client {
			if ps, ok := s.Peers[peerAddr]; ok {
				ps.Active = false
				ps.Choked = true
				if s.pauseEpoch != connectionPauseEpoch && !s.paused && !s.closed {
					ps.LastAttempt = time.Time{}
					reconnectAfterResume = true
				} else {
					ps.LastAttempt = time.Now()
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
	// Fast messages that arrive before we have metadata reference piece indices we
	// cannot validate yet; remember them and replay once the piece count is known
	// (a seed sends have_all exactly once, right after the handshake — well before
	// a magnet transfer has finished fetching metadata).
	peerHaveAllPending := false
	var pendingAllowedFast []int64

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
		infoLen := len(s.Torrent.InfoBytes)
		extensions := s.extensionHandshakeMapLocked()
		s.mu.RUnlock()
		_ = client.SendExtHandshakeWithExtensions(extensions, infoLen)
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
	// Drop this peer's contribution to swarm piece availability on exit. peerBitfield
	// accumulates exactly the pieces we counted (bitfield delta + Haves), so the
	// closure reads its final value here. (#7, rarest-first.)
	defer func() { s.removePeerAvailability(peerBitfield) }()

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

	type requestFinishReason int
	const (
		requestFinishAccepted requestFinishReason = iota
		requestFinishTimeout
		requestFinishCancel
		requestFinishAbandon
	)

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
		for i, dl := range activeDownloads {
			if dl.pieceIndex == index {
				releaseDownloadBuffers(dl)
				activeDownloads = append(activeDownloads[:i], activeDownloads[i+1:]...)
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
		case requestFinishCancel, requestFinishAbandon:
			pipeline.OnCancel(req, now)
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
	pexAdvertised := make(map[string]struct{})
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
		oldBF := append([]byte(nil), peerBitfield...)
		peerBitfield = newBF
		s.applyBitfieldAvailability(oldBF, peerBitfield)
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

	// openNewPiece claims the highest-priority, rarest empty wanted piece this peer has
	// and marks it PieceDownloading. In endgame (no fresh pieces left to claim) it
	// instead returns a redundant copy of an in-progress piece this peer has, leaving
	// that piece's state owned by the original downloader. Returns nil when the peer
	// has nothing left for us.
	openNewPiece := func(canRequestPiece func(int64) bool) *activeDownload {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.paused || s.closed {
			return nil
		}
		endgame := false
		bestIdx := s.selectNeededPieceLocked(canRequestPiece)
		if bestIdx == -1 {
			if s.endgameActiveLocked() {
				owned := make(map[int64]bool, len(activeDownloads))
				for _, dl := range activeDownloads {
					owned[dl.pieceIndex] = true
				}
				bestIdx = s.selectEndgamePieceLocked(canRequestPiece, owned)
				endgame = true
			}
			if bestIdx == -1 {
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
		for _, dl := range activeDownloads {
			if requestable(dl) {
				return true
			}
		}
		if len(activeDownloads) >= pieceCap {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.hasSelectableNeededPieceLocked(canRequestPiece) {
			return true
		}
		if s.endgameActiveLocked() {
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

	// uploadQueue holds this peer's block requests awaiting upload bandwidth. It is
	// owned by this peer goroutine and drained FIFO by uploadPump; never touched by
	// other goroutines, so it needs no lock.
	var uploadQueue []uploadRequest

	// pump re-arms timed-out requests, then fills the request window across all
	// active pieces, opening new pieces as needed. Called after each inbound
	// message — INCLUDING keep-alives — so the pipeline stays full across piece
	// boundaries and, crucially, so the timeout sweep below still runs when a peer
	// that already took our requests goes quiet but keeps the socket warm with
	// keep-alives (which otherwise reset the read deadline and skipped pump).
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
		// set is exhausted doesn't run the full piece scan on every message.
		if choked && !hasAllowedFastWork() {
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
				if len(activeDownloads) >= pieceCap {
					pipeline.OnPieceCapLimited(now)
					break
				}
				newDL := openNewPiece(canRequestPiece)
				if newDL == nil {
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
			// Lock-free counter update (see the download hot path above).
			s.Uploaded.Add(r.length)
			atomic.AddInt64(&pState.Uploaded, r.length)
			uploadQueue = uploadQueue[1:]
		}
		return 0
	}

	// dropCompletedElsewhere is the endgame "cancel on receipt" path: it drops any
	// in-progress piece that another peer has finished (so its state is no longer
	// PieceDownloading) and sends a Cancel for each of our still-outstanding blocks so
	// the peer stops feeding us data the swarm no longer needs. Bounded by
	// the adaptive per-peer piece cap, so it is cheap to run every message.
	dropCompletedElsewhere := func() {
		if len(activeDownloads) == 0 {
			return
		}
		now := time.Now()
		var finished []int64
		s.mu.RLock()
		for _, dl := range activeDownloads {
			if dl.pieceIndex < 0 || dl.pieceIndex >= int64(len(s.PieceStates)) ||
				s.PieceStates[dl.pieceIndex] != PieceDownloading {
				finished = append(finished, dl.pieceIndex)
			}
		}
		s.mu.RUnlock()
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

	type peerReadResult struct {
		msg *peer.Message
		err error
	}
	readCh := make(chan peerReadResult, 1)
	readDone := make(chan struct{})
	readStop := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			msg, err := client.ReadMessage()
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
peerLoop:
	for {
		pooledMsg.Release()
		pooledMsg = nil

		s.mu.RLock()
		paused := s.paused
		s.mu.RUnlock()
		if paused {
			disconnectReason = "paused"
			break
		}

		// Reap an unproductive peer: drop a connection that hasn't delivered a block
		// within peerStallTimeout so its outbound slot can be reused for a peer that
		// will. Gated by cheap checks that keep the O(pieces) completion check off the
		// per-message hot path: only OUTBOUND connections (an inbound peer holds an
		// inbound slot, not an outbound one, and is keyed by an ephemeral port we can't
		// redial — reaping it just drops a productive uploader). A recently issued
		// request also grants a fresh timeout window, which prevents an intentionally
		// slow limiter wait from making the request look stale before it is sent.
		lastUsefulAt := lastProgressAt
		if lastRequestAt.After(lastUsefulAt) {
			lastUsefulAt = lastRequestAt
		}
		if outbound && !waitingForBandwidth && time.Since(lastUsefulAt) > peerStallTimeout {
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
						logging.Duration("idle", time.Since(lastUsefulAt)),
					)
				}
				break
			}
		}

		var msg *peer.Message
		select {
		case result := <-readCh:
			if result.err != nil {
				disconnectReason = "read_error"
				disconnectErr = result.err
				break peerLoop
			}
			msg = result.msg
			pooledMsg = msg // release its pooled buffer at the top of the next iteration
		case <-pexTick:
			sendPEXDelta()
			continue
		case <-rateRetry:
			rateRetry = nil
			// The timer covers whichever pump was waiting on bandwidth: re-run both the
			// download request pump and the upload serve pump, then re-arm for the sooner.
			scheduleRateRetry(minRetry(pump(), uploadPump()))
			continue
		case <-s.ctx.Done():
			disconnectReason = "context_cancelled"
			disconnectErr = s.ctx.Err()
			break peerLoop
		}

		if msg == nil {
			// Keep alive: still run pump so outstanding requests to a now-silent peer
			// time out (and the peer is dropped after its retry budget) instead of the
			// keep-alive merely resetting the read deadline and stalling forever. Also
			// drain any queued uploads that have since accrued bandwidth.
			scheduleRateRetry(minRetry(pump(), uploadPump()))
			continue
		}

		s.mu.RLock()
		inMetaNow := s.metadataMode
		numPiecesNow := len(s.PieceStates)
		s.mu.RUnlock()

		if !inMetaNow && !initializedPeersAndBitfield {
			// Initialize now that metadata is downloaded!
			sendInitialPeerState()

			peerBitfield = make([]byte, (numPiecesNow+7)/8)
			initializedPeersAndBitfield = true

			// Replay any fast-extension availability the peer announced before we had
			// metadata (have_none is the default zero bitfield, so nothing to do).
			if peerHaveAllPending && numPiecesNow > 0 {
				setPeerBitfield(fullPieceBitfield(numPiecesNow))
			}
			peerHaveAllPending = false
			for _, idx := range pendingAllowedFast {
				if idx >= 0 && idx < int64(numPiecesNow) {
					peerAllowedFast[idx] = struct{}{}
				}
			}
			pendingAllowedFast = nil
		}

		// A peer that never negotiated the fast extension shouldn't be sending its
		// messages; ignore them rather than tearing down an otherwise productive
		// connection (a stray suggest_piece must not cost us in-flight downloads).
		if !fastEnabled && isFastMessage(msg.ID) {
			continue
		}

		switch msg.ID {
		case peer.MsgExtended:
			if len(msg.Payload) < 2 {
				continue
			}
			extMsgID := msg.Payload[0]
			payloadBytes := msg.Payload[1:]

			if extMsgID == peer.ExtHandshake {
				hs, err := peer.ParseExtensionHandshake(payloadBytes)
				if err == nil {
					if utPexID, ok := hs.Extensions[peer.ExtNamePEX]; ok && s.pexEnabled() {
						peerUtPexID = utPexID
						startPEX()
					}
					if utID, ok := hs.Extensions[peer.ExtNameMetadata]; ok {
						peerUtMetadataID = utID

						// If we are in metadata mode, request the metadata blocks
						s.mu.Lock()
						inMetaMode := s.metadataMode
						metadataComp := s.metadataCompleted
						sz := s.metadataSize
						s.mu.Unlock()

						if inMetaMode && !metadataComp {
							if hs.MetadataSize <= 0 || hs.MetadataSize > peer.MaxMetadataSize {
								s.mu.Lock()
								s.lastErr = fmt.Errorf("invalid metadata size from peer: %d", hs.MetadataSize)
								s.mu.Unlock()
								continue
							}
							s.mu.Lock()
							if s.metadataSize == 0 {
								s.metadataSize = hs.MetadataSize
								s.metadataBuf = make([]byte, hs.MetadataSize)
								numBlocks := (hs.MetadataSize + peer.MetadataBlockSize - 1) / peer.MetadataBlockSize
								s.metadataPieces = make([]bool, numBlocks)
								sz = hs.MetadataSize
							} else if s.metadataSize != hs.MetadataSize {
								s.mu.Unlock()
								continue
							}
							s.mu.Unlock()

							if sz > 0 {
								numBlocks := (sz + peer.MetadataBlockSize - 1) / peer.MetadataBlockSize
								for i := 0; i < numBlocks; i++ {
									s.mu.Lock()
									alreadyGot := s.metadataPieces[i]
									s.mu.Unlock()
									if !alreadyGot {
										_ = client.SendMetadataRequest(byte(peerUtMetadataID), i)
									}
								}
							}
						}
					}
				}
			} else if extMsgID == peer.LocalMetadataExtID {
				metaMsg, err := peer.ParseMetadataMessage(payloadBytes)
				if err == nil {
					switch metaMsg.MsgType {
					case peer.MetadataRequest:
						s.mu.Lock()
						inMetaMode := s.metadataMode
						infoBytes := s.Torrent.InfoBytes
						s.mu.Unlock()

						if !inMetaMode && len(infoBytes) > 0 {
							offset := int64(metaMsg.Piece) * peer.MetadataBlockSize
							if offset >= 0 && offset < int64(len(infoBytes)) {
								blockLen := int64(peer.MetadataBlockSize)
								if offset+blockLen > int64(len(infoBytes)) {
									blockLen = int64(len(infoBytes)) - offset
								}
								blockData := infoBytes[offset : offset+blockLen]
								if peerUtMetadataID != -1 {
									_ = client.SendMetadataData(byte(peerUtMetadataID), metaMsg.Piece, len(infoBytes), blockData)
								}
							} else {
								if peerUtMetadataID != -1 {
									_ = client.SendMetadataReject(byte(peerUtMetadataID), metaMsg.Piece)
								}
							}
						} else {
							if peerUtMetadataID != -1 {
								_ = client.SendMetadataReject(byte(peerUtMetadataID), metaMsg.Piece)
							}
						}

					case peer.MetadataData:
						s.mu.Lock()
						if s.metadataMode && !s.metadataCompleted && s.metadataSize > 0 && metaMsg.Piece >= 0 && metaMsg.Piece < len(s.metadataPieces) && !s.metadataPieces[metaMsg.Piece] {
							if metaMsg.TotalSize > 0 && metaMsg.TotalSize != s.metadataSize {
								s.mu.Unlock()
								continue
							}
							offset := metaMsg.Piece * peer.MetadataBlockSize
							expectedLen := peer.MetadataBlockSize
							if offset+expectedLen > s.metadataSize {
								expectedLen = s.metadataSize - offset
							}
							if expectedLen > 0 && len(metaMsg.Data) == expectedLen && offset+len(metaMsg.Data) <= len(s.metadataBuf) {
								copy(s.metadataBuf[offset:], metaMsg.Data)
								s.metadataPieces[metaMsg.Piece] = true
								lastProgressAt = time.Now() // metadata progress; keeps the stall reaper off

								allCompleted := true
								for _, done := range s.metadataPieces {
									if !done {
										allCompleted = false
										break
									}
								}

								if allCompleted {
									bufCopy := make([]byte, len(s.metadataBuf))
									copy(bufCopy, s.metadataBuf)
									s.mu.Unlock()

									err := s.onMetadataDownloaded(bufCopy)
									if err != nil {
										s.mu.Lock()
										s.lastErr = err
										s.mu.Unlock()
									}
								} else {
									s.mu.Unlock()
								}
							} else {
								s.mu.Unlock()
							}
						} else {
							s.mu.Unlock()
						}

					case peer.MetadataReject:
						// Peer rejected metadata piece request, nothing to do.
					}
				}
			} else if extMsgID == peer.LocalPEXExtID && s.pexEnabled() {
				pexMsg, err := peer.ParsePEXMessage(payloadBytes)
				if err == nil {
					s.handlePEXMessage(peerAddr, pexMsg)
				}
			}

		case peer.MsgChoke:
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
			now := time.Now()
			s.mu.Lock()
			pState.Choked = false
			s.mu.Unlock()
			// A fresh unchoke means the peer is willing to serve again, so clear any
			// pieces it previously rejected — a reject is "not this request now", not
			// a permanent refusal. Without this a single (often transient) reject
			// would bar the piece from this peer for the whole connection.
			clear(peerRejectedPieces)
			pipeline.OnUnchoke(now)
			publishPipelineSnapshot(now, true)

		case peer.MsgInterested:
			s.mu.Lock()
			pState.Interested = true
			unchokedInterested := 0
			for _, candidate := range s.Peers {
				if candidate.Active && candidate.Interested && !candidate.AmChoking {
					unchokedInterested++
				}
			}
			shouldUnchoke := pState.AmChoking && unchokedInterested < 4
			if shouldUnchoke {
				pState.AmChoking = false
			}
			s.mu.Unlock()
			if shouldUnchoke {
				_ = client.SendUnchoke()
			}

		case peer.MsgNotInterested:
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
				}
			}

		case peer.MsgHaveAll:
			if len(msg.Payload) != 0 {
				continue
			}
			if numPiecesNow == 0 {
				// Before metadata: remember it and replay once the count is known.
				peerHaveAllPending = true
				continue
			}
			setPeerBitfield(fullPieceBitfield(numPiecesNow))

		case peer.MsgHaveNone:
			if len(msg.Payload) != 0 {
				continue
			}
			if numPiecesNow == 0 {
				// The default zeroed bitfield already represents have_none; just make
				// sure a previously buffered have_all isn't replayed.
				peerHaveAllPending = false
				continue
			}
			setPeerBitfield(make([]byte, (numPiecesNow+7)/8))

		case peer.MsgBitfield:
			expectedLen := (numPiecesNow + 7) / 8
			if expectedLen == 0 || len(msg.Payload) != expectedLen {
				continue
			}
			newBF := make([]byte, expectedLen)
			copy(newBF, msg.Payload)
			setPeerBitfield(newBF)

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
				pendingAllowedFast = append(pendingAllowedFast, index)
				continue
			}
			if index >= 0 && index < int64(numPiecesNow) {
				peerAllowedFast[index] = struct{}{}
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

			// Validate against our outstanding requests for this piece.
			dl := findDownload(index)
			if dl == nil {
				pipeline.OnUnsolicited(int64(len(blockData)), now)
				continue // not a piece we're currently downloading; discard
			}

			// Validate begin is block-aligned
			if begin%BlockSize != 0 {
				pipeline.OnUnsolicited(int64(len(blockData)), now)
				continue
			}

			// Validate this block was requested and not already received
			req, exists := dl.pending[begin]
			if !exists || !req.requested || req.received {
				if exists && req.received {
					pipeline.OnDuplicate(int64(len(blockData)), now)
				} else {
					pipeline.OnUnsolicited(int64(len(blockData)), now)
				}
				continue // Unsolicited or duplicate block
			}

			// Validate block length matches expected
			if int64(len(blockData)) != req.length {
				pipeline.OnUnsolicited(int64(len(blockData)), now)
				continue
			}

			// Accept the block
			blockIndex := begin / BlockSize
			if blockIndex >= int64(len(dl.blocks)) {
				pipeline.OnUnsolicited(int64(len(blockData)), now)
				continue
			}
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
			case s.pieceWriteCh <- pieceWriteJob{index: pieceIdx, hash: pieceHash, data: pieceData, pieceBuf: pieceBuf, conn: conn}:
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
				_, requestAllowedFast := allowedFastForPeer[index]

				if paused || (amChoking && !requestAllowedFast) {
					if fastEnabled && length > 0 {
						_ = client.SendRejectRequest(uint32(index), uint32(begin), uint32(length))
					}
					continue
				}

				if isCompleted && length > 0 && length <= BlockSize && begin >= 0 && begin+length <= pieceLen {
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

		case peer.MsgPort:
			// BEP 5: the peer advertises its DHT UDP port. Combine it with the
			// peer's source IP and feed it into the routing table so live peers
			// grow our DHT beyond bootstrap nodes and lookups.
			if len(msg.Payload) == 2 {
				dhtPort := binary.BigEndian.Uint16(msg.Payload)
				s.mu.RLock()
				d := s.DHT
				allowDHT := s.allowsDecentralizedPeerDiscoveryLocked()
				s.mu.RUnlock()
				if allowDHT && d != nil && dhtPort != 0 {
					if pip := net.ParseIP(ip); pip != nil {
						d.AddNode(pip, dhtPort)
					}
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

// sendPeerControlLocked queues a peer control message while s.mu is held.
func (s *Session) sendPeerControlLocked(c *peer.Client, fn func(*peer.Client) error) {
	if s.closed {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		_ = fn(c)
	}()
}

func (s *Session) dhtLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Initial lookup
	select {
	case <-time.After(1 * time.Second):
	case <-s.ctx.Done():
		return
	}
	s.mu.RLock()
	paused := s.paused
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

	if !paused && d != nil && hasTorrent && hasInbound {
		d.LookupWithOptions(infoHash, peerPort, dht.LookupOptions{Announce: allowAnnounce})
	}

	for {
		select {
		case <-ticker.C:
			s.mu.RLock()
			paused = s.paused
			d = s.DHT
			peerPort = s.Port
			hasInbound = s.hasInboundListenerLocked()
			hasTorrent = s.Torrent != nil
			allowAnnounce = s.allowsDHTAnnounceLocked()
			if hasTorrent {
				infoHash = s.Torrent.InfoHash
			}
			s.mu.RUnlock()

			if !paused && d != nil && hasTorrent && hasInbound {
				d.LookupWithOptions(infoHash, peerPort, dht.LookupOptions{Announce: allowAnnounce})
			}
		case <-s.ctx.Done():
			return
		}
	}
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
// tracker-sourced connections.
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
	var shouldDial bool
	if !exists {
		shouldDial = true
	} else {
		// Discovery supplies a listening endpoint, so an inbound-only entry with the
		// same address becomes eligible for maintenance retries.
		pState.Dialable = true
		if !pState.Active && !pState.Dialing && time.Since(pState.LastAttempt) > peerRedialBackoff {
			shouldDial = true
		}
	}

	// Don't exceed the outbound connection cap.
	if shouldDial && len(s.outboundSlots) >= maxOutboundPeers {
		shouldDial = false
	}

	if shouldDial {
		if !exists {
			s.prunePeersLocked()
			s.Peers[peerAddr] = &PeerState{
				IP:          host,
				Port:        uint16(port),
				AmChoking:   true,
				Choked:      true,
				LastAttempt: time.Now(),
				Dialable:    true,
				Dialing:     true,
			}
		} else {
			s.Peers[peerAddr].LastAttempt = time.Now()
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

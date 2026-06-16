package downloader

import (
	"encoding/binary"
	"fmt"
	"net"
	"sainttorrent/pkg/dht"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/tracker"
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
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcpConn.SetNoDelay(true)
	_ = tcpConn.SetReadBuffer(peerSocketBufferSize)
	_ = tcpConn.SetWriteBuffer(peerSocketBufferSize)
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
			return
		}
	}
	acquiredSlots = true

	var dialErr error
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(s.ctx, "tcp", peerAddr)
	if err != nil {
		dialErr = err
	}

	if dialErr != nil {
		s.mu.Lock()
		if ps, ok := s.Peers[peerAddr]; ok {
			ps.Active = false
			ps.LastAttempt = time.Now()
		}
		s.mu.Unlock()
		return
	}
	defer conn.Close()
	tunePeerConn(conn)

	// Spawn context monitor to interrupt immediately on shutdown
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
			_ = conn.Close()
		case <-doneCh:
		}
	}()

	// Handshake with deadline
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	client := peer.NewClient(conn, s.Torrent.InfoHash, s.PeerID)
	s.mu.RLock()
	client.DisableDHT = !s.allowsDecentralizedPeerDiscoveryLocked()
	s.mu.RUnlock()
	handshake, err := client.Handshake()
	if err != nil {
		s.mu.Lock()
		if ps, ok := s.Peers[peerAddr]; ok {
			ps.Active = false
			ps.LastAttempt = time.Now()
		}
		s.mu.Unlock()
		return
	}
	_ = conn.SetDeadline(time.Time{}) // clear deadline

	if handshake.InfoHash != s.Torrent.InfoHash {
		s.mu.Lock()
		if ps, ok := s.Peers[peerAddr]; ok {
			ps.Active = false
			ps.LastAttempt = time.Now()
		}
		s.mu.Unlock()
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
			_ = conn.Close()
		case <-doneCh:
		}
	}()

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	if handshake == nil {
		var err error
		handshake, err = peer.ParseHandshake(conn)
		if err != nil {
			return
		}
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
	respHs.Reserved[5] = 0x10  // Support extension protocol (BEP 10)
	if allowDHT {
		respHs.Reserved[7] |= 0x01 // Support DHT (BEP 5)
	}
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
	s.mu.Lock()
	if s.paused || s.closed {
		s.mu.Unlock()
		return
	}
	connectionPauseEpoch := s.pauseEpoch
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
	s.mu.Unlock()

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
			s.AddPeerFromDiscovery(peerAddr)
		}
	}()

	s.mu.RLock()
	inMeta := s.metadataMode
	numPieces := len(s.PieceStates)
	s.mu.RUnlock()

	var initializedPeersAndBitfield bool = false

	if !inMeta {
		s.mu.RLock()
		bf := make([]byte, (numPieces+7)/8)
		hasAny := false
		isComplete := s.isCompletedLocked()
		for i, state := range s.PieceStates {
			if state == PieceCompleted {
				bf[i/8] |= 1 << (7 - (i % 8))
				hasAny = true
			}
		}
		s.mu.RUnlock()

		if hasAny {
			_ = client.SendBitfield(bf)
		}
		if isComplete {
			_ = client.SendNotInterested()
		} else {
			_ = client.SendInterested()
		}
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
	removeDownload := func(index int64) {
		for i, dl := range activeDownloads {
			if dl.pieceIndex == index {
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
				s.PieceStates[dl.pieceIndex] = PieceEmpty
				s.addNeededLocked(int(dl.pieceIndex))
			}
		}
		s.mu.Unlock()
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

	// openNewPiece claims the highest-priority, rarest empty wanted piece this peer has
	// and marks it PieceDownloading. In endgame (no fresh pieces left to claim) it
	// instead returns a redundant copy of an in-progress piece this peer has, leaving
	// that piece's state owned by the original downloader. Returns nil when the peer
	// has nothing left for us.
	openNewPiece := func() *activeDownload {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.paused || s.closed {
			return nil
		}
		endgame := false
		bestIdx := s.selectNeededPieceLocked(hasPiece)
		if bestIdx == -1 {
			if s.endgameActiveLocked() {
				owned := make(map[int64]bool, len(activeDownloads))
				for _, dl := range activeDownloads {
					owned[dl.pieceIndex] = true
				}
				bestIdx = s.selectEndgamePieceLocked(hasPiece, owned)
				endgame = true
			}
			if bestIdx == -1 {
				return nil
			}
		}
		if !endgame {
			s.PieceStates[bestIdx] = PieceDownloading
			s.removeNeededLocked(bestIdx)
		}
		numBlocks := s.blocksInPiece(int64(bestIdx))
		return &activeDownload{
			pieceIndex: int64(bestIdx),
			hash:       s.Torrent.PieceHashes[bestIdx],
			length:     s.Storage.PieceLength(int64(bestIdx)),
			numBlocks:  numBlocks,
			blocks:     make([][]byte, numBlocks),
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
	requestableWorkAvailable := func(pieceCap int) bool {
		for _, dl := range activeDownloads {
			if requestable(dl) {
				return true
			}
		}
		if len(activeDownloads) >= pieceCap {
			return false
		}
		s.mu.RLock()
		defer s.mu.RUnlock()
		for idx := range s.neededPieces {
			if idx >= 0 && idx < len(s.PieceStates) && s.PieceStates[idx] == PieceEmpty &&
				hasPiece(int64(idx)) && s.isPieceWanted(int64(idx)) {
				return true
			}
		}
		if s.endgameActiveLocked() {
			for i, state := range s.PieceStates {
				if state == PieceDownloading && hasPiece(int64(i)) && s.isPieceWanted(int64(i)) {
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
		if paused {
			waitingForBandwidth = false
			publishPipelineSnapshot(now, false)
			return 0
		}

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
		// for not leaking a stalled connection.
		if choked {
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
				newDL := openNewPiece()
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
		if outstanding >= window && requestableWorkAvailable(pieceCap) {
			pipeline.OnWindowLimited(now)
		}
		waitingForBandwidth = false
		publishPipelineSnapshot(now, false)
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
peerLoop:
	for {
		s.mu.RLock()
		paused := s.paused
		s.mu.RUnlock()
		if paused {
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
				break
			}
		}

		var msg *peer.Message
		select {
		case result := <-readCh:
			if result.err != nil {
				break peerLoop
			}
			msg = result.msg
		case <-pexTick:
			sendPEXDelta()
			continue
		case <-rateRetry:
			rateRetry = nil
			scheduleRateRetry(pump())
			continue
		case <-s.ctx.Done():
			break peerLoop
		}

		if msg == nil {
			// Keep alive: still run pump so outstanding requests to a now-silent peer
			// time out (and the peer is dropped after its retry budget) instead of the
			// keep-alive merely resetting the read deadline and stalling forever.
			scheduleRateRetry(pump())
			continue
		}

		s.mu.RLock()
		inMetaNow := s.metadataMode
		numPiecesNow := len(s.PieceStates)
		s.mu.RUnlock()

		if !inMetaNow && !initializedPeersAndBitfield {
			// Initialize now that metadata is downloaded!
			s.mu.RLock()
			bf := make([]byte, (numPiecesNow+7)/8)
			hasAny := false
			isComplete := s.isCompletedLocked()
			for i, state := range s.PieceStates {
				if state == PieceCompleted {
					bf[i/8] |= 1 << (7 - (i % 8))
					hasAny = true
				}
			}
			s.mu.RUnlock()

			if hasAny {
				_ = client.SendBitfield(bf)
			}
			if isComplete {
				_ = client.SendNotInterested()
			} else {
				_ = client.SendInterested()
			}

			peerBitfield = make([]byte, (numPiecesNow+7)/8)
			initializedPeersAndBitfield = true
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
			// pieces so other peers can grab them. We re-pick on unchoke.
			pipeline.OnChoke(now)
			releasePipelineReservations(activeDownloads, now)
			releaseDownloads(activeDownloads)
			activeDownloads = nil
			publishPipelineSnapshot(now, true)

		case peer.MsgUnchoke:
			now := time.Now()
			s.mu.Lock()
			pState.Choked = false
			s.mu.Unlock()
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
				byteIndex := index / 8
				bitIndex := index % 8
				if byteIndex >= uint32(len(peerBitfield)) {
					continue
				}
				mask := byte(1 << (7 - bitIndex))
				if peerBitfield[byteIndex]&mask == 0 {
					peerBitfield[byteIndex] |= mask
					s.addPieceAvailability(int(index))
				}
			}

		case peer.MsgBitfield:
			expectedLen := (numPiecesNow + 7) / 8
			if expectedLen == 0 || len(msg.Payload) != expectedLen {
				continue
			}
			if len(peerBitfield) != expectedLen {
				peerBitfield = make([]byte, expectedLen)
			}
			oldBF := append([]byte(nil), peerBitfield...)
			copy(peerBitfield, msg.Payload)
			s.applyBitfieldAvailability(oldBF, peerBitfield)

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

			// Piece complete: assemble the buffer and hand it to the async hash/write
			// pool. The peer goroutine keeps draining the socket and requesting instead
			// of stalling on sha1 + WriteBlock + the fast-resume persist. The pool
			// verifies the hash, writes, persists state, and — on a hash failure —
			// disconnects this peer (via its conn) and returns the piece to the pool.
			pieceData := make([]byte, dl.length)
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

			pieceIdx := dl.pieceIndex
			pieceHash := dl.hash
			removeDownload(dl.pieceIndex)

			if !validPiece || offset != int64(len(pieceData)) {
				// Assembly invariant violated (shouldn't happen): return to the pool.
				s.mu.Lock()
				if pieceIdx >= 0 && pieceIdx < int64(len(s.PieceStates)) && s.PieceStates[pieceIdx] == PieceDownloading {
					s.PieceStates[pieceIdx] = PieceEmpty
					s.addNeededLocked(int(pieceIdx))
				}
				s.mu.Unlock()
				break
			}

			s.ensurePieceWritePool()
			writeQueueStarted := time.Now()
			select {
			case s.pieceWriteCh <- pieceWriteJob{index: pieceIdx, hash: pieceHash, data: pieceData, conn: conn}:
				if blocked := time.Since(writeQueueStarted); blocked > 10*time.Millisecond {
					pipeline.OnWriterLimited(time.Now())
					publishPipelineSnapshot(time.Now(), true)
				}
			case <-s.ctx.Done():
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

				if paused || amChoking {
					continue
				}

				if isCompleted && length > 0 && length <= BlockSize && begin >= 0 && begin+length <= pieceLen {
					// Apply upload rate limiting
					if err := s.UploadLimiter.Wait(s.ctx, int(length)); err != nil {
						continue
					}
					if s.GlobalUploadLimiter != nil {
						if err := s.GlobalUploadLimiter.Wait(s.ctx, int(length)); err != nil {
							continue
						}
					}
					buf := make([]byte, length)
					_, err := s.Storage.ReadBlock(index, begin, buf)
					if err == nil {
						if err := client.SendPiece(uint32(index), uint32(begin), buf); err == nil {
							// Lock-free counter update (see the download hot path above).
							s.Uploaded.Add(length)
							atomic.AddInt64(&pState.Uploaded, length)
						}
					}
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
		// needed (pump no-ops when paused, choked, or seeding).
		dropCompletedElsewhere()
		scheduleRateRetry(pump())
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
		if p.Active {
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
		if !p.Active {
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
	var infoHash [20]byte
	if s.Torrent != nil {
		infoHash = s.Torrent.InfoHash
	}
	s.mu.RUnlock()

	if !paused && d != nil && s.Torrent != nil && hasInbound {
		d.Lookup(infoHash, peerPort)
	}

	for {
		select {
		case <-ticker.C:
			s.mu.RLock()
			paused = s.paused
			d = s.DHT
			peerPort = s.Port
			hasInbound = s.hasInboundListenerLocked()
			if s.Torrent != nil {
				infoHash = s.Torrent.InfoHash
			}
			s.mu.RUnlock()

			if !paused && d != nil && s.Torrent != nil && hasInbound {
				d.Lookup(infoHash, peerPort)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// AddPeerFromDiscovery adds a new peer found via DHT or other discovery mechanism
// and attempts to initiate a connection.
func (s *Session) AddPeerFromDiscovery(peerAddr string) {
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
	defer s.mu.Unlock()

	if s.paused || s.closed || !s.started {
		return
	}
	if !s.allowsDecentralizedPeerDiscoveryLocked() {
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

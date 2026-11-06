package downloader

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"

	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/netpolicy"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/tracker"
	"sainttorrent/pkg/utp"
)

// BEP 55 holepunching. A peer behind a NAT without UPnP or NAT-PMP cannot be
// dialed, and when we cannot accept connections either, a direct dial to it
// just times out. If a third peer is connected to both of us, we ask it (the
// relay) to introduce us with a rendezvous message; it sends each of us a
// connect message naming the other, and both sides dial each other over uTP
// at once. Each side's SYN opens its own NAT to the other's endpoint, so the
// SYNs that follow get through. Every path here runs per control message or
// per failed dial, never per block.

// holepunchExpectTTL is how long, after a connect message, inbound uTP from
// the named endpoint is let through a socket that otherwise refuses inbound
// uTP (see utp.Socket.ExpectIncoming). It covers the other side's dial
// timeout with room for the relay's two messages to arrive apart.
var holepunchExpectTTL = 2 * peerDialTimeout

// holepunchRetryInterval is how long after a rendezvous for an endpoint we
// wait before asking again. A var so tests can shorten it.
var holepunchRetryInterval = peerRedialBackoff

// maxHolepunchRelays is how many relays (peers that listed an endpoint in PEX
// with the holepunch flag) one known peer remembers.
const maxHolepunchRelays = 4

// maxHolepunchPerInterval bounds the ut_holepunch messages one connection may
// have us act on per pexInterval. A rendezvous makes us write to another
// peer, and a connect makes us dial, so like ut_pex they are rate-limited; an
// honest peer sends a few per minute at most. The rest are ignored.
const maxHolepunchPerInterval = 32

// liveConn is what other connections' loops may need to know about one live
// connection: how to reach it and whether it speaks ut_holepunch. Guarded by
// Session.mu.
type liveConn struct {
	client   *peer.Client
	outbound bool
	utp      bool
	// endpoint is where other peers can reach this peer: the address we
	// dialed, the source of an inbound uTP connection (a NAT'd peer's mapped
	// UDP endpoint, the one a holepunch needs), or the listen port an inbound
	// TCP peer announced. "" when unknown.
	endpoint string
	// holepunchID is the peer's ut_holepunch message ID; 0 when unsupported.
	holepunchID byte
}

// pexFlags returns the BEP 11 flags we advertise for w.
func (w *liveConn) pexFlags() byte {
	var flags byte
	if w.utp {
		flags |= peer.PEXFlagUTP
	}
	if w.holepunchID != 0 {
		flags |= peer.PEXFlagHolepunch
	}
	if w.outbound {
		flags |= peer.PEXFlagOutgoing
	}
	return flags
}

// registerLiveConnLocked records the connection at key. Caller holds s.mu.
func (s *Session) registerLiveConnLocked(key string, w *liveConn) {
	if s.liveConns == nil {
		s.liveConns = make(map[string]*liveConn)
	}
	s.liveConns[key] = w
}

// releaseLiveConnLocked forgets the connection at key if it is still client's.
// Caller holds s.mu.
func (s *Session) releaseLiveConnLocked(key string, client *peer.Client) {
	if w := s.liveConns[key]; w != nil && w.client == client {
		delete(s.liveConns, key)
	}
}

// findLiveConnLocked returns the live connection reachable at endpoint: the
// one keyed by it, or one whose endpoint it is. Caller holds s.mu.
func (s *Session) findLiveConnLocked(endpoint string) (string, *liveConn) {
	if w := s.liveConns[endpoint]; w != nil {
		return endpoint, w
	}
	for key, w := range s.liveConns {
		if w.endpoint == endpoint {
			return key, w
		}
	}
	return "", nil
}

// holepunchEnabledLocked reports whether we take part in BEP 55 for this
// torrent. Like PEX it is off for private torrents (BEP 27): it introduces
// peers to each other outside the tracker. Caller holds s.mu.
func (s *Session) holepunchEnabledLocked() bool {
	return s.pexEnabledLocked()
}

// isUTPConn reports whether conn runs over uTP, under any encryption or
// buffering wrappers.
func isUTPConn(conn net.Conn) bool {
	for conn != nil {
		if _, ok := conn.(*utp.Conn); ok {
			return true
		}
		wrapped, ok := conn.(underlyingConn)
		if !ok {
			return false
		}
		conn = wrapped.UnderlyingConn()
	}
	return false
}

// holepunchLimiter counts the ut_holepunch messages one connection sent in the
// current pexInterval window. Owned by the connection's message loop.
type holepunchLimiter struct {
	windowStart time.Time
	count       int
}

func (l *holepunchLimiter) admit(now time.Time) bool {
	if l.windowStart.IsZero() || now.Sub(l.windowStart) >= pexInterval {
		l.windowStart = now
		l.count = 0
	}
	if l.count >= maxHolepunchPerInterval {
		return false
	}
	l.count++
	return true
}

// noteHolepunchRelaysLocked records relay as a peer that can introduce us to
// each of addrs: it listed them in PEX as holepunch-capable, or relayed a
// connect for them. Caller holds s.mu.
func (s *Session) noteHolepunchRelaysLocked(relay string, addrs []string) {
	for _, addr := range addrs {
		ps := s.Peers[addr]
		if ps == nil || addr == relay {
			continue
		}
		known := false
		for _, r := range ps.holepunchRelays {
			if r == relay {
				known = true
				break
			}
		}
		if known {
			continue
		}
		copy(ps.holepunchRelays[1:], ps.holepunchRelays[:maxHolepunchRelays-1])
		ps.holepunchRelays[0] = relay
	}
}

// requestHolepunch asks a relay to introduce us to peerAddr, whose direct dial
// just failed. It does nothing unless a connected relay that speaks
// ut_holepunch listed peerAddr and the last request for it is older than
// holepunchRetryInterval. The rendezvous is written from its own goroutine,
// so a relay with a full send buffer holds up nobody.
func (s *Session) requestHolepunch(peerAddr string) {
	defer s.crashGuard("holepunch_request")()
	target, err := netip.ParseAddrPort(peerAddr)
	if err != nil {
		return
	}
	target = netip.AddrPortFrom(target.Addr().Unmap(), target.Port())

	s.mu.Lock()
	ps := s.Peers[peerAddr]
	if ps == nil || ps.Active || s.paused || s.closed || s.utpSocket == nil || !s.holepunchEnabledLocked() {
		s.mu.Unlock()
		return
	}
	now := time.Now()
	if !ps.holepunchAt.IsZero() && now.Sub(ps.holepunchAt) < holepunchRetryInterval {
		s.mu.Unlock()
		return
	}
	var relayAddr string
	var relay *liveConn
	for _, r := range ps.holepunchRelays {
		if r == "" {
			continue
		}
		if w := s.liveConns[r]; w != nil && w.holepunchID != 0 && s.activePeers[r] == w.client {
			relayAddr, relay = r, w
			break
		}
	}
	if relay == nil {
		s.mu.Unlock()
		return
	}
	ps.holepunchAt = now
	client, extID := relay.client, relay.holepunchID
	s.mu.Unlock()

	if logging.Enabled() {
		logging.Debug("holepunch_rendezvous",
			logging.String("peer", peerAddr),
			logging.String("relay", relayAddr),
		)
	}
	go func() {
		defer s.crashGuard("holepunch_send")()
		_ = client.SendHolepunch(extID, &peer.HolepunchMessage{Type: peer.HolepunchRendezvous, Addr: target})
	}()
}

// holepunchConn is the per-connection state handleHolepunchMessage needs:
// the connection it arrived on, the sender's ut_holepunch ID, and its rate
// limit. Owned by that connection's message loop.
type holepunchConn struct {
	key    string // activePeers key of the sender
	ip     string // sender's IP
	client *peer.Client
	peerID byte // sender's ut_holepunch ID; 0 until its handshake names one
	limit  holepunchLimiter
}

// handleHolepunchMessage acts on one ut_holepunch payload from hc's peer.
func (s *Session) handleHolepunchMessage(hc *holepunchConn, payload []byte) {
	if len(payload) > peer.MaxHolepunchMessageSize || !hc.limit.admit(time.Now()) {
		return
	}
	msg, err := peer.ParseHolepunchMessage(payload)
	if err != nil {
		return
	}
	switch msg.Type {
	case peer.HolepunchRendezvous:
		s.relayHolepunch(hc, msg.Addr)
	case peer.HolepunchConnect:
		s.holepunchConnect(hc, msg.Addr)
	case peer.HolepunchError:
		if logging.Enabled() {
			logging.Debug("holepunch_failed",
				logging.String("peer", msg.Addr.String()),
				logging.String("relay", hc.key),
				logging.String("reason", msg.ErrCode.String()),
			)
		}
	}
}

// relayHolepunch serves a rendezvous: hc's peer (the initiator) asks us to
// introduce it to target. When we are connected to target and it speaks
// ut_holepunch, each is sent a connect naming the other; otherwise the
// initiator gets an error message.
func (s *Session) relayHolepunch(hc *holepunchConn, target netip.AddrPort) {
	if hc.peerID == 0 {
		return // it never told us its ID, so we cannot answer
	}
	reply := func(errCode peer.HolepunchErrCode) {
		_ = hc.client.SendHolepunch(hc.peerID, &peer.HolepunchMessage{Type: peer.HolepunchError, Addr: target, ErrCode: errCode})
	}
	if !target.IsValid() || target.Port() == 0 || target.Addr().IsUnspecified() {
		reply(peer.HolepunchErrNoSuchPeer)
		return
	}
	targetAddr := target.String()

	s.mu.RLock()
	if !s.holepunchEnabledLocked() {
		s.mu.RUnlock()
		return
	}
	initiator := s.liveConns[hc.key]
	targetKey, tw := s.findLiveConnLocked(targetAddr)
	_, self := s.admission.selfAddrs[targetAddr]
	var errCode peer.HolepunchErrCode
	switch {
	case initiator == nil || initiator.client != hc.client:
		s.mu.RUnlock()
		return // the connection is going away
	case self:
		errCode = peer.HolepunchErrNoSelf
	case tw == nil:
		errCode = peer.HolepunchErrNotConnected
	case targetKey == hc.key:
		errCode = peer.HolepunchErrNoSuchPeer
	case tw.holepunchID == 0:
		errCode = peer.HolepunchErrNoSupport
	}
	initiatorEndpoint := initiator.endpoint
	if initiatorEndpoint == "" {
		// An inbound TCP peer that announced no listen port: its source
		// address is all we have, as libtorrent would send.
		initiatorEndpoint = hc.key
	}
	var targetClient *peer.Client
	var targetID byte
	if tw != nil {
		targetClient, targetID = tw.client, tw.holepunchID
	}
	s.mu.RUnlock()
	if errCode != 0 {
		reply(errCode)
		return
	}
	initiatorAP, err := netip.ParseAddrPort(initiatorEndpoint)
	if err != nil {
		reply(peer.HolepunchErrNoSuchPeer)
		return
	}

	if logging.Enabled() {
		logging.Debug("holepunch_relayed",
			logging.String("initiator", initiatorEndpoint),
			logging.String("target", targetAddr),
		)
	}
	// The target's connection belongs to another loop: write from a goroutine
	// so its send buffer cannot stall this one.
	go func() {
		defer s.crashGuard("holepunch_send")()
		_ = targetClient.SendHolepunch(targetID, &peer.HolepunchMessage{Type: peer.HolepunchConnect, Addr: initiatorAP})
	}()
	_ = hc.client.SendHolepunch(hc.peerID, &peer.HolepunchMessage{Type: peer.HolepunchConnect, Addr: target})
}

// holepunchConnect serves a connect: hc's peer (the relay) tells us to
// connect to target, which it has told to connect to us. We let target's uTP
// SYN through our socket's inbound refusal and dial it over uTP at once; our
// SYN opens our NAT for its SYN, and its SYN opens its NAT for ours.
// Whichever connection completes first is kept (see admitPeerLocked).
func (s *Session) holepunchConnect(hc *holepunchConn, target netip.AddrPort) {
	source, _ := netip.ParseAddr(hc.ip)
	if !target.IsValid() || target.Port() == 0 || target.Addr().IsUnspecified() || !netpolicy.PeerAllowed(target, source.Unmap()) {
		return
	}
	addr := net.JoinHostPort(target.Addr().String(), strconv.Itoa(int(target.Port())))
	if addr == hc.key {
		return
	}

	s.mu.Lock()
	socket := s.utpSocket
	if socket == nil || !s.started || s.paused || s.closed || !s.holepunchEnabledLocked() || s.metadataStalledLocked() ||
		s.refusesDialLocked(addr, target.Addr().String()) {
		s.mu.Unlock()
		return
	}
	if _, active := s.activePeers[addr]; active || s.heldByInboundLocked(addr) {
		s.mu.Unlock()
		return
	}
	now := time.Now()
	ps := s.Peers[addr]
	if ps == nil {
		s.prunePeersLocked()
		ps = &PeerState{
			IP:        target.Addr().String(),
			Port:      target.Port(),
			AmChoking: true,
			Choked:    true,
			Dialable:  true,
			Source:    PeerSourceDiscovery,
			seenAt:    now,
		}
		s.Peers[addr] = ps
	} else {
		ps.Source |= PeerSourceDiscovery
		ps.seenAt = now
	}
	s.noteHolepunchRelaysLocked(hc.key, []string{addr})
	dial := !ps.Dialing && !s.privateRefusesDialLocked(ps)
	if dial {
		ps.Dialing = true
		ps.LastAttempt = now
		s.wg.Add(1)
	}
	s.mu.Unlock()

	// Before our SYN goes out, so a SYN of theirs that crosses it is answered.
	socket.ExpectIncoming(target, holepunchExpectTTL)
	if logging.Enabled() {
		logging.Debug("holepunch_connect",
			logging.String("peer", addr),
			logging.String("relay", hc.key),
			logging.Bool("dial", dial),
		)
	}
	if dial {
		go func() {
			defer s.wg.Done()
			s.connectToPeerVia(tracker.Peer{IP: target.Addr().AsSlice(), Port: target.Port()}, true)
		}()
	}
}

// dialPeerUTP connects to peerAddr over uTP only: a holepunched endpoint is
// reachable through the NAT mapping our own UDP socket opened, and nothing
// else.
func (s *Session) dialPeerUTP(peerAddr string) (net.Conn, string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, peerDialTimeout)
	defer cancel()
	s.mu.RLock()
	socket := s.utpSocket
	s.mu.RUnlock()
	if socket == nil {
		return nil, "", errors.New("utp dial failed: no uTP socket")
	}
	conn, err := socket.DialContext(ctx, peerAddr)
	if err != nil {
		return nil, "", fmt.Errorf("utp dial failed: %w", err)
	}
	return conn, "utp", nil
}

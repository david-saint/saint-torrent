package downloader

import (
	"bytes"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"

	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/peer"
)

// StartPeerListener starts the manager-wide BitTorrent TCP listener. All
// managed sessions share this socket and are selected by the incoming
// handshake's info-hash.
func (m *TorrentManager) StartPeerListener(port uint16) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("parse peer listener address: %w", err)
	}
	actualPort, err := strconv.Atoi(portText)
	if err != nil || actualPort <= 0 || actualPort > 65535 {
		_ = listener.Close()
		return fmt.Errorf("invalid peer listener port %q", portText)
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = listener.Close()
		return fmt.Errorf("torrent manager is closed")
	}
	if m.peerListener != nil {
		m.mu.Unlock()
		_ = listener.Close()
		return fmt.Errorf("peer listener already started")
	}
	for _, sess := range m.sessions {
		sess.mu.RLock()
		if sess.started {
			sess.mu.RUnlock()
			m.mu.Unlock()
			_ = listener.Close()
			return fmt.Errorf("cannot enable shared listener after sessions have started")
		}
		sess.mu.RUnlock()
	}
	m.peerListener = listener
	m.peerListenPort = uint16(actualPort)
	m.advertisedPeerPort = uint16(actualPort)
	m.natStatus.ListenPort = uint16(actualPort)
	m.natStatus.AdvertisedPort = uint16(actualPort)
	for _, sess := range m.sessions {
		sess.mu.Lock()
		sess.sharedInbound = true
		sess.Port = uint16(actualPort)
		sess.mu.Unlock()
	}
	m.wg.Add(1)
	m.mu.Unlock()

	if logging.Enabled() {
		logging.Info("peer_listener_started",
			logging.Uint16("port", uint16(actualPort)),
		)
	}
	go m.peerAcceptLoop(listener)
	return nil
}

// PeerListenPort returns the local TCP port used by the shared peer listener.
func (m *TorrentManager) PeerListenPort() uint16 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.peerListenPort
}

// AdvertisedPeerPort returns the port announced to trackers and the DHT.
func (m *TorrentManager) AdvertisedPeerPort() uint16 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.advertisedPeerPort
}

func (m *TorrentManager) peerAcceptLoop(listener net.Listener) {
	defer m.wg.Done()
	m.acceptLoop(listener, func() bool {
		return m.peerListener == listener
	})
}

func (m *TorrentManager) utpAcceptLoop(listener net.Listener) {
	defer m.wg.Done()
	m.acceptLoop(listener, func() bool {
		return m.utpListener == listener
	})
}

// acceptLoop serves manager-wide listeners. current is called while m.mu is held.
func (m *TorrentManager) acceptLoop(listener net.Listener, current func() bool) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			m.mu.RLock()
			closed := m.closed || !current()
			m.mu.RUnlock()
			if closed {
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		m.mu.Lock()
		if m.closed || !current() {
			m.mu.Unlock()
			_ = conn.Close()
			return
		}
		m.wg.Add(1)
		m.mu.Unlock()
		go m.handleRoutedIncomingConnection(conn)
	}
}

const (
	// maxInboundHandshakes caps inbound connections that have not yet
	// completed a BitTorrent handshake for a torrent we serve. They draw from
	// this budget instead of globalInboundSlots, so connections that stall
	// before the handshake (or never send a byte) cannot hold the slots of
	// established peers; a real handshake takes a few round trips, so 256 in
	// flight is far more than legitimate arrivals need.
	maxInboundHandshakes = 256
	// maxInboundHandshakesPerSource is the share of that budget one source
	// may hold once more than half of it is in use (see admitHandshakeSource).
	// It is generous enough for several peers behind one CGNAT address.
	maxInboundHandshakesPerSource = 8
)

func (m *TorrentManager) handleRoutedIncomingConnection(conn net.Conn) {
	defer m.wg.Done()
	defer conn.Close()

	// With every established slot taken the peer could not be served anyway,
	// so it is turned away before its handshake costs anything.
	if len(m.globalInboundSlots) == cap(m.globalInboundSlots) {
		return
	}
	select {
	case m.inboundHandshakeSlots <- struct{}{}:
	default:
		return
	}
	src, hasSrc := handshakeSource(conn.RemoteAddr())
	if hasSrc && !m.admitHandshakeSource(src) {
		<-m.inboundHandshakeSlots
		return
	}
	conn, handshake, sess := m.readRoutedHandshake(conn)
	if hasSrc {
		m.releaseHandshakeSource(src)
	}
	<-m.inboundHandshakeSlots
	if sess == nil {
		return
	}

	// Only a peer that named one of our torrents takes an established slot,
	// held for the life of the connection.
	select {
	case m.globalInboundSlots <- struct{}{}:
		defer func() { <-m.globalInboundSlots }()
	default:
		return
	}

	if logging.Enabled() {
		logging.Debug("peer_inbound_routed",
			logging.String("remote_addr", conn.RemoteAddr().String()),
			logging.String("info_hash", fmt.Sprintf("%x", handshake.InfoHash)),
		)
	}
	sess.handleRoutedIncomingConnection(conn, handshake)
}

// handshakeSource returns the address a pre-handshake connection is counted
// under: the remote IPv4 address, or the /64 of an IPv6 one, since one IPv6
// host is routinely handed a whole /64 to pick source addresses from.
func handshakeSource(addr net.Addr) (netip.Addr, bool) {
	var ip netip.Addr
	switch a := addr.(type) {
	case *net.TCPAddr:
		ip = a.AddrPort().Addr()
	case *net.UDPAddr:
		ip = a.AddrPort().Addr()
	default:
		return netip.Addr{}, false
	}
	ip = ip.Unmap().WithZone("")
	if ip.Is6() {
		prefix, err := ip.Prefix(64)
		if err != nil {
			return netip.Addr{}, false
		}
		ip = prefix.Addr()
	}
	return ip, ip.IsValid()
}

// admitHandshakeSource counts one more pre-handshake connection from src. The
// caller already holds a handshake slot. While fewer than half the budget's
// handshakes are in flight any source is admitted, so a burst from one
// address (a cross-seeding box dialing us for many torrents at once) is not
// slowed; past that, a source holding maxInboundHandshakesPerSource is turned
// away. One host holding idle sockets can then take half the budget, not all
// of it, and the rest stays open to every other peer.
func (m *TorrentManager) admitHandshakeSource(src netip.Addr) bool {
	m.handshakeSourcesMu.Lock()
	defer m.handshakeSourcesMu.Unlock()
	n := m.handshakeSources[src]
	if n >= maxInboundHandshakesPerSource && m.sourcedHandshakes >= maxInboundHandshakes/2 {
		return false
	}
	m.handshakeSources[src] = n + 1
	m.sourcedHandshakes++
	return true
}

func (m *TorrentManager) releaseHandshakeSource(src netip.Addr) {
	m.handshakeSourcesMu.Lock()
	defer m.handshakeSourcesMu.Unlock()
	m.sourcedHandshakes--
	if n := m.handshakeSources[src]; n > 1 {
		m.handshakeSources[src] = n - 1
	} else {
		delete(m.handshakeSources, src)
	}
}

// readRoutedHandshake negotiates MSE and reads the BitTorrent handshake under
// peerHandshakeTimeout, returning the session it names, or a nil session if
// the peer fails to complete it or names a torrent we do not serve.
func (m *TorrentManager) readRoutedHandshake(conn net.Conn) (net.Conn, *peer.Handshake, *Session) {
	_ = conn.SetDeadline(time.Now().Add(peerHandshakeTimeout))
	m.mu.RLock()
	policy := m.encryptionPolicy
	m.mu.RUnlock()

	conn, mseResult, encrypted, err := negotiateIncomingPeerConn(conn, policy, m.secretKeys.lookup)
	if err != nil {
		return nil, nil, nil
	}
	handshake, err := peer.ParseHandshake(conn)
	if err != nil {
		return nil, nil, nil
	}
	if encrypted && !bytes.Equal(mseResult.SecretKey, handshake.InfoHash[:]) {
		return nil, nil, nil
	}

	m.mu.RLock()
	sess := m.sessions[fmt.Sprintf("%x", handshake.InfoHash)]
	m.mu.RUnlock()
	if sess == nil {
		return nil, nil, nil
	}
	return conn, handshake, sess
}

func (m *TorrentManager) setAdvertisedPeerPort(port uint16) {
	if port == 0 {
		return
	}
	m.mu.Lock()
	if m.closed || m.advertisedPeerPort == port {
		m.mu.Unlock()
		return
	}
	m.advertisedPeerPort = port
	m.natStatus.AdvertisedPort = port
	sessions := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		sessions = append(sessions, sess)
	}
	m.mu.Unlock()

	for _, sess := range sessions {
		sess.setAdvertisedPort(port)
	}
	if logging.Enabled() {
		logging.Info("peer_advertised_port_changed",
			logging.Uint16("port", port),
		)
	}
}

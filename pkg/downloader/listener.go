package downloader

import (
	"fmt"
	"net"
	"strconv"
	"time"

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
	for {
		conn, err := listener.Accept()
		if err != nil {
			m.mu.RLock()
			closed := m.closed || m.peerListener != listener
			m.mu.RUnlock()
			if closed {
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		m.mu.Lock()
		if m.closed || m.peerListener != listener {
			m.mu.Unlock()
			_ = conn.Close()
			return
		}
		m.wg.Add(1)
		m.mu.Unlock()
		go m.handleRoutedIncomingConnection(conn)
	}
}

func (m *TorrentManager) handleRoutedIncomingConnection(conn net.Conn) {
	defer m.wg.Done()
	defer conn.Close()

	select {
	case m.globalInboundSlots <- struct{}{}:
		defer func() { <-m.globalInboundSlots }()
	default:
		return
	}

	tunePeerConn(conn)
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	handshake, err := peer.ParseHandshake(conn)
	if err != nil {
		return
	}

	m.mu.RLock()
	sess := m.sessions[fmt.Sprintf("%x", handshake.InfoHash)]
	m.mu.RUnlock()
	if sess == nil {
		return
	}

	sess.handleRoutedIncomingConnection(conn, handshake)
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
}

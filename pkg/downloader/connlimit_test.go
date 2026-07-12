package downloader

import (
	"crypto/sha1"
	"fmt"
	"net"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
	"sainttorrent/pkg/tracker"
)

// newCapTestSession builds a minimal single-file session backed by real on-disk
// storage in a temp dir. Used by the connection-limit tests.
func newCapTestSession(t *testing.T) *Session {
	t.Helper()
	tempDir := t.TempDir()
	tor := &torrent.Torrent{
		Name:        "captest",
		PieceLength: 65536,
		PieceHashes: [][20]byte{sha1.Sum([]byte("p0")), sha1.Sum([]byte("p1"))},
		Files:       []torrent.File{{Length: 100000, Path: []string{"captest"}}},
	}
	st, err := storage.NewStorage(tempDir, []storage.FileInfo{{Path: "captest", Length: 100000}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("storage.NewStorage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, tempDir)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	return sess
}

// TestPrunePeersLocked verifies that once the Peers map grows past maxKnownPeers,
// pruning evicts inactive entries oldest-first down to ~75% of the cap, and never
// evicts active peers.
func TestPrunePeersLocked(t *testing.T) {
	s := &Session{Peers: make(map[string]*PeerState)}
	base := time.Now()

	// Active peers must survive even with a very old LastAttempt.
	activeAddrs := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		addr := fmt.Sprintf("10.0.0.%d:6881", i)
		s.Peers[addr] = &PeerState{Active: true, LastAttempt: base.Add(-24 * time.Hour)}
		activeAddrs = append(activeAddrs, addr)
	}

	// Fill well past the cap with inactive peers; larger i == more recent attempt.
	nInactive := maxKnownPeers + 200
	var oldestInactive, newestInactive string
	for i := 0; i < nInactive; i++ {
		addr := fmt.Sprintf("192.168.%d.%d:%d", i/256, i%256, 7000+i%1000)
		s.Peers[addr] = &PeerState{Active: false, LastAttempt: base.Add(time.Duration(i) * time.Second)}
		switch i {
		case 0:
			oldestInactive = addr
		case nInactive - 1:
			newestInactive = addr
		}
	}

	before := len(s.Peers)
	s.mu.Lock()
	s.prunePeersLocked()
	s.mu.Unlock()
	after := len(s.Peers)

	if after >= before {
		t.Fatalf("prune did not shrink the map: before=%d after=%d", before, after)
	}
	if want := maxKnownPeers * 3 / 4; after != want {
		t.Errorf("surviving count after prune = %d, want %d (~75%% of cap)", after, want)
	}
	for _, addr := range activeAddrs {
		if _, ok := s.Peers[addr]; !ok {
			t.Errorf("active peer %s was evicted", addr)
		}
	}
	if _, ok := s.Peers[oldestInactive]; ok {
		t.Errorf("oldest inactive peer %s should have been evicted first", oldestInactive)
	}
	if _, ok := s.Peers[newestInactive]; !ok {
		t.Errorf("newest inactive peer %s should have survived", newestInactive)
	}
}

// TestPrunePeersLockedNoOpUnderCap verifies pruning is a no-op below the cap.
func TestPrunePeersLockedNoOpUnderCap(t *testing.T) {
	s := &Session{Peers: make(map[string]*PeerState)}
	for i := 0; i < 100; i++ {
		addr := fmt.Sprintf("10.1.%d.%d:6881", i/256, i%256)
		s.Peers[addr] = &PeerState{Active: false, LastAttempt: time.Now()}
	}
	s.mu.Lock()
	s.prunePeersLocked()
	s.mu.Unlock()
	if len(s.Peers) != 100 {
		t.Errorf("prune evicted peers while under the cap: got %d, want 100", len(s.Peers))
	}
}

// dialInboundTestPeer opens a real TCP connection to addr and completes a minimal
// BitTorrent handshake + empty bitfield, so the session's inbound accept path
// creates a genuine PeerState keyed by this dial's ephemeral source port. It
// returns the connection (so the test controls exactly when it closes) and that
// source address, which is the s.Peers key the session will use.
func dialInboundTestPeer(t *testing.T, addr string, infoHash [20]byte) (net.Conn, string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	var pid [20]byte
	copy(pid[:], "-MOCKconnlimittest01")
	client := peer.NewClient(conn, infoHash, pid)
	if _, err := client.Handshake(); err != nil {
		conn.Close()
		t.Fatalf("handshake: %v", err)
	}
	if err := client.SendBitfield(nil); err != nil {
		conn.Close()
		t.Fatalf("send bitfield: %v", err)
	}
	return conn, conn.LocalAddr().String()
}

// TestInboundConnectDeletesUndialablePeerOnDisconnect covers issue #62: an
// inbound-only PeerState (never confirmed dialable by tracker/DHT discovery or a
// successful outbound dial, and keyed by an ephemeral source port that can never
// be redialed) must be dropped from s.Peers outright on disconnect rather than
// retained inactive forever — otherwise inbound reconnect churn on a seeding or
// private session grows the map without bound.
func TestInboundConnectDeletesUndialablePeerOnDisconnect(t *testing.T) {
	sess, bf, _ := newStallTestTorrent(t, 4)
	sess.Start()
	defer sess.Close()
	sess.WaitVerified()

	sess.mu.RLock()
	port := sess.Port
	infoHash := sess.Torrent.InfoHash
	sess.mu.RUnlock()
	if port == 0 {
		t.Skip("session did not bind an inbound listener")
	}
	_ = bf

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, peerAddr := dialInboundTestPeer(t, addr, infoHash)

	waitForActivePeers(t, sess, 1, 2*time.Second, "inbound peer never connected")

	sess.mu.RLock()
	ps, ok := sess.Peers[peerAddr]
	sess.mu.RUnlock()
	if !ok {
		t.Fatalf("inbound peer %s missing from Peers map while connected", peerAddr)
	}
	if ps.Dialable {
		t.Fatalf("inbound-only peer %s should not be marked Dialable", peerAddr)
	}

	conn.Close()
	waitForActivePeers(t, sess, 0, 2*time.Second, "inbound peer never disconnected")

	// The disconnect defer runs asynchronously relative to activePeers hitting 0
	// (it clears activePeers first, deletes the map entry second), so poll briefly.
	deadline := time.Now().Add(1 * time.Second)
	for {
		sess.mu.RLock()
		_, stillPresent := sess.Peers[peerAddr]
		sess.mu.RUnlock()
		if !stillPresent {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("inbound-only peer %s was retained in Peers after disconnect", peerAddr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestInboundConnectPrunesOversizedPeersMap covers issue #62: a session with no
// tracker/DHT discovery churn (seeding, or a private torrent with a static peer
// list) never hits the discovery-path insert branches that normally drive
// prunePeersLocked. A single inbound connection's map insert must trigger pruning
// too, or such a session's Peers map can grow without bound from inbound reconnect
// churn alone.
func TestInboundConnectPrunesOversizedPeersMap(t *testing.T) {
	sess, bf, _ := newStallTestTorrent(t, 4)
	_ = bf

	// Pre-fill the map with far more inactive entries than the cap, before the
	// listener is up, so nothing else can touch it first.
	sess.mu.Lock()
	// Every prefilled entry's LastAttempt must be well in the past so the real
	// inbound connection dialed below (whose LastAttempt is set to time.Now() at
	// insert time) sorts as the newest entry and survives the prune. Entries are
	// non-Dialable: they model the inbound-only endpoints that accumulate under
	// issue #62, and this keeps the live peerMaintenanceLoop (running on its 5s
	// tick once Start() is called) from ever treating them as redial candidates.
	// prunePeersLocked ignores Dialable (it only spares Active peers), so the
	// prune assertions below are unchanged either way.
	base := time.Now().Add(-24 * time.Hour)
	for i := 0; i < maxKnownPeers+200; i++ {
		a := fmt.Sprintf("192.168.%d.%d:%d", i/256, i%256, 7000+i%1000)
		sess.Peers[a] = &PeerState{Active: false, LastAttempt: base.Add(time.Duration(i) * time.Second), Dialable: false}
	}
	preCount := len(sess.Peers)
	sess.mu.Unlock()

	sess.Start()
	defer sess.Close()
	sess.WaitVerified()

	sess.mu.RLock()
	port := sess.Port
	infoHash := sess.Torrent.InfoHash
	sess.mu.RUnlock()
	if port == 0 {
		t.Skip("session did not bind an inbound listener")
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, _ := dialInboundTestPeer(t, addr, infoHash)
	defer conn.Close()

	waitForActivePeers(t, sess, 1, 5*time.Second, "inbound peer never connected")

	sess.mu.RLock()
	after := len(sess.Peers)
	sess.mu.RUnlock()
	if after >= preCount {
		t.Fatalf("a single inbound connect did not prune the oversized Peers map: before=%d after=%d", preCount, after)
	}
	if after > maxKnownPeers {
		t.Fatalf("Peers map still over cap after inbound-triggered prune: %d > %d", after, maxKnownPeers)
	}
}

// TestNewSessionConnSlotCapacities verifies the per-session connection semaphores
// are sized correctly and that a standalone session has no global slots until it is
// added to a manager.
func TestNewSessionConnSlotCapacities(t *testing.T) {
	sess := newCapTestSession(t)
	if got := cap(sess.outboundSlots); got != maxOutboundPeers {
		t.Errorf("outboundSlots cap = %d, want %d", got, maxOutboundPeers)
	}
	if got := cap(sess.inboundSlots); got != maxInboundPeers {
		t.Errorf("inboundSlots cap = %d, want %d", got, maxInboundPeers)
	}
	if sess.globalOutboundSlots != nil || sess.globalInboundSlots != nil {
		t.Error("standalone session should not have global conn slots wired")
	}
}

// TestAddSessionWiresGlobalConnSlots verifies AddSession injects the manager's shared
// global connection semaphores into the session.
func TestAddSessionWiresGlobalConnSlots(t *testing.T) {
	m := NewTorrentManager()
	sess := newCapTestSession(t)
	m.AddSession(fmt.Sprintf("%x", sess.Torrent.InfoHash), sess)

	if sess.globalOutboundSlots != m.globalOutboundSlots {
		t.Error("session globalOutboundSlots should be the manager's shared channel")
	}
	if sess.globalInboundSlots != m.globalInboundSlots {
		t.Error("session globalInboundSlots should be the manager's shared channel")
	}
	if got := cap(sess.globalOutboundSlots); got != maxGlobalOutboundPeers {
		t.Errorf("globalOutboundSlots cap = %d, want %d", got, maxGlobalOutboundPeers)
	}
	if got := cap(sess.globalInboundSlots); got != maxGlobalInboundPeers {
		t.Errorf("globalInboundSlots cap = %d, want %d", got, maxGlobalInboundPeers)
	}
}

// TestConnectToPeerRespectsOutboundCap verifies that connectToPeer bails immediately
// (without dialing) when the per-session outbound semaphore is exhausted. A real dial
// to the non-routable test address would block on the dialer timeout, so a prompt
// return demonstrates the cap short-circuits before any socket work.
func TestConnectToPeerRespectsOutboundCap(t *testing.T) {
	sess := newCapTestSession(t)
	for i := 0; i < cap(sess.outboundSlots); i++ {
		sess.outboundSlots <- struct{}{}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		sess.connectToPeer(tracker.Peer{IP: net.ParseIP("203.0.113.1"), Port: 9}) // RFC 5737 TEST-NET-3
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("connectToPeer attempted to dial despite the outbound cap being full")
	}

	if got := len(sess.outboundSlots); got != cap(sess.outboundSlots) {
		t.Errorf("outbound slot count changed after a capped bail: got %d, want %d", got, cap(sess.outboundSlots))
	}
}

// TestHandleIncomingConnectionRespectsInboundCap verifies that an inbound connection is
// dropped immediately when the inbound semaphore is exhausted. Uses net.Pipe so the test
// is fully in-memory and deterministic: if the cap were not enforced, the handler would
// block reading a handshake that never arrives until its 10s deadline.
func TestHandleIncomingConnectionRespectsInboundCap(t *testing.T) {
	sess := newCapTestSession(t)
	for i := 0; i < cap(sess.inboundSlots); i++ {
		sess.inboundSlots <- struct{}{}
	}

	client, server := net.Pipe()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		sess.handleIncomingConnection(server)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleIncomingConnection did not drop the connection when the inbound cap was full")
	}

	if got := len(sess.inboundSlots); got != cap(sess.inboundSlots) {
		t.Errorf("inbound slot count changed after a capped drop: got %d, want %d", got, cap(sess.inboundSlots))
	}
}

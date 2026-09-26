package downloader

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/tracker"
)

// admissionConn is one runPeerMessageLoop connection over net.Pipe whose remote
// end drains everything the loop sends.
type admissionConn struct {
	client *peer.Client
	remote net.Conn
	done   chan struct{}
}

func startAdmissionConn(t *testing.T, sess *Session, ip string, port uint16, remoteID [20]byte) *admissionConn {
	t.Helper()
	local, remote := net.Pipe()
	client := peer.NewClient(local, sess.Torrent.InfoHash, sess.PeerID)
	client.RemotePeerID = remoteID
	c := &admissionConn{client: client, remote: remote, done: make(chan struct{})}
	go func() { _, _ = io.Copy(io.Discard, remote) }()
	addr := net.JoinHostPort(ip, strconv.Itoa(int(port)))
	go func() {
		sess.runPeerMessageLoop(client, local, addr, ip, port, fastReserved(), false)
		close(c.done)
	}()
	t.Cleanup(func() {
		_ = remote.Close()
		select {
		case <-c.done:
		case <-time.After(5 * time.Second):
			t.Error("peer loop did not exit")
		}
	})
	return c
}

// rejected fails unless the loop refused the connection and returned.
func (c *admissionConn) rejected(t *testing.T) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		t.Fatal("connection was admitted, want it rejected")
	}
}

// admitted fails unless client becomes the active connection for addr.
func admitted(t *testing.T, sess *Session, addr string, client *peer.Client) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		sess.mu.RLock()
		active := sess.activePeers[addr]
		sess.mu.RUnlock()
		if active == client {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("connection %s was not admitted", addr)
		}
		time.Sleep(time.Millisecond)
	}
}

func peerIDFor(i int) [20]byte {
	var id [20]byte
	copy(id[:], fmt.Sprintf("-TT0001-%012d", i))
	return id
}

func withPeerID(sess *Session) *Session {
	sess.PeerID = peerIDFor(999999)
	return sess
}

// One host could take every inbound slot by connecting from different source
// ports. It gets maxConnectionsPerIP; IPv6 hosts are counted per /64, and
// loopback is exempt.
func TestConnectionsPerHostAreCapped(t *testing.T) {
	cases := []struct {
		name     string
		ip       func(i int) string
		overflow string // a further address counted with the first ones, or "" if exempt
		other    string // an address counted separately
	}{
		{"ipv4", func(i int) string { return "10.1.2.3" }, "10.1.2.3", "10.1.2.4"},
		{"ipv6 /64", func(i int) string { return fmt.Sprintf("2001:db8:1:2::%x", i+1) }, "2001:db8:1:2:ffff::1", "2001:db8:1:3::1"},
		{"loopback", func(i int) string { return "127.0.0.1" }, "", "127.0.0.1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sess := newWireTestSession(t, 4, 16*1024)
			port := uint16(7700)
			for i := 0; i < maxConnectionsPerIP; i++ {
				c := startAdmissionConn(t, sess, tc.ip(i), port, peerIDFor(int(port)))
				admitted(t, sess, net.JoinHostPort(tc.ip(i), strconv.Itoa(int(port))), c.client)
				port++
			}
			if tc.overflow != "" {
				startAdmissionConn(t, sess, tc.overflow, port, peerIDFor(int(port))).rejected(t)
				port++
			}
			c := startAdmissionConn(t, sess, tc.other, port, peerIDFor(int(port)))
			admitted(t, sess, net.JoinHostPort(tc.other, strconv.Itoa(int(port))), c.client)
		})
	}
}

// A slot freed by a closed connection is available again.
func TestHostCapReleasedOnDisconnect(t *testing.T) {
	sess := newWireTestSession(t, 4, 16*1024)
	var first *admissionConn
	for i := 0; i < maxConnectionsPerIP; i++ {
		c := startAdmissionConn(t, sess, "10.9.9.9", uint16(7800+i), peerIDFor(7800+i))
		admitted(t, sess, fmt.Sprintf("10.9.9.9:%d", 7800+i), c.client)
		if first == nil {
			first = c
		}
	}
	_ = first.remote.Close()
	<-first.done
	c := startAdmissionConn(t, sess, "10.9.9.9", 7810, peerIDFor(7810))
	admitted(t, sess, "10.9.9.9:7810", c.client)
}

// A second connection from a peer ID we are already connected to (the same peer
// dialling in while we dial out) is refused, as is a connection that carries our
// own peer ID.
func TestDuplicateAndSelfPeerIDsAreRejected(t *testing.T) {
	sess := withPeerID(newWireTestSession(t, 4, 16*1024))
	c := startAdmissionConn(t, sess, "127.0.0.1", 7900, peerIDFor(1))
	admitted(t, sess, "127.0.0.1:7900", c.client)

	startAdmissionConn(t, sess, "127.0.0.1", 7901, peerIDFor(1)).rejected(t)
	startAdmissionConn(t, sess, "127.0.0.1", 7902, sess.PeerID).rejected(t)

	sess.mu.RLock()
	defer sess.mu.RUnlock()
	if len(sess.activePeers) != 1 {
		t.Fatalf("%d active connections, want only the first", len(sess.activePeers))
	}
}

// A second connection under an address key that is already active (uTP inbound
// and outbound share one) must not take over the first one's activePeers entry
// and PeerState.
func TestActiveConnectionIsNotOverwritten(t *testing.T) {
	sess := newWireTestSession(t, 4, 16*1024)
	first := startAdmissionConn(t, sess, "127.0.0.1", 7950, peerIDFor(1))
	admitted(t, sess, "127.0.0.1:7950", first.client)

	startAdmissionConn(t, sess, "127.0.0.1", 7950, peerIDFor(2)).rejected(t)

	sess.mu.RLock()
	active := sess.activePeers["127.0.0.1:7950"]
	ps := sess.Peers["127.0.0.1:7950"]
	sess.mu.RUnlock()
	if active != first.client || ps == nil || !ps.Active {
		t.Fatal("the rejected connection displaced the active one")
	}
}

// startDirectedConnA1 is startAdmissionConn with a choice of direction: an
// outbound connection runs as connectToPeer runs it after a successful dial.
func startDirectedConnA1(t *testing.T, sess *Session, ip string, port uint16, remoteID [20]byte, outbound bool) *admissionConn {
	t.Helper()
	local, remote := net.Pipe()
	client := peer.NewClient(local, sess.Torrent.InfoHash, sess.PeerID)
	client.RemotePeerID = remoteID
	c := &admissionConn{client: client, remote: remote, done: make(chan struct{})}
	go func() { _, _ = io.Copy(io.Discard, remote) }()
	addr := net.JoinHostPort(ip, strconv.Itoa(int(port)))
	go func() {
		sess.runPeerMessageLoop(client, local, addr, ip, port, fastReserved(), outbound)
		close(c.done)
	}()
	t.Cleanup(func() {
		_ = remote.Close()
		select {
		case <-c.done:
		case <-time.After(5 * time.Second):
			t.Error("peer loop did not exit")
		}
	})
	return c
}

// knownDialedPeerA1 records addr as connectToPeer leaves a peer it just
// handshook with: dialable, failure count cleared, attempted long ago.
func knownDialedPeerA1(sess *Session, ip string, port uint16) *PeerState {
	ps := &PeerState{IP: ip, Port: port, Dialable: true, AmChoking: true, Choked: true, LastAttempt: time.Now().Add(-time.Hour)}
	sess.mu.Lock()
	sess.Peers[net.JoinHostPort(ip, strconv.Itoa(int(port)))] = ps
	sess.mu.Unlock()
	return ps
}

// An outbound connection refused at admission (the host is at its connection
// cap, or the peer ID is connected already) counts as a failed attempt, so the
// address backs off like one that failed to dial instead of being redialled
// every peerRedialBackoff.
func TestRefusedOutboundConnectionBacksOff(t *testing.T) {
	t.Run("per_ip_limit", func(t *testing.T) {
		sess := newWireTestSession(t, 4, 16*1024)
		for i := 0; i < maxConnectionsPerIP; i++ {
			c := startAdmissionConn(t, sess, "10.3.3.3", uint16(8100+i), peerIDFor(8100+i))
			admitted(t, sess, fmt.Sprintf("10.3.3.3:%d", 8100+i), c.client)
		}
		ps := knownDialedPeerA1(sess, "10.3.3.3", 6881)
		startDirectedConnA1(t, sess, "10.3.3.3", 6881, peerIDFor(8199), true).rejected(t)
		sess.mu.RLock()
		defer sess.mu.RUnlock()
		if ps.FailCount != 1 || time.Since(ps.LastAttempt) > time.Minute {
			t.Fatalf("after a per_ip_limit refusal: FailCount %d, LastAttempt %v ago; want 1, just now", ps.FailCount, time.Since(ps.LastAttempt))
		}
	})
	t.Run("duplicate_peer_id", func(t *testing.T) {
		sess := withPeerID(newWireTestSession(t, 4, 16*1024))
		c := startAdmissionConn(t, sess, "10.4.4.4", 8200, peerIDFor(1))
		admitted(t, sess, "10.4.4.4:8200", c.client)
		ps := knownDialedPeerA1(sess, "10.5.5.5", 6881)
		startDirectedConnA1(t, sess, "10.5.5.5", 6881, peerIDFor(1), true).rejected(t)
		sess.mu.RLock()
		defer sess.mu.RUnlock()
		if ps.FailCount != 1 || time.Since(ps.LastAttempt) > time.Minute {
			t.Fatalf("after a duplicate_peer_id refusal: FailCount %d, LastAttempt %v ago; want 1, just now", ps.FailCount, time.Since(ps.LastAttempt))
		}
	})
}

// When we dial a peer while it dials us, both connections carry the same peer ID
// from the same host. Refusing the second one on both ends dropped both; now each
// end keeps the connection opened by the side with the greater peer ID, whichever
// arrived first, and the loser's exit leaves the winner's ID entry in place. The
// survivor is worked out with libtorrent's rule from the remote's side, so a
// libtorrent peer that resolves the duplicate by ID closes the same connection.
func TestSimultaneousOpenKeepsOneConnection(t *testing.T) {
	lowID := peerIDFor(1) // "-TT0001-000000000001", below withPeerID's
	var highID [20]byte
	copy(highID[:], "-ZZ0001-000000000001")
	for _, tc := range []struct {
		name          string
		remoteID      [20]byte
		firstOutbound bool
	}{
		{"we are lower, outbound first", highID, true},
		{"we are lower, inbound first", highID, false},
		{"we are higher, outbound first", lowID, true},
		{"we are higher, inbound first", lowID, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := withPeerID(newWireTestSession(t, 4, 16*1024))
			// libtorrent keeps a new connection over an existing one with the
			// same ID iff (pid < our_peer_id) == is_outgoing()
			// (bt_peer_connection.cpp). On the remote's side pid is our ID,
			// our_peer_id its own, and our outbound connection is incoming, so
			// it keeps its own outgoing connection (our inbound one) iff our ID
			// is the lower.
			remoteKeepsItsOutgoing := string(sess.PeerID[:]) < string(tc.remoteID[:])
			const ip = "10.6.6.6"
			ports := map[bool]uint16{true: 6881, false: 51413} // outbound: listen port; inbound: source port
			if tc.firstOutbound {
				knownDialedPeerA1(sess, ip, ports[true])
			}
			first := startDirectedConnA1(t, sess, ip, ports[tc.firstOutbound], tc.remoteID, tc.firstOutbound)
			firstAddr := net.JoinHostPort(ip, strconv.Itoa(int(ports[tc.firstOutbound])))
			admitted(t, sess, firstAddr, first.client)

			secondOutbound := !tc.firstOutbound
			if secondOutbound {
				knownDialedPeerA1(sess, ip, ports[true])
			}
			second := startDirectedConnA1(t, sess, ip, ports[secondOutbound], tc.remoteID, secondOutbound)
			secondAddr := net.JoinHostPort(ip, strconv.Itoa(int(ports[secondOutbound])))

			// We must keep the connection the remote keeps.
			ourOutboundWins := !remoteKeepsItsOutgoing
			winner, winnerAddr, loser := second, secondAddr, first
			if secondOutbound != ourOutboundWins {
				winner, winnerAddr, loser = first, firstAddr, second
			}
			if winner == second {
				admitted(t, sess, secondAddr, second.client)
			}
			loser.rejected(t) // its loop has exited
			select {
			case <-winner.done:
				t.Fatal("the winning connection was closed too")
			default:
			}

			sess.mu.RLock()
			defer sess.mu.RUnlock()
			if len(sess.activePeers) != 1 || sess.activePeers[winnerAddr] != winner.client {
				t.Fatalf("active connections %v, want only %s", len(sess.activePeers), winnerAddr)
			}
			if owner, ok := sess.admission.peerIDs[tc.remoteID]; !ok || owner.addr != winnerAddr {
				t.Fatalf("peer ID owner %+v (present %v), want %s", owner, ok, winnerAddr)
			}
			if n := sess.admission.perHost[ip]; n != 1 {
				t.Fatalf("host counted %d times, want 1", n)
			}
		})
	}
}

// Only a simultaneous open (same host, other direction) may replace a connection:
// the same peer ID from another host, or from the same host in the same
// direction, is refused, so a peer spoofing an ID cannot evict its owner.
func TestDuplicatePeerIDFromAnotherHostIsRefused(t *testing.T) {
	lowID := peerIDFor(1) // below withPeerID's, so our outbound would win a tie-break
	sess := withPeerID(newWireTestSession(t, 4, 16*1024))
	owner := startAdmissionConn(t, sess, "10.7.7.7", 51413, lowID)
	admitted(t, sess, "10.7.7.7:51413", owner.client)

	knownDialedPeerA1(sess, "10.8.8.8", 6881)
	startDirectedConnA1(t, sess, "10.8.8.8", 6881, lowID, true).rejected(t)
	startAdmissionConn(t, sess, "10.7.7.7", 51414, lowID).rejected(t)

	select {
	case <-owner.done:
		t.Fatal("a duplicate peer ID evicted the connection that owns it")
	default:
	}
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	if o := sess.admission.peerIDs[lowID]; o.addr != "10.7.7.7:51413" {
		t.Fatalf("peer ID owner %+v, want the first connection", o)
	}
}

// Dialling an address that answers with our own peer ID (our listener, handed
// back by a tracker or the DHT) is detected, and the address is not dialled again.
func TestSelfDialIsRememberedAndNotRepeated(t *testing.T) {
	sess := withPeerID(newWireTestSession(t, 4, 16*1024))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var accepts atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				if _, err := peer.ParseHandshake(conn); err != nil {
					return
				}
				resp := &peer.Handshake{Pstr: "BitTorrent protocol", InfoHash: sess.Torrent.InfoHash, PeerID: sess.PeerID}
				_, _ = conn.Write(resp.Serialize())
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	target := tracker.Peer{IP: addr.IP, Port: uint16(addr.Port)}

	sess.connectToPeer(target)
	if n := accepts.Load(); n != 1 {
		t.Fatalf("first dial made %d connections, want 1", n)
	}
	sess.mu.RLock()
	active := len(sess.activePeers)
	sess.mu.RUnlock()
	if active != 0 {
		t.Fatal("a connection to ourselves was admitted")
	}

	sess.connectToPeer(target)
	time.Sleep(50 * time.Millisecond)
	if n := accepts.Load(); n != 1 {
		t.Fatalf("our own address was dialled again (%d connections)", n)
	}
}

// A uTP peer sends from its listen port, so a connection it opens to us while
// our dial to it is in flight is keyed like that dial. The dial failing must
// leave the live connection active (the choker, stats and dial gating skip
// inactive entries) and charge the peer no failure; once the connection is
// gone a failed dial counts again.
func TestFailedDialKeepsInboundConnectionUnderSameKey(t *testing.T) {
	sess := newWireTestSession(t, 4, 16*1024)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	closedAddr := ln.Addr().(*net.TCPAddr)
	_ = ln.Close() // dials to it are refused at once
	ip, port := "127.0.0.1", uint16(closedAddr.Port)
	addr := net.JoinHostPort(ip, strconv.Itoa(int(port)))
	target := tracker.Peer{IP: closedAddr.IP, Port: port}

	ps := knownDialedPeerA1(sess, ip, port)
	sess.mu.Lock()
	ps.Dialing = true // as maintenance leaves it when it launches the dial
	sess.mu.Unlock()
	c := startAdmissionConn(t, sess, ip, port, peerIDFor(1))
	admitted(t, sess, addr, c.client)

	sess.connectToPeer(target)
	sess.mu.RLock()
	active, dialing, failCount := ps.Active, ps.Dialing, ps.FailCount
	live := sess.activePeers[addr] == c.client
	sess.mu.RUnlock()
	if !live || !active || dialing || failCount != 0 {
		t.Fatalf("after a failed dial beside a live inbound conn: live=%v Active=%v Dialing=%v FailCount=%d; want true, true, false, 0", live, active, dialing, failCount)
	}
	if n := len(sess.GetActivePeers()); n != 1 {
		t.Fatalf("GetActivePeers lists %d peers, want the live connection", n)
	}

	_ = c.remote.Close()
	<-c.done
	sess.connectToPeer(target)
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	if ps.Active || ps.FailCount != 1 {
		t.Fatalf("after a failed dial with no connection: Active=%v FailCount=%d; want false, 1", ps.Active, ps.FailCount)
	}
}

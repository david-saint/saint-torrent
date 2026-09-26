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

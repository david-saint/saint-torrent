package downloader

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// startCorruptSeed runs a connection from ip whose peer claims every piece,
// unchokes us and answers each request with zeros, so every piece it supplies
// fails its hash check.
func startCorruptSeed(t *testing.T, sess *Session, ip string, port uint16) chan struct{} {
	t.Helper()
	local, remote := net.Pipe()
	client := peer.NewClient(local, sess.Torrent.InfoHash, sess.PeerID)
	client.RemotePeerID = peerIDFor(int(port))
	done := make(chan struct{})
	go func() {
		sess.runPeerMessageLoop(client, local, net.JoinHostPort(ip, strconv.Itoa(int(port))), ip, port, fastReserved(), false)
		close(done)
	}()
	requests := make(chan []byte, 1024)
	go func() {
		defer close(requests)
		for {
			msg, err := peer.ParseMessage(remote)
			if err != nil {
				return
			}
			if msg != nil && msg.ID == peer.MsgRequest {
				requests <- msg.Payload
			}
		}
	}()
	go func() {
		_, _ = remote.Write((&peer.Message{ID: peer.MsgHaveAll}).Serialize())
		_, _ = remote.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize())
		for req := range requests {
			length := binary.BigEndian.Uint32(req[8:12])
			payload := make([]byte, 8+length)
			copy(payload, req[0:8])
			if _, err := remote.Write((&peer.Message{ID: peer.MsgPiece, Payload: payload}).Serialize()); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		_ = remote.Close()
		<-done
	})
	return done
}

func waitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: connection still open", what)
	}
}

// A peer whose piece failed the hash check was only disconnected, so it could
// reconnect at once and do it again. A host is banned on its second offending
// connection: its open connections are closed and new ones refused, while other
// hosts are unaffected.
func TestHashFailuresBanTheHost(t *testing.T) {
	sess := newWireTestSession(t, 4, 16*1024)
	const bad = "10.20.30.40"

	waitDone(t, startCorruptSeed(t, sess, bad, 8001), "first corrupt seed")
	idle := startAdmissionConn(t, sess, bad, 8002, peerIDFor(8002))
	admitted(t, sess, net.JoinHostPort(bad, "8002"), idle.client) // one strike is forgiven

	waitDone(t, startCorruptSeed(t, sess, bad, 8003), "second corrupt seed")
	waitDone(t, idle.done, "idle connection from the banned host")
	startAdmissionConn(t, sess, bad, 8004, peerIDFor(8004)).rejected(t)

	other := startAdmissionConn(t, sess, "10.20.30.41", 8005, peerIDFor(8005))
	admitted(t, sess, "10.20.30.41:8005", other.client)
}

// A banned host is neither dialled nor answered.
func TestBannedHostIsNotDialedOrAnswered(t *testing.T) {
	sess := newWireTestSession(t, 4, 16*1024)
	now := time.Now()
	sess.mu.Lock()
	for i := 0; i < hashFailStrikesToBan; i++ {
		sess.strikePeerHostLocked("10.20.30.40", now)
	}
	sess.Peers["10.20.30.40:6881"] = &PeerState{IP: "10.20.30.40", Port: 6881, Dialable: true}
	sess.started = true
	sess.mu.Unlock()

	sess.maintainPeerConnections()
	sess.mu.RLock()
	dialing := sess.Peers["10.20.30.40:6881"].Dialing
	sess.mu.RUnlock()
	if dialing {
		t.Fatal("the maintenance loop dialled a banned host")
	}

	local, remote := net.Pipe()
	defer remote.Close()
	conn := &remoteAddrConn{Conn: local, remote: &net.TCPAddr{IP: net.ParseIP("10.20.30.40"), Port: 5555}}
	served := make(chan struct{})
	go func() {
		sess.serveIncomingConnection(conn, &peer.Handshake{Pstr: "BitTorrent protocol", InfoHash: sess.Torrent.InfoHash, PeerID: peerIDFor(1)})
		close(served)
	}()
	select {
	case <-served:
	case <-time.After(2 * time.Second):
		t.Fatal("answered a connection from a banned host")
	}
}

type remoteAddrConn struct {
	net.Conn
	remote net.Addr
}

func (c *remoteAddrConn) RemoteAddr() net.Addr { return c.remote }

// Strikes are forgotten after peerBanDuration, a ban lasts peerBanDuration, and
// the strike table stays bounded however many hosts offend.
func TestStrikesExpireAndStayBounded(t *testing.T) {
	sess := newWireTestSession(t, 1, 16*1024)
	t0 := time.Now()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	a := &sess.admission

	if sess.strikePeerHostLocked("10.0.0.1", t0) {
		t.Fatal("banned on the first strike")
	}
	if sess.strikePeerHostLocked("10.0.0.1", t0.Add(peerBanDuration+time.Minute)) {
		t.Fatal("a strike older than peerBanDuration still counted")
	}
	banAt := t0.Add(peerBanDuration + 2*time.Minute)
	if !sess.strikePeerHostLocked("10.0.0.1", banAt) {
		t.Fatal("two strikes within peerBanDuration did not ban")
	}
	if !a.bannedLocked("10.0.0.1", banAt.Add(peerBanDuration-time.Second)) {
		t.Fatal("ban ended early")
	}
	if a.bannedLocked("10.0.0.1", banAt.Add(peerBanDuration+time.Second)) {
		t.Fatal("ban did not expire")
	}

	for i := 0; i < 3*maxStrikeEntries; i++ {
		sess.strikePeerHostLocked(fmt.Sprintf("10.1.%d.%d", i/256, i%256), banAt)
	}
	if n := len(a.strikes); n > maxStrikeEntries {
		t.Fatalf("strike table holds %d hosts, want at most %d", n, maxStrikeEntries)
	}
}

package downloader

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// An inbound TCP peer is keyed by its ephemeral source port. Once its
// extension handshake names its listen port (BEP 10 p), a tracker, DHT, PEX
// or maintenance pass must not dial that endpoint while the connection lasts:
// libtorrent and Transmission drop one of two connections to the same peer,
// possibly the established one. The endpoint is recorded as a known peer and
// becomes dialable again once the inbound connection ends.
func TestInboundListenPortIsNotRedialed(t *testing.T) {
	var dials atomic.Int32
	oldDial := peerTCPDial
	peerTCPDial = func(context.Context, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("test: dial refused")
	}
	t.Cleanup(func() { peerTCPDial = oldDial })

	sess := newWireTestSession(t, 4, 16*1024)
	sess.mu.Lock()
	sess.started = true
	sess.mu.Unlock()

	w := startWirePeer(t, sess, 50000, fastReserved())
	hs, err := (&peer.ExtensionHandshake{Extensions: map[string]int{}, ListenPort: 6881}).Serialize()
	if err != nil {
		t.Fatalf("serialize extension handshake: %v", err)
	}
	w.sendExtended(peer.ExtHandshake, hs)
	w.barrier()

	const listenAddr = "127.0.0.1:6881"
	sess.connectTrackerPeers([]netip.AddrPort{netip.MustParseAddrPort(listenAddr)})
	sess.addPeer(listenAddr, true)
	sess.maintainPeerConnections()

	// A launched dial sets Dialing before its goroutine starts and, failing,
	// charges a failure before clearing it: neither may have happened.
	sess.mu.RLock()
	ps := sess.Peers[listenAddr]
	held := sess.heldByInboundLocked(listenAddr)
	var dialing, dialable bool
	var failCount uint8
	if ps != nil {
		dialing, dialable, failCount = ps.Dialing, ps.Dialable, ps.FailCount
	}
	sess.mu.RUnlock()
	if !held || ps == nil || !dialable || dialing || failCount != 0 || dials.Load() != 0 {
		t.Fatalf("listen endpoint of a connected inbound peer: held=%v recorded=%v Dialable=%v Dialing=%v FailCount=%d dials=%d; want it held and recorded, undialed",
			held, ps != nil, dialable, dialing, failCount, dials.Load())
	}

	// Once the connection ends the endpoint is an ordinary known peer, dialed
	// after the usual backoff.
	w.close()
	sess.mu.Lock()
	held = sess.heldByInboundLocked(listenAddr)
	sinceLast := time.Since(ps.LastAttempt)
	ps.LastAttempt = time.Now().Add(-time.Hour) // the backoff has run out
	sess.mu.Unlock()
	if held || sinceLast > time.Minute {
		t.Fatalf("after the inbound connection ended: held=%v, LastAttempt %v ago; want released, just now", held, sinceLast)
	}
	sess.maintainPeerConnections()
	waitForCondition(t, "maintenance to dial the released endpoint", func() bool { return dials.Load() == 1 })
}

// TestNoteListenPortKeepsOneHolder pins which endpoints an inbound connection
// may hold: not its own key (a uTP peer connects from its listen port), not an
// endpoint another connection already has, and only one at a time.
func TestNoteListenPortKeepsOneHolder(t *testing.T) {
	sess := newWireTestSession(t, 1, 16*1024)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.activePeers["10.1.1.1:6881"] = &peer.Client{} // we dialed this one

	if got := sess.noteListenPortLocked("10.2.2.2:6881", "10.2.2.2", 6881, ""); got != "" {
		t.Errorf("a connection from its listen port holds %q, want nothing", got)
	}
	if got := sess.noteListenPortLocked("10.1.1.1:50000", "10.1.1.1", 6881, ""); got != "" {
		t.Errorf("an endpoint we are connected to was taken as %q", got)
	}
	delete(sess.activePeers, "10.1.1.1:6881") // it has no conn for Close to close

	first := sess.noteListenPortLocked("10.3.3.3:50000", "10.3.3.3", 6881, "")
	if first != "10.3.3.3:6881" {
		t.Fatalf("inbound connection holds %q, want 10.3.3.3:6881", first)
	}
	if got := sess.noteListenPortLocked("10.3.3.3:50001", "10.3.3.3", 6881, ""); got != "" {
		t.Errorf("a second connection took the held endpoint as %q", got)
	}
	// A re-sent handshake moves the connection's endpoint.
	moved := sess.noteListenPortLocked("10.3.3.3:50000", "10.3.3.3", 6882, first)
	if moved != "10.3.3.3:6882" || sess.heldByInboundLocked(first) || !sess.heldByInboundLocked(moved) {
		t.Fatalf("re-announced port: holds %q, old held=%v; want 10.3.3.3:6882 only", moved, sess.heldByInboundLocked(first))
	}
	// Only the holder releases an endpoint.
	sess.releaseListenPortLocked("10.3.3.3:50001", moved)
	if !sess.heldByInboundLocked(moved) {
		t.Fatal("another connection released the endpoint")
	}
	sess.releaseListenPortLocked("10.3.3.3:50000", moved)
	if len(sess.admission.listenAddrs) != 0 {
		t.Fatalf("endpoints left after release: %v", sess.admission.listenAddrs)
	}

	v6 := sess.noteListenPortLocked("[2001:db8::7]:50000", "2001:db8::7", 51413, "")
	if want := netip.MustParseAddrPort("[2001:db8::7]:51413").String(); v6 != want {
		t.Fatalf("IPv6 endpoint %q, want the tracker key form %q", v6, want)
	}
}

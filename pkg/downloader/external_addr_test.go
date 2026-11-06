package downloader

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"

	"sainttorrent/pkg/peer"
)

var (
	extTestOurs      = netip.MustParseAddr("203.0.113.9")
	extTestReporterA = netip.MustParseAddr("198.51.100.1")
	extTestReporterB = netip.MustParseAddr("198.51.100.2")
)

// One peer's yourip is not enough to take an address as ours: a hostile peer
// could otherwise stop us dialing any endpoint at our port. A second, distinct
// reporter confirms it; the same reporter again does not.
func TestExternalAddrsNeedDistinctReporters(t *testing.T) {
	e := newExternalAddrs()
	e.report(extTestReporterA, extTestOurs)
	e.report(extTestReporterA, extTestOurs)
	if e.contains(extTestOurs) || e.any() {
		t.Fatal("an address one reporter named was taken as ours")
	}
	e.report(extTestReporterB, extTestOurs)
	if !e.contains(extTestOurs) || !e.contains(netip.MustParseAddr("::ffff:203.0.113.9")) {
		t.Fatal("an address two distinct reporters named was not taken as ours")
	}

	// IPv6 reporters in one /64 are one host.
	ours6 := netip.MustParseAddr("2001:db8:ffff::9")
	e.report(netip.MustParseAddr("2001:db8:1::1"), ours6)
	e.report(netip.MustParseAddr("2001:db8:1::2"), ours6)
	if e.contains(ours6) {
		t.Fatal("two reporters in one /64 confirmed an address")
	}
	e.report(netip.MustParseAddr("2001:db8:2::1"), ours6)
	if !e.contains(ours6) {
		t.Fatal("reporters in two /64s did not confirm an address")
	}
}

// Only public addresses count: a LAN peer reports our LAN address, which no
// tracker or DHT node hands back, and a reporter we reach over a LAN or
// loopback says nothing about our public address.
func TestExternalAddrsIgnoreNonPublic(t *testing.T) {
	e := newExternalAddrs()
	for _, r := range []string{"10.0.0.1", "10.0.0.2", "127.0.0.1"} {
		e.report(netip.MustParseAddr(r), netip.MustParseAddr("192.168.1.5"))
		e.report(netip.MustParseAddr(r), extTestOurs)
	}
	e.setMapped(netip.MustParseAddr("100.64.0.1")) // a CGNAT gateway's "external" address
	if e.any() {
		t.Fatal("a non-public report or mapping was recorded")
	}
	e.setMapped(extTestOurs)
	if !e.contains(extTestOurs) {
		t.Fatal("a NAT-mapped public address was not recorded")
	}
}

// Each network change adds an address; only the newest maxExternalAddrs are
// kept, and the candidate table stays bounded.
func TestExternalAddrsAreBounded(t *testing.T) {
	e := newExternalAddrs()
	var addrs []netip.Addr
	for i := range maxExternalAddrs + 2 {
		a := netip.AddrFrom4([4]byte{203, 0, 113, byte(10 + i)})
		addrs = append(addrs, a)
		e.setMapped(a)
	}
	if e.contains(addrs[0]) || e.contains(addrs[1]) {
		t.Fatal("the oldest addresses were kept past the cap")
	}
	for _, a := range addrs[2:] {
		if !e.contains(a) {
			t.Fatalf("recent address %v was dropped", a)
		}
	}

	for i := range 4 * maxExternalAddrCandidates {
		e.report(extTestReporterA, netip.AddrFrom4([4]byte{198, 18, byte(i >> 8), byte(i)}))
	}
	e.mu.Lock()
	n := len(e.votes)
	e.mu.Unlock()
	if n > maxExternalAddrCandidates {
		t.Fatalf("%d candidates held, want at most %d", n, maxExternalAddrCandidates)
	}
}

func TestExternalAddrsNilKnowsNothing(t *testing.T) {
	var e *externalAddrs
	e.report(extTestReporterA, extTestOurs)
	e.setMapped(extTestOurs)
	if e.any() || e.contains(extTestOurs) || e.isOwnEndpoint("203.0.113.9:51413", 51413) {
		t.Fatal("a nil externalAddrs knew an address")
	}
}

// Behind a NAT with no port mapping, trackers list our own public endpoint.
// Once two peers' yourip agree on that address, neither the tracker nor the
// discovery path dials it at the port we advertise, while another port on the
// same address is still dialed, and the manager's DHT filter agrees.
func TestOwnExternalEndpointIsNotDialed(t *testing.T) {
	var mu sync.Mutex
	dialed := map[string]int{}
	oldDial := peerTCPDial
	peerTCPDial = func(_ context.Context, addr string) (net.Conn, error) {
		mu.Lock()
		dialed[addr]++
		mu.Unlock()
		return nil, errors.New("test: dial refused")
	}
	t.Cleanup(func() { peerTCPDial = oldDial })

	ext := newExternalAddrs()
	sess := newWireTestSession(t, 4, 16*1024)
	sess.mu.Lock()
	sess.started = true
	sess.Port = 51413
	sess.extAddrs = ext
	sess.mu.Unlock()

	hs, err := (&peer.ExtensionHandshake{Extensions: map[string]int{}, YourIP: extTestOurs}).Serialize()
	if err != nil {
		t.Fatalf("serialize extension handshake: %v", err)
	}
	for i, r := range []netip.Addr{extTestReporterA, extTestReporterB} {
		w := startWirePeerAtA2(t, sess, r.String(), uint16(40000+i))
		w.sendExtended(peer.ExtHandshake, hs)
		w.barrier()
	}
	if !ext.contains(extTestOurs) {
		t.Fatal("two peers' yourip did not record our external address")
	}

	const own, other = "203.0.113.9:51413", "203.0.113.9:6881"
	sess.connectTrackerPeers([]netip.AddrPort{netip.MustParseAddrPort(own), netip.MustParseAddrPort(other)})
	sess.addPeer(own, true)
	sess.maintainPeerConnections()
	// Dials run in goroutines that clear Dialing when done: wait for every
	// launched dial to finish, then count.
	waitForCondition(t, "the other port to be dialed and every dial to finish", func() bool {
		sess.mu.RLock()
		busy := false
		for _, ps := range sess.Peers {
			busy = busy || ps.Dialing
		}
		sess.mu.RUnlock()
		mu.Lock()
		defer mu.Unlock()
		return dialed[other] > 0 && !busy
	})
	mu.Lock()
	ownDials := dialed[own]
	mu.Unlock()
	if ownDials != 0 {
		t.Fatalf("our own endpoint was dialed %d times", ownDials)
	}

	m := &TorrentManager{advertisedPeerPort: 51413, extAddrs: ext}
	if !m.isOwnPeerEndpointLocked(net.ParseIP("203.0.113.9"), 51413) || m.isOwnPeerEndpointLocked(net.ParseIP("203.0.113.9"), 6881) {
		t.Fatal("the manager's DHT filter disagrees about our endpoint")
	}
}

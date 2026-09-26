package downloader

import (
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/mse"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/torrent"
	"sainttorrent/pkg/utp"
)

func TestManagerSharedListenerRoutesByInfoHash(t *testing.T) {
	mgr := NewTorrentManager()
	if err := mgr.StartPeerListener(0); err != nil {
		t.Fatalf("failed to start shared peer listener: %v", err)
	}
	defer mgr.Close()

	newManagedSession := func(name string) *Session {
		infoHash := sha1.Sum([]byte(name))
		tor := &torrent.Torrent{
			Name:        name,
			InfoHash:    infoHash,
			PieceLength: 1,
			PieceHashes: [][20]byte{sha1.Sum([]byte("x"))},
			Files:       []torrent.File{{Length: 1, Path: []string{name}}},
		}
		sess, err := NewSession(tor, nil, [20]byte{}, 0, t.TempDir())
		if err != nil {
			t.Fatalf("failed to create session: %v", err)
		}
		mgr.AddSession(fmt.Sprintf("%x", infoHash), sess)
		sess.Start()
		return sess
	}

	first := newManagedSession("first")
	second := newManagedSession("second")
	if first.Port == 0 || first.Port != second.Port || first.Port != mgr.PeerListenPort() {
		t.Fatalf("expected sessions to share port %d, got %d and %d",
			mgr.PeerListenPort(), first.Port, second.Port)
	}
	if first.listener != nil || second.listener != nil {
		t.Fatal("managed sessions should not own individual listeners")
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", mgr.PeerListenPort()))
	if err != nil {
		t.Fatalf("failed to dial shared listener: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	handshake := &peer.Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: second.Torrent.InfoHash,
		PeerID:   [20]byte{9, 9, 9},
	}
	if _, err := conn.Write(handshake.Serialize()); err != nil {
		t.Fatalf("failed to write handshake: %v", err)
	}
	response, err := peer.ParseHandshake(conn)
	if err != nil {
		t.Fatalf("failed to read handshake response: %v", err)
	}
	if response.InfoHash != second.Torrent.InfoHash {
		t.Fatalf("expected response for second torrent, got %x", response.InfoHash)
	}

	deadline := time.Now().Add(time.Second)
	for len(second.GetActivePeers()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("shared listener did not route connection to the matching session")
		}
		time.Sleep(time.Millisecond)
	}
	if len(first.GetActivePeers()) != 0 {
		t.Fatal("shared listener routed connection to the wrong session")
	}
}

func TestManagerSharedUTPListenerRoutesByInfoHash(t *testing.T) {
	mgr := NewTorrentManager()
	if err := mgr.StartPeerListener(0); err != nil {
		t.Fatalf("failed to start shared peer listener: %v", err)
	}
	if err := mgr.StartDHT(t.TempDir(), int(mgr.PeerListenPort())); err != nil {
		t.Fatalf("failed to start shared UDP/DHT listener: %v", err)
	}
	defer mgr.Close()

	newManagedSession := func(name string) *Session {
		infoHash := sha1.Sum([]byte(name))
		tor := &torrent.Torrent{
			Name:        name,
			InfoHash:    infoHash,
			PieceLength: 1,
			PieceHashes: [][20]byte{sha1.Sum([]byte("x"))},
			Files:       []torrent.File{{Length: 1, Path: []string{name}}},
		}
		sess, err := NewSession(tor, nil, [20]byte{}, 0, t.TempDir())
		if err != nil {
			t.Fatalf("failed to create session: %v", err)
		}
		mgr.AddSession(fmt.Sprintf("%x", infoHash), sess)
		sess.Start()
		return sess
	}

	first := newManagedSession("first-utp")
	second := newManagedSession("second-utp")
	if mgr.DHTListenPort() == 0 || mgr.DHTListenPort() != mgr.PeerListenPort() {
		t.Fatalf("expected DHT/uTP UDP port to match TCP listen port %d, got %d",
			mgr.PeerListenPort(), mgr.DHTListenPort())
	}
	if first.utpSocket == nil || second.utpSocket == nil {
		t.Fatal("managed sessions did not receive the shared uTP socket")
	}

	clientSocket, err := utp.NewSocket(0)
	if err != nil {
		t.Fatalf("failed to create client uTP socket: %v", err)
	}
	defer clientSocket.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := clientSocket.DialContext(ctx, fmt.Sprintf("127.0.0.1:%d", mgr.DHTListenPort()))
	if err != nil {
		t.Fatalf("failed to dial shared uTP listener: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	handshake := &peer.Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: second.Torrent.InfoHash,
		PeerID:   [20]byte{9, 9, 9},
	}
	if _, err := conn.Write(handshake.Serialize()); err != nil {
		t.Fatalf("failed to write handshake: %v", err)
	}
	response, err := peer.ParseHandshake(conn)
	if err != nil {
		t.Fatalf("failed to read handshake response: %v", err)
	}
	if response.InfoHash != second.Torrent.InfoHash {
		t.Fatalf("expected response for second torrent, got %x", response.InfoHash)
	}

	deadline := time.Now().Add(time.Second)
	for len(second.GetActivePeers()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("shared uTP listener did not route connection to the matching session")
		}
		time.Sleep(time.Millisecond)
	}
	if len(first.GetActivePeers()) != 0 {
		t.Fatal("shared uTP listener routed connection to the wrong session")
	}
}

// TestManagerSharedUTPListenerRoutesEncryptedConnection runs a required-MSE
// handshake over uTP end to end: the dialer's handshake ACK promotes the
// half-open conn, the receiver waits for Ya before answering, and both sides'
// coalesced flights cross a transport whose writes wait for acks.
func TestManagerSharedUTPListenerRoutesEncryptedConnection(t *testing.T) {
	mgr := NewTorrentManager()
	mgr.SetEncryptionPolicy(mse.PolicyRequire)
	if err := mgr.StartPeerListener(0); err != nil {
		t.Fatalf("failed to start shared peer listener: %v", err)
	}
	if err := mgr.StartDHT(t.TempDir(), int(mgr.PeerListenPort())); err != nil {
		t.Fatalf("failed to start shared UDP/DHT listener: %v", err)
	}
	defer mgr.Close()
	sess := newEncryptionTestManagedSession(t, mgr, "encrypted-utp")

	clientSocket, err := utp.NewSocket(0)
	if err != nil {
		t.Fatalf("failed to create client uTP socket: %v", err)
	}
	defer clientSocket.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := clientSocket.DialContext(ctx, fmt.Sprintf("127.0.0.1:%d", mgr.DHTListenPort()))
	if err != nil {
		t.Fatalf("failed to dial shared uTP listener: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	wrapped, _, err := mse.Initiate(conn, sess.Torrent.InfoHash[:], nil, mse.CryptoMethodRC4)
	if err != nil {
		t.Fatalf("MSE initiate over uTP failed: %v", err)
	}
	client := peer.NewClient(wrapped, sess.Torrent.InfoHash, [20]byte{9, 9, 9})
	response, err := client.Handshake()
	if err != nil {
		t.Fatalf("encrypted peer handshake over uTP failed: %v", err)
	}
	if response.InfoHash != sess.Torrent.InfoHash {
		t.Fatalf("expected response for %x, got %x", sess.Torrent.InfoHash, response.InfoHash)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(sess.GetActivePeers()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("shared uTP listener did not route the encrypted connection")
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func dialIdle(t *testing.T, port uint16) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial shared listener: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// dialIdleFrom is dialIdle from another loopback address, so a test can play
// peers on distinct hosts. Linux routes all of 127/8 to lo; where binding
// another loopback address fails, the test is skipped.
func dialIdleFrom(t *testing.T, port uint16, local string) net.Conn {
	t.Helper()
	d := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(local)}}
	conn, err := d.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Skipf("cannot dial from loopback address %s: %v", local, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func handshakeWith(t *testing.T, port uint16, infoHash [20]byte) net.Conn {
	t.Helper()
	return handshakeOver(t, dialIdle(t, port), infoHash)
}

func handshakeOver(t *testing.T, conn net.Conn, infoHash [20]byte) net.Conn {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	hs := &peer.Handshake{Pstr: "BitTorrent protocol", InfoHash: infoHash, PeerID: [20]byte{8, 8, 8}}
	if _, err := conn.Write(hs.Serialize()); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	resp, err := peer.ParseHandshake(conn)
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	if resp.InfoHash != infoHash {
		t.Fatalf("response for %x, want %x", resp.InfoHash, infoHash)
	}
	return conn
}

// TestPreHandshakeConnectionsHoldNoEstablishedSlot reproduces inbound slot
// starvation: connections that never send a handshake used to take one of
// the maxGlobalInboundPeers slots for the whole handshake timeout, so enough
// of them (or spoofed uTP SYNs) turned every real peer away. They now draw
// from the separate handshake budget, and a slot is taken only once a
// handshake names a torrent we serve.
func TestPreHandshakeConnectionsHoldNoEstablishedSlot(t *testing.T) {
	mgr := NewTorrentManager()
	if err := mgr.StartPeerListener(0); err != nil {
		t.Fatalf("start shared listener: %v", err)
	}
	t.Cleanup(mgr.Close) // after the idle conns are closed
	sess := newEncryptionTestManagedSession(t, mgr, "admission")
	port := mgr.PeerListenPort()

	const idle = 20
	for i := 0; i < idle; i++ {
		dialIdle(t, port)
	}
	waitForCondition(t, "idle connections to reach the handshake stage", func() bool {
		return len(mgr.inboundHandshakeSlots) == idle
	})
	if n := len(mgr.globalInboundSlots); n != 0 {
		t.Fatalf("%d idle connections hold %d established slots, want 0", idle, n)
	}

	handshakeWith(t, port, sess.Torrent.InfoHash)
	waitForCondition(t, "the routed peer to hold one established slot", func() bool {
		return len(mgr.globalInboundSlots) == 1 && len(mgr.inboundHandshakeSlots) == idle
	})

	// A handshake for a torrent we do not serve never takes a slot.
	stranger := dialIdle(t, port)
	_ = stranger.SetDeadline(time.Now().Add(3 * time.Second))
	hs := &peer.Handshake{Pstr: "BitTorrent protocol", InfoHash: sha1.Sum([]byte("not ours"))}
	if _, err := stranger.Write(hs.Serialize()); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	if _, err := stranger.Read(make([]byte, 1)); err == nil {
		t.Fatal("handshake for an unknown torrent was answered")
	}
	if n := len(mgr.globalInboundSlots); n != 1 {
		t.Fatalf("established slots = %d after an unknown-torrent handshake, want 1", n)
	}
}

// TestInboundHandshakeBudgetIsBounded checks that pre-handshake connections
// beyond maxInboundHandshakes are turned away at once, and that a freed
// handshake slot admits a real peer again.
func TestInboundHandshakeBudgetIsBounded(t *testing.T) {
	mgr := NewTorrentManager()
	if err := mgr.StartPeerListener(0); err != nil {
		t.Fatalf("start shared listener: %v", err)
	}
	t.Cleanup(mgr.Close) // after the idle conns are closed
	sess := newEncryptionTestManagedSession(t, mgr, "admission-budget")
	port := mgr.PeerListenPort()

	// The idle connections come from many hosts, a few each, as a crowd of
	// distinct peers would, so no one source reaches its share of the budget.
	idle := make([]net.Conn, 0, maxInboundHandshakes)
	for i := 0; i < maxInboundHandshakes; i++ {
		idle = append(idle, dialIdleFrom(t, port, fmt.Sprintf("127.0.1.%d", 1+i/4)))
	}
	waitForCondition(t, "the handshake budget to fill", func() bool {
		return len(mgr.inboundHandshakeSlots) == maxInboundHandshakes
	})

	extra := dialIdleFrom(t, port, "127.0.2.1")
	_ = extra.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := extra.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection beyond the handshake budget was not closed")
	}
	if n := len(mgr.globalInboundSlots); n != 0 {
		t.Fatalf("established slots = %d with only idle connections, want 0", n)
	}

	_ = idle[0].Close()
	waitForCondition(t, "a handshake slot to free up", func() bool {
		return len(mgr.inboundHandshakeSlots) < maxInboundHandshakes
	})
	handshakeWith(t, port, sess.Torrent.InfoHash)
}

// TestOneSourceCannotTakeTheWholeHandshakeBudget reproduces the single-host
// variant of inbound starvation: one machine opening maxInboundHandshakes
// sockets that never send a byte used to hold every handshake slot, so every
// other peer was turned away for as long as it kept reconnecting. Past half
// the budget a source is now held to maxInboundHandshakesPerSource, and a
// peer on another host still gets through.
func TestOneSourceCannotTakeTheWholeHandshakeBudget(t *testing.T) {
	mgr := NewTorrentManager()
	if err := mgr.StartPeerListener(0); err != nil {
		t.Fatalf("start shared listener: %v", err)
	}
	t.Cleanup(mgr.Close) // after the idle conns are closed
	sess := newEncryptionTestManagedSession(t, mgr, "admission-per-source")
	port := mgr.PeerListenPort()

	var refused atomic.Int32
	for i := 0; i < maxInboundHandshakes; i++ {
		conn := dialIdle(t, port)
		go func() {
			// A refused connection is closed at once; an admitted one
			// stays open until the test closes it.
			if _, err := conn.Read(make([]byte, 1)); err != nil {
				refused.Add(1)
			}
		}()
	}
	waitForCondition(t, "the flooding host to be held to half the budget", func() bool {
		return refused.Load() == maxInboundHandshakes/2 && len(mgr.inboundHandshakeSlots) == maxInboundHandshakes/2
	})

	handshakeOver(t, dialIdleFrom(t, port, "127.0.0.2"), sess.Torrent.InfoHash)
}

// TestHandshakeSourceGroupsAddresses pins what one source is: an IPv4 host
// (however it is written), or an IPv6 /64, which a single host can draw any
// number of addresses from.
func TestHandshakeSourceGroupsAddresses(t *testing.T) {
	key := func(addr net.Addr) string {
		t.Helper()
		src, ok := handshakeSource(addr)
		if !ok {
			t.Fatalf("handshakeSource(%v) found no source", addr)
		}
		return src.String()
	}
	for _, tc := range []struct {
		addr net.Addr
		want string
	}{
		{&net.TCPAddr{IP: net.ParseIP("192.0.2.7"), Port: 6881}, "192.0.2.7"},
		{&net.TCPAddr{IP: net.ParseIP("::ffff:192.0.2.7"), Port: 51413}, "192.0.2.7"},
		{&net.UDPAddr{IP: net.ParseIP("192.0.2.7"), Port: 1}, "192.0.2.7"},
		{&net.TCPAddr{IP: net.ParseIP("2001:db8:1:2:aaaa::1"), Port: 6881}, "2001:db8:1:2::"},
		{&net.UDPAddr{IP: net.ParseIP("2001:db8:1:2:bbbb:cccc:dddd:eeee"), Port: 6881}, "2001:db8:1:2::"},
		{&net.TCPAddr{IP: net.ParseIP("2001:db8:1:3::1"), Port: 6881}, "2001:db8:1:3::"},
		{&net.TCPAddr{IP: net.ParseIP("fe80::1"), Port: 6881, Zone: "eth0"}, "fe80::"},
	} {
		if got := key(tc.addr); got != tc.want {
			t.Errorf("handshakeSource(%v) = %s, want %s", tc.addr, got, tc.want)
		}
	}
	if _, ok := handshakeSource(&net.UnixAddr{Name: "sock", Net: "unix"}); ok {
		t.Error("a non-IP address was given a source")
	}
}

// TestRequiredEncryptionRefusesPlaintextAtOnce checks that under
// PolicyRequire a plaintext handshake is refused as soon as it is
// recognised. The MSE receiver reads the initiator's key first, which a
// plaintext peer never sends, so the connection used to sit in a handshake
// slot for the whole peerHandshakeTimeout.
func TestRequiredEncryptionRefusesPlaintextAtOnce(t *testing.T) {
	mgr := NewTorrentManager()
	mgr.SetEncryptionPolicy(mse.PolicyRequire)
	if err := mgr.StartPeerListener(0); err != nil {
		t.Fatalf("start shared listener: %v", err)
	}
	t.Cleanup(mgr.Close)
	sess := newEncryptionTestManagedSession(t, mgr, "require-plaintext")

	conn := dialIdle(t, mgr.PeerListenPort())
	hs := &peer.Handshake{Pstr: "BitTorrent protocol", InfoHash: sess.Torrent.InfoHash, PeerID: [20]byte{5}}
	if _, err := conn.Write(hs.Serialize()); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	start := time.Now()
	_ = conn.SetReadDeadline(time.Now().Add(peerHandshakeTimeout))
	if n, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatalf("plaintext handshake was answered with %d bytes under PolicyRequire", n)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("plaintext peer was held for %v before being refused", waited)
	}
	waitForCondition(t, "the handshake slot to be released", func() bool {
		return len(mgr.inboundHandshakeSlots) == 0
	})
}

func TestManagerSecretKeysSnapshotTracksSessionLifecycle(t *testing.T) {
	mgr := NewTorrentManager()

	first := newEncryptionTestSession(t, "secret-cache-first")
	second := newEncryptionTestSession(t, "secret-cache-second")
	firstHex := fmt.Sprintf("%x", first.Torrent.InfoHash)
	secondHex := fmt.Sprintf("%x", second.Torrent.InfoHash)

	mgr.AddSession(firstHex, first)
	mgr.AddSession(secondHex, second)

	if n := mgr.secretKeys.size(); n != 2 || !hasSecret(&mgr.secretKeys, first.Torrent.InfoHash) || !hasSecret(&mgr.secretKeys, second.Torrent.InfoHash) {
		t.Fatalf("unexpected cached secrets after add: %d entries", n)
	}

	if err := mgr.RemoveSession(firstHex, false); err != nil {
		t.Fatalf("remove first session: %v", err)
	}
	if n := mgr.secretKeys.size(); n != 1 || hasSecret(&mgr.secretKeys, first.Torrent.InfoHash) || !hasSecret(&mgr.secretKeys, second.Torrent.InfoHash) {
		t.Fatalf("unexpected cached secrets after remove: %d entries", n)
	}

	mgr.Close()
	if n := mgr.secretKeys.size(); n != 0 {
		t.Fatalf("expected cached secrets to clear on close, got %d entries", n)
	}
}

// TestSecretKeyIndexLookupAndRefcount checks the obfuscated-hash index: a
// lookup by HASH('req2', infohash) finds the info hash, and an info hash
// added twice stays until removed twice, as the old list of keys did.
func TestSecretKeyIndexLookupAndRefcount(t *testing.T) {
	a := sha1.Sum([]byte("a"))
	b := sha1.Sum([]byte("b"))
	var idx secretKeyIndex
	idx.add(a)
	idx.add(b)
	idx.add(a)
	if got, ok := idx.lookup(mse.ObfuscatedHash(a[:])); !ok || !bytes.Equal(got, a[:]) {
		t.Fatalf("lookup(a) = %x, %v", got, ok)
	}
	if _, ok := idx.lookup(sha1.Sum(a[:])); ok {
		t.Fatal("lookup by the plain info hash must not match")
	}
	idx.remove(a)
	if _, ok := idx.lookup(mse.ObfuscatedHash(a[:])); !ok {
		t.Fatal("info hash added twice was dropped by a single removal")
	}
	idx.remove(a)
	idx.remove(sha1.Sum([]byte("never added")))
	if _, ok := idx.lookup(mse.ObfuscatedHash(a[:])); ok || idx.size() != 1 {
		t.Fatalf("after removals: len=%d, a still present=%v", idx.size(), ok)
	}
}

// TestSecretKeyIndexUpdatesInPlace pins the cost of adding a torrent: the
// index used to be cloned on every add and remove, so restoring N torrents
// copied O(N²) entries under the manager lock (about 1.9 s of clones at 10k
// torrents). An update now changes the one map in place.
func TestSecretKeyIndexUpdatesInPlace(t *testing.T) {
	var idx secretKeyIndex
	for i := 0; i < 1000; i++ {
		idx.add(sha1.Sum([]byte{byte(i), byte(i >> 8)}))
	}
	idx.mu.RLock()
	before := reflect.ValueOf(idx.byHash).Pointer()
	idx.mu.RUnlock()

	extra := sha1.Sum([]byte("extra"))
	idx.add(extra)
	idx.remove(extra)

	idx.mu.RLock()
	after := reflect.ValueOf(idx.byHash).Pointer()
	idx.mu.RUnlock()
	if before != after {
		t.Fatal("adding or removing a torrent replaced the whole index")
	}
}

// TestSecretKeyIndexConcurrentLookup runs handshake lookups against sessions
// being added and removed, the access pattern the index's lock exists for;
// the race detector checks it.
func TestSecretKeyIndexConcurrentLookup(t *testing.T) {
	var idx secretKeyIndex
	stable := sha1.Sum([]byte("stable"))
	idx.add(stable)
	obfuscated := mse.ObfuscatedHash(stable[:])

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			h := sha1.Sum([]byte{byte(i), byte(i >> 8)})
			idx.add(h)
			idx.remove(h)
		}
	}()
	for {
		select {
		case <-done:
			return
		default:
		}
		if got, ok := idx.lookup(obfuscated); !ok || !bytes.Equal(got, stable[:]) {
			t.Fatalf("lookup of a stable torrent = %x, %v", got, ok)
		}
	}
}

func (x *secretKeyIndex) size() int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return len(x.byHash)
}

func hasSecret(idx *secretKeyIndex, want [20]byte) bool {
	got, ok := idx.lookup(mse.ObfuscatedHash(want[:]))
	return ok && bytes.Equal(got, want[:])
}

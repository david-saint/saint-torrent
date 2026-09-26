package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/torrent"
)

func knownPeerState(sess *Session, addr string) (PeerState, bool) {
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	ps, ok := sess.Peers[addr]
	if !ok {
		return PeerState{}, false
	}
	return PeerState{
		LastAttempt: ps.LastAttempt,
		Dialing:     ps.Dialing,
		Dialable:    ps.Dialable,
		Active:      ps.Active,
		Source:      ps.Source,
		FailCount:   ps.FailCount,
	}, true
}

// TestFailingPeerIsNotRedialedEveryMinute covers junk addresses (from DHT, PEX or
// a tracker) that never answer: after maxPeerFailCount failed attempts in a row,
// neither maintenance nor a DHT/PEX re-listing redials one every
// peerRedialBackoff, so they stop holding outbound slots. It is still retried
// after failedPeerRedialBackoff, and a tracker listing earns it another try.
func TestFailingPeerIsNotRedialedEveryMinute(t *testing.T) {
	sess, _, _ := newStallTestTorrent(t, 1)
	defer sess.Close()
	sess.mu.Lock()
	sess.started = true
	sess.mu.Unlock()

	port := refusingLoopbackPort(t)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	sess.mu.Lock()
	sess.Peers[addr] = &PeerState{IP: "127.0.0.1", Port: uint16(port), AmChoking: true, Choked: true, Dialable: true}
	sess.mu.Unlock()

	for i := 0; i < maxPeerFailCount; i++ {
		sess.connectToPeer(trackerPeer("127.0.0.1", uint16(port)))
	}
	if ps, _ := knownPeerState(sess, addr); ps.FailCount != maxPeerFailCount {
		t.Fatalf("FailCount = %d after %d refused dials, want %d", ps.FailCount, maxPeerFailCount, maxPeerFailCount)
	}

	stale := time.Now().Add(-2 * peerRedialBackoff)
	setLastAttempt := func(at time.Time) {
		sess.mu.Lock()
		sess.Peers[addr].LastAttempt = at
		sess.mu.Unlock()
	}
	setLastAttempt(stale)
	sess.maintainPeerConnections()
	sess.AddPeerFromDiscovery(addr)
	if ps, _ := knownPeerState(sess, addr); ps.Dialing || !ps.LastAttempt.Equal(stale) {
		t.Fatal("a peer past maxPeerFailCount was redialed after the normal backoff")
	}

	setLastAttempt(time.Now().Add(-failedPeerRedialBackoff - time.Second))
	sess.maintainPeerConnections()
	if ps, _ := knownPeerState(sess, addr); !ps.Dialing && ps.LastAttempt.Before(time.Now().Add(-time.Minute)) {
		t.Fatal("a failing peer was never retried after failedPeerRedialBackoff")
	}

	ps := &PeerState{FailCount: maxPeerFailCount, Source: PeerSourceDiscovery}
	ps.markTrackerListed()
	if ps.FailCount != maxPeerFailCount-1 || ps.Source != PeerSourceDiscovery|PeerSourceTracker {
		t.Fatalf("after a tracker listing FailCount = %d, Source = %b", ps.FailCount, ps.Source)
	}
}

// TestIPv6DialIsAccountedOnItsEntry covers IPv6 peers from DHT or PEX, which
// addPeer keys as "[ip]:port". connectToPeer looked them up as "ip:port", so a
// dial never found its entry: the entry stayed Dialing (never redialed), its
// failures were never counted, and a successful dial registered a second entry.
func TestIPv6DialIsAccountedOnItsEntry(t *testing.T) {
	sess, _, _ := newStallTestTorrent(t, 1)
	defer sess.Close()
	sess.mu.Lock()
	sess.started = true
	sess.mu.Unlock()

	port := refusingLoopbackPort(t)
	addr := net.JoinHostPort("::1", strconv.Itoa(port))
	sess.mu.Lock()
	sess.Peers[addr] = &PeerState{IP: "::1", Port: uint16(port), AmChoking: true, Choked: true,
		Dialable: true, Dialing: true, LastAttempt: time.Now(), Source: PeerSourceDiscovery}
	sess.mu.Unlock()

	sess.connectToPeer(trackerPeer("::1", uint16(port)))
	ps, ok := knownPeerState(sess, addr)
	if !ok || ps.Dialing || ps.FailCount != 1 {
		t.Fatalf("after a failed dial: known %v, Dialing %v, FailCount %d; want known, not dialing, one failure", ok, ps.Dialing, ps.FailCount)
	}
	sess.mu.RLock()
	known := len(sess.Peers)
	sess.mu.RUnlock()
	if known != 1 {
		t.Fatalf("known peers = %d after dialing one IPv6 peer, want 1", known)
	}
}

// TestSuccessfulHandshakeClearsFailCount checks that failures only count in a
// row: a peer that answers again is a normal redial candidate.
func TestSuccessfulHandshakeClearsFailCount(t *testing.T) {
	sess, _, _ := newStallTestTorrent(t, 1)
	defer sess.Close()
	sess.mu.Lock()
	sess.started = true
	sess.mu.Unlock()

	ln, port := startPEXHandshakePeer(t, nil, false, nil)
	defer ln.Close()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	sess.mu.Lock()
	sess.Peers[addr] = &PeerState{IP: "127.0.0.1", Port: uint16(port), AmChoking: true, Choked: true, Dialable: true, FailCount: maxPeerFailCount - 1}
	sess.mu.Unlock()

	sess.connectToPeer(trackerPeer("127.0.0.1", uint16(port)))
	if ps, _ := knownPeerState(sess, addr); ps.FailCount != 0 {
		t.Fatalf("FailCount = %d after a successful handshake, want 0", ps.FailCount)
	}
}

// TestPrunePeersEvictsFailedPeersFirst covers a flood of dead addresses: when the
// known-peer map overflows, those are evicted before peers that still answer,
// however old their last attempt.
func TestPrunePeersEvictsFailedPeersFirst(t *testing.T) {
	s := &Session{Peers: make(map[string]*PeerState)}
	now := time.Now()
	healthy := maxKnownPeers / 2
	for i := 0; i < healthy; i++ {
		s.Peers[fmt.Sprintf("198.51.100.%d:%d", i%256, 1000+i)] = &PeerState{LastAttempt: now.Add(-24 * time.Hour)}
	}
	for i := 0; i < maxKnownPeers; i++ {
		s.Peers[fmt.Sprintf("203.0.113.%d:%d", i%256, 1000+i)] = &PeerState{LastAttempt: now, FailCount: maxPeerFailCount}
	}

	s.mu.Lock()
	s.prunePeersLocked()
	s.mu.Unlock()

	kept := 0
	for _, ps := range s.Peers {
		if ps.FailCount == 0 {
			kept++
		}
	}
	if kept != healthy {
		t.Fatalf("prune kept %d of %d answering peers, want all", kept, healthy)
	}
}

func privatePurgeInfoDict(t *testing.T) []byte {
	t.Helper()
	infoBytes, err := bencode.Marshal(map[string]interface{}{
		"name":         "private-purge.txt",
		"piece length": int64(1),
		"pieces":       string(make([]byte, 20)),
		"length":       int64(1),
		"private":      int64(1),
	})
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	return infoBytes
}

// acceptCountingListener accepts and counts connections on loopback.
func acceptCountingListener(t *testing.T) (net.Listener, int, func() int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	accepted := make(chan struct{}, 64)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- struct{}{}
			_ = conn.Close()
		}
	}()
	count := func() int {
		n := 0
		for {
			select {
			case <-accepted:
				n++
			case <-time.After(200 * time.Millisecond):
				return n
			}
		}
	}
	return ln, ln.Addr().(*net.TCPAddr).Port, count
}

// TestPrivateMetadataDropsDiscoveryPeers covers a private torrent opened by
// magnet: while its private flag was unknown it took peers from DHT and PEX.
// Once metadata shows it is private, those connections are closed and the
// addresses forgotten, a dial that was already under way is refused, and no
// path (maintenance, Resume, reconnect, direct dial) connects to one again,
// even one that has also connected to us. Tracker-listed peers, and ones that
// only connected to us, are kept (BEP 27).
func TestPrivateMetadataDropsDiscoveryPeers(t *testing.T) {
	infoBytes := privatePurgeInfoDict(t)
	sess, err := NewSession(&torrent.Torrent{Name: "private-purge", InfoHash: sha1.Sum(infoBytes)}, nil, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	defer sess.Close()
	sess.mu.Lock()
	sess.started = true
	sess.mu.Unlock()

	loop := func(port int) string { return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) }
	discovered := loop(refusingLoopbackPort(t))
	discoveredIncoming := loop(refusingLoopbackPort(t))
	listed := loop(refusingLoopbackPort(t))
	incoming := loop(refusingLoopbackPort(t))
	inFlight := loop(refusingLoopbackPort(t))
	sess.mu.Lock()
	add := func(addr string, ps *PeerState) {
		host, portStr, _ := net.SplitHostPort(addr)
		port, _ := strconv.Atoi(portStr)
		ps.IP, ps.Port, ps.AmChoking, ps.Choked = host, uint16(port), true, true
		sess.Peers[addr] = ps
	}
	add(discovered, &PeerState{Dialable: true, Source: PeerSourceDiscovery})
	add(discoveredIncoming, &PeerState{Dialable: true, Source: PeerSourceDiscovery | PeerSourceIncoming})
	add(listed, &PeerState{Dialable: true, Source: PeerSourceDiscovery | PeerSourceTracker})
	add(incoming, &PeerState{Source: PeerSourceIncoming})
	add(inFlight, &PeerState{Dialable: true, Dialing: true, Source: PeerSourceDiscovery})
	connected := loop(7700)
	add(connected, &PeerState{Dialable: true, Source: PeerSourceDiscovery})
	sess.mu.Unlock()

	// A live connection we dialed to a DHT/PEX peer during the metadata fetch.
	runOutbound := func(addr string) (chan struct{}, net.Conn) {
		clientConn, remote := net.Pipe()
		go func() { _, _ = io.Copy(io.Discard, remote) }()
		host, portStr, _ := net.SplitHostPort(addr)
		port, _ := strconv.Atoi(portStr)
		client := peer.NewClient(clientConn, sess.Torrent.InfoHash, sess.PeerID)
		done := make(chan struct{})
		go func() {
			sess.runPeerMessageLoop(client, clientConn, addr, host, uint16(port), [8]byte{}, true)
			close(done)
		}()
		return done, remote
	}
	connDone, connRemote := runOutbound(connected)
	defer connRemote.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		sess.mu.RLock()
		_, active := sess.activePeers[connected]
		sess.mu.RUnlock()
		if active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("discovery connection never became active")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := sess.onMetadataDownloaded(infoBytes); err != nil {
		t.Fatalf("onMetadataDownloaded: %v", err)
	}

	select {
	case <-connDone:
	case <-time.After(5 * time.Second):
		t.Fatal("DHT/PEX connection stayed open after metadata showed the torrent is private")
	}
	for _, addr := range []string{discovered, discoveredIncoming, connected} {
		if sessionKnowsPeer(sess, addr) {
			t.Errorf("DHT/PEX peer %s still known after the private flip", addr)
		}
	}
	for _, addr := range []string{listed, incoming} {
		if !sessionKnowsPeer(sess, addr) {
			t.Errorf("tracker-listed or incoming peer %s was forgotten", addr)
		}
	}

	// The dial that was in flight completes its handshake after the flip.
	sess.mu.Lock()
	sess.Peers[inFlight].Dialing = false
	sess.mu.Unlock()
	lateDone, lateRemote := runOutbound(inFlight)
	defer lateRemote.Close()
	select {
	case <-lateDone:
	case <-time.After(5 * time.Second):
		t.Fatal("a DHT/PEX dial that straddled the private flip was admitted")
	}
	if sessionKnowsPeer(sess, inFlight) {
		t.Error("the refused in-flight DHT/PEX peer is still known")
	}

	// A DHT/PEX entry left over by any path is never dialed again.
	ln, lnPort, accepted := acceptCountingListener(t)
	defer ln.Close()
	leftover := loop(lnPort)
	sess.mu.Lock()
	add(leftover, &PeerState{Dialable: true, Source: PeerSourceDiscovery | PeerSourceIncoming})
	sess.mu.Unlock()
	sess.maintainPeerConnections()
	if ps, ok := knownPeerState(sess, listed); !ok || ps.LastAttempt.IsZero() {
		t.Fatal("maintenance did not dial the tracker-listed peer")
	}
	sess.addPeer(leftover, false)
	sess.Pause()
	sess.Resume()
	sess.maintainPeerConnections()
	sess.connectToPeer(trackerPeer("127.0.0.1", uint16(lnPort)))
	if n := accepted(); n != 0 {
		t.Fatalf("a DHT/PEX peer of a private torrent was dialed %d time(s)", n)
	}
}

// TestTrackerPeerSource pins down which trackers may hand out local peers. It
// takes trackerLogID's output, which is what announceAndConnect passes.
func TestTrackerPeerSource(t *testing.T) {
	cases := map[string]string{
		"http://127.0.0.1:6969/announce":          "127.0.0.1",
		"udp://[::1]:6969/announce":               "::1",
		"http://localhost:6969/announce":          "127.0.0.1",
		"http://LOCALHOST.:6969/announce":         "127.0.0.1",
		"http://192.168.1.5/announce":             "192.168.1.5",
		"udp://tracker.example.org:1337/announce": "",
		"http://localhost.example.org/announce":   "",
		"not a url":                               "",
	}
	for raw, want := range cases {
		got := trackerPeerSource(trackerLogID(raw))
		if (want == "" && got.IsValid()) || (want != "" && got != netip.MustParseAddr(want)) {
			t.Errorf("trackerPeerSource(%q) = %v, want %q", raw, got, want)
		}
	}
}

// TestAnnounceDropsUnusableTrackerPeers covers a tracker listing endpoints that
// can never be a peer, or that are more local than the tracker itself.
func TestAnnounceDropsUnusableTrackerPeers(t *testing.T) {
	oldTimeout := trackerAnnounceTimeout
	trackerAnnounceTimeout = 2 * time.Second
	defer func() { trackerAnnounceTimeout = oldTimeout }()

	usable := refusingLoopbackPort(t)
	var compact []byte
	for _, ep := range []struct {
		ip   string
		port int
	}{
		{"127.0.0.1", usable},
		{"224.0.0.1", 6881},
		{"255.255.255.255", 6881},
		{"0.1.2.3", 6881},
		{"240.0.0.1", 6881},
	} {
		compact = append(compact, net.ParseIP(ep.ip).To4()...)
		compact = binary.BigEndian.AppendUint16(compact, uint16(ep.port))
	}
	resp, err := bencode.Marshal(map[string]interface{}{"interval": int64(1800), "peers": string(compact)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(resp)
	}))
	defer srv.Close()

	sess, err := NewSession(&torrent.Torrent{
		Name:     "tracker-filter",
		InfoHash: sha1.Sum([]byte("tracker-filter")),
		Trackers: []string{srv.URL + "/announce"},
	}, nil, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	defer sess.Close()
	sess.announceAndConnect()

	sess.mu.RLock()
	defer sess.mu.RUnlock()
	want := fmt.Sprintf("127.0.0.1:%d", usable)
	for addr := range sess.Peers {
		if addr != want {
			t.Errorf("tracker endpoint %s was accepted", addr)
		}
	}
	if ps := sess.Peers[want]; ps == nil || ps.Source != PeerSourceTracker {
		t.Fatalf("loopback tracker's loopback peer was not recorded as tracker-sourced: %+v", ps)
	}
}

func TestTrackerPeerAllowed(t *testing.T) {
	public := netip.Addr{}
	loopback := netip.MustParseAddr("127.0.0.1")
	cases := []struct {
		ip     string
		source netip.Addr
		want   bool
	}{
		{"198.51.100.7", public, true},
		{"127.0.0.1", public, false},
		{"::ffff:127.0.0.1", public, false},
		{"169.254.169.254", public, false},
		{"fe80::1", public, false},
		{"192.168.1.1", public, true},
		{"10.1.2.3", public, true},
		{"fd00::1", public, true},
		{"127.0.0.1", loopback, true},
		{"192.168.1.1", loopback, true},
		{"169.254.169.254", loopback, true},
		{"224.0.0.1", loopback, false},
	}
	for _, c := range cases {
		p := trackerPeer(c.ip, 6881)
		if got := trackerPeerAllowed(p, c.source); got != c.want {
			t.Errorf("trackerPeerAllowed(%s, %v) = %v, want %v", c.ip, c.source, got, c.want)
		}
	}
	if trackerPeerAllowed(trackerPeer("198.51.100.7", 0), public) {
		t.Error("port 0 was allowed")
	}
	if trackerPeerAllowed(trackerPeer("192.168.1.1", 0), public) {
		t.Error("a private peer with port 0 was allowed")
	}
}

// TestHostnameTrackerListsLANPeers covers a LAN or company swarm whose tracker
// is reached by name: every peer it lists has a private address, and all of them
// must stay dialable, or nothing downloads. Loopback and link-local peers stay
// refused, since a name tells us nothing about where the tracker is.
func TestHostnameTrackerListsLANPeers(t *testing.T) {
	source := trackerPeerSource(trackerLogID("http://tracker.corp.example:6969/announce"))
	for _, ip := range []string{"10.20.30.40", "172.16.0.9", "192.168.0.7", "fd12:3456::7"} {
		if !trackerPeerAllowed(trackerPeer(ip, 6881), source) {
			t.Errorf("peer %s from a hostname tracker was refused", ip)
		}
	}
	for _, ip := range []string{"127.0.0.1", "169.254.169.254", "0.0.0.1", "239.1.2.3"} {
		if trackerPeerAllowed(trackerPeer(ip, 6881), source) {
			t.Errorf("peer %s from a hostname tracker was allowed", ip)
		}
	}
}

// TestDHTPeerOwnEndpointIsDropped covers DHT nodes echoing our own announce back
// in get_peers values: our NAT-mapped external address with our advertised port
// is never dialed.
func TestDHTPeerOwnEndpointIsDropped(t *testing.T) {
	m := &TorrentManager{advertisedPeerPort: 6881}
	if m.isOwnPeerEndpointLocked(net.ParseIP("203.0.113.5"), 6881) {
		t.Fatal("an endpoint was taken as ours without a known external address")
	}
	m.natStatus.ExternalIP = "203.0.113.5"
	for _, c := range []struct {
		ip   net.IP
		port uint16
		want bool
	}{
		{net.ParseIP("203.0.113.5").To4(), 6881, true},
		{net.ParseIP("::ffff:203.0.113.5"), 6881, true},
		{net.ParseIP("203.0.113.5"), 6882, false},
		{net.ParseIP("203.0.113.6"), 6881, false},
	} {
		if got := m.isOwnPeerEndpointLocked(c.ip, c.port); got != c.want {
			t.Errorf("isOwnPeerEndpointLocked(%v, %d) = %v, want %v", c.ip, c.port, got, c.want)
		}
	}
}

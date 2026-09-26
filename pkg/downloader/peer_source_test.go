package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
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

	ps := &PeerState{FailCount: maxPeerFailCount}
	ps.markTrackerListed()
	if ps.FailCount != maxPeerFailCount-1 {
		t.Fatalf("after a tracker listing FailCount = %d, want %d", ps.FailCount, maxPeerFailCount-1)
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
	if sess.Peers[want] == nil {
		t.Fatal("the loopback tracker's loopback peer was not accepted")
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
		{"192.168.1.1", public, false},
		{"127.0.0.1", loopback, true},
		{"192.168.1.1", loopback, true},
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
}

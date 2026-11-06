package downloader

import (
	"crypto/sha1"
	"errors"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/torrent"
	"sainttorrent/pkg/utp"
)

// holepunchPipePeer is one end of an in-memory peer connection: client is
// the session's side, and every ut_holepunch message written to it shows up
// on msgs with the extended message ID it was sent under.
type holepunchPipePeer struct {
	client *peer.Client
	remote net.Conn
	msgs   chan sentHolepunch
}

type sentHolepunch struct {
	extID byte
	msg   peer.HolepunchMessage
}

func newHolepunchPipePeer(t *testing.T, remoteID [20]byte) *holepunchPipePeer {
	t.Helper()
	ours, theirs := net.Pipe()
	t.Cleanup(func() { _ = ours.Close(); _ = theirs.Close() })
	client := peer.NewClient(ours, [20]byte{}, [20]byte{})
	client.RemotePeerID = remoteID
	p := &holepunchPipePeer{client: client, remote: theirs, msgs: make(chan sentHolepunch, 16)}
	reader := peer.NewClient(theirs, [20]byte{}, [20]byte{})
	go func() {
		for {
			m, err := reader.ReadMessage()
			if err != nil {
				return
			}
			if m == nil || m.ID != peer.MsgExtended || len(m.Payload) < 2 {
				continue
			}
			hp, err := peer.ParseHolepunchMessage(m.Payload[1:])
			if err != nil {
				continue
			}
			p.msgs <- sentHolepunch{extID: m.Payload[0], msg: *hp}
		}
	}()
	return p
}

func (p *holepunchPipePeer) expect(t *testing.T, what string) sentHolepunch {
	t.Helper()
	select {
	case m := <-p.msgs:
		return m
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: no ut_holepunch message", what)
		return sentHolepunch{}
	}
}

func (p *holepunchPipePeer) expectNone(t *testing.T, what string) {
	t.Helper()
	select {
	case m := <-p.msgs:
		t.Fatalf("%s: unexpected ut_holepunch message %+v", what, m)
	case <-time.After(100 * time.Millisecond):
	}
}

func newHolepunchTestSession(t *testing.T) *Session {
	t.Helper()
	tor := &torrent.Torrent{
		Name:        "holepunch",
		InfoHash:    sha1.Sum([]byte("holepunch")),
		PieceLength: 1,
		PieceHashes: [][20]byte{sha1.Sum([]byte("x"))},
		Files:       []torrent.File{{Length: 1, Path: []string{"holepunch"}}},
	}
	sess, err := NewSession(tor, nil, [20]byte{5}, 0, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sess.Close)
	return sess
}

// addLiveTestPeer registers p as a live connection at key, as
// runPeerMessageLoop does.
func addLiveTestPeer(sess *Session, key string, p *holepunchPipePeer, lc liveConn) {
	lc.client = p.client
	sess.mu.Lock()
	sess.activePeers[key] = p.client
	sess.registerLiveConnLocked(key, &lc)
	sess.mu.Unlock()
}

func TestExtensionHandshakeAdvertisesHolepunchUnlessPrivate(t *testing.T) {
	sess := &Session{Torrent: &torrent.Torrent{}}
	sess.mu.Lock()
	extensions := sess.extensionHandshakeMapLocked()
	sess.mu.Unlock()
	if extensions[peer.ExtNameHolepunch] != peer.LocalHolepunchExtID {
		t.Fatalf("ut_holepunch = %d, want %d", extensions[peer.ExtNameHolepunch], peer.LocalHolepunchExtID)
	}
	sess.Torrent.Private = true
	sess.mu.Lock()
	extensions = sess.extensionHandshakeMapLocked()
	sess.mu.Unlock()
	if _, ok := extensions[peer.ExtNameHolepunch]; ok {
		t.Fatal("private torrent advertised ut_holepunch")
	}
}

// TestRelayHolepunchIntroducesBothPeers checks the relay side of BEP 55: a
// rendezvous naming a connected, holepunch-capable target sends each side a
// connect naming the other, on each one's own ut_holepunch ID. The target is
// found by its connection key or by the endpoint it is reachable on, and the
// initiator is introduced at its listen endpoint, not its TCP source port.
func TestRelayHolepunchIntroducesBothPeers(t *testing.T) {
	for _, tc := range []struct {
		name       string
		targetKey  string
		targetLC   liveConn
		rendezvous string // the endpoint the initiator asks for
	}{
		{
			name:       "uTP target by key",
			targetKey:  "203.0.113.9:40000",
			targetLC:   liveConn{utp: true, endpoint: "203.0.113.9:40000", holepunchID: 9},
			rendezvous: "203.0.113.9:40000",
		},
		{
			name:       "TCP target by listen endpoint",
			targetKey:  "203.0.113.9:50001",
			targetLC:   liveConn{endpoint: "203.0.113.9:6881", holepunchID: 9},
			rendezvous: "203.0.113.9:6881",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newHolepunchTestSession(t)
			initiator := newHolepunchPipePeer(t, [20]byte{1})
			target := newHolepunchPipePeer(t, [20]byte{2})
			addLiveTestPeer(sess, "198.51.100.1:5000", initiator, liveConn{endpoint: "198.51.100.1:6881", holepunchID: 7})
			addLiveTestPeer(sess, tc.targetKey, target, tc.targetLC)

			hc := &holepunchConn{key: "198.51.100.1:5000", ip: "198.51.100.1", client: initiator.client, peerID: 7}
			payload, _ := peer.SerializeHolepunchMessage(&peer.HolepunchMessage{
				Type: peer.HolepunchRendezvous,
				Addr: netip.MustParseAddrPort(tc.rendezvous),
			})
			sess.handleHolepunchMessage(hc, payload)

			got := initiator.expect(t, "initiator")
			if got.extID != 7 || got.msg.Type != peer.HolepunchConnect || got.msg.Addr.String() != tc.rendezvous {
				t.Fatalf("initiator got %+v, want connect to %s on ID 7", got, tc.rendezvous)
			}
			got = target.expect(t, "target")
			if got.extID != 9 || got.msg.Type != peer.HolepunchConnect || got.msg.Addr.String() != "198.51.100.1:6881" {
				t.Fatalf("target got %+v, want connect to 198.51.100.1:6881 on ID 9", got)
			}
		})
	}
}

// TestRelayHolepunchErrors checks every BEP 55 error a relay sends back, and
// that the target hears nothing when the introduction fails.
func TestRelayHolepunchErrors(t *testing.T) {
	const initiatorKey = "198.51.100.1:6881"
	for _, tc := range []struct {
		name   string
		target string
		want   peer.HolepunchErrCode
	}{
		{"unknown target", "192.0.2.50:6881", peer.HolepunchErrNotConnected},
		{"target without ut_holepunch", "203.0.113.20:6881", peer.HolepunchErrNoSupport},
		{"port zero", "203.0.113.9:0", peer.HolepunchErrNoSuchPeer},
		{"initiator itself", initiatorKey, peer.HolepunchErrNoSuchPeer},
		{"the relay itself", "192.0.2.99:6881", peer.HolepunchErrNoSelf},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newHolepunchTestSession(t)
			initiator := newHolepunchPipePeer(t, [20]byte{1})
			noSupport := newHolepunchPipePeer(t, [20]byte{3})
			addLiveTestPeer(sess, initiatorKey, initiator, liveConn{outbound: true, endpoint: initiatorKey, holepunchID: 7})
			addLiveTestPeer(sess, "203.0.113.20:6881", noSupport, liveConn{outbound: true, endpoint: "203.0.113.20:6881"})
			sess.mu.Lock()
			sess.admission.rememberSelfAddrLocked("192.0.2.99:6881")
			sess.mu.Unlock()

			hc := &holepunchConn{key: initiatorKey, ip: "198.51.100.1", client: initiator.client, peerID: 7}
			sess.relayHolepunch(hc, netip.MustParseAddrPort(tc.target))
			got := initiator.expect(t, "initiator")
			if got.msg.Type != peer.HolepunchError || got.msg.ErrCode != tc.want || got.msg.Addr.String() != tc.target {
				t.Fatalf("initiator got %+v, want error %v for %s", got.msg, tc.want, tc.target)
			}
			noSupport.expectNone(t, "peer without ut_holepunch")
		})
	}
}

// TestRelayHolepunchNeedsInitiatorID checks that a rendezvous from a peer
// that never advertised ut_holepunch is dropped: there is no ID to answer on.
func TestRelayHolepunchNeedsInitiatorID(t *testing.T) {
	sess := newHolepunchTestSession(t)
	initiator := newHolepunchPipePeer(t, [20]byte{1})
	target := newHolepunchPipePeer(t, [20]byte{2})
	addLiveTestPeer(sess, "198.51.100.1:6881", initiator, liveConn{outbound: true, endpoint: "198.51.100.1:6881"})
	addLiveTestPeer(sess, "203.0.113.9:6881", target, liveConn{outbound: true, endpoint: "203.0.113.9:6881", holepunchID: 9})
	hc := &holepunchConn{key: "198.51.100.1:6881", ip: "198.51.100.1", client: initiator.client}
	sess.relayHolepunch(hc, netip.MustParseAddrPort("203.0.113.9:6881"))
	initiator.expectNone(t, "initiator")
	target.expectNone(t, "target")
}

func TestHolepunchLimiter(t *testing.T) {
	var l holepunchLimiter
	now := time.Now()
	for i := 0; i < maxHolepunchPerInterval; i++ {
		if !l.admit(now) {
			t.Fatalf("message %d refused within the budget", i)
		}
	}
	if l.admit(now.Add(pexInterval / 2)) {
		t.Fatal("message past the budget admitted")
	}
	if !l.admit(now.Add(pexInterval)) {
		t.Fatal("message in a new window refused")
	}
}

// TestHandleHolepunchMessageRateLimited checks a flood of rendezvous messages
// is cut off at maxHolepunchPerInterval answers.
func TestHandleHolepunchMessageRateLimited(t *testing.T) {
	sess := newHolepunchTestSession(t)
	initiator := newHolepunchPipePeer(t, [20]byte{1})
	initiator.msgs = make(chan sentHolepunch, 2*maxHolepunchPerInterval)
	addLiveTestPeer(sess, "198.51.100.1:6881", initiator, liveConn{outbound: true, endpoint: "198.51.100.1:6881", holepunchID: 7})
	hc := &holepunchConn{key: "198.51.100.1:6881", ip: "198.51.100.1", client: initiator.client, peerID: 7}
	payload, _ := peer.SerializeHolepunchMessage(&peer.HolepunchMessage{Type: peer.HolepunchRendezvous, Addr: netip.MustParseAddrPort("192.0.2.1:1")})
	for i := 0; i < maxHolepunchPerInterval+10; i++ {
		sess.handleHolepunchMessage(hc, payload)
	}
	for i := 0; i < maxHolepunchPerInterval; i++ {
		initiator.expect(t, "answer within the budget")
	}
	initiator.expectNone(t, "answer past the budget")
}

// TestPEXSnapshotListsInboundEndpointsWithFlags checks that inbound peers are
// advertised at their reachable endpoint with BEP 11/55 flags, and that the
// receiver is not told about itself.
func TestPEXSnapshotListsInboundEndpointsWithFlags(t *testing.T) {
	sess := &Session{Torrent: &torrent.Torrent{}, Peers: map[string]*PeerState{
		"192.0.2.1:6881": {IP: "192.0.2.1", Port: 6881, Active: true, Dialable: true},
	}}
	sess.liveConns = map[string]*liveConn{
		"192.0.2.1:6881":    {outbound: true, endpoint: "192.0.2.1:6881", holepunchID: 3},
		"203.0.113.9:40000": {utp: true, endpoint: "203.0.113.9:40000", holepunchID: 4},
		"198.51.100.7:5123": {endpoint: "198.51.100.7:6881"}, // inbound TCP with a listen port
		"198.51.100.8:5124": {},                              // inbound TCP without one
		"198.51.100.9:5125": {endpoint: "198.51.100.9:6881"}, // the receiver
	}
	peers, ok := sess.pexSnapshot("198.51.100.9:5125")
	if !ok {
		t.Fatal("snapshot refused")
	}
	want := map[string]byte{
		"192.0.2.1:6881":    peer.PEXFlagOutgoing | peer.PEXFlagHolepunch,
		"203.0.113.9:40000": peer.PEXFlagUTP | peer.PEXFlagHolepunch,
		"198.51.100.7:6881": 0,
	}
	if len(peers) != len(want) {
		t.Fatalf("snapshot = %v, want %v", peers, want)
	}
	for addr, flags := range want {
		p, ok := peers[addr]
		if !ok || p.Flags != flags {
			t.Fatalf("snapshot[%s] = %+v (present %v), want flags %#x", addr, p, ok, flags)
		}
	}
}

// TestHandlePEXMessageRecordsHolepunchRelays checks that a PEX sender becomes
// a relay for the holepunch-capable peers it lists, and only those.
func TestHandlePEXMessageRecordsHolepunchRelays(t *testing.T) {
	sess := &Session{Torrent: &torrent.Torrent{}, Peers: map[string]*PeerState{}}
	old := pexDiscoverPeer
	pexDiscoverPeer = func(s *Session, addr string) {
		s.mu.Lock()
		if s.Peers[addr] == nil {
			s.Peers[addr] = &PeerState{}
		}
		s.mu.Unlock()
	}
	t.Cleanup(func() { pexDiscoverPeer = old })

	msg := &peer.PEXMessage{Added: []peer.PEXPeer{
		{IP: net.ParseIP("203.0.113.9"), Port: 40000, Flags: peer.PEXFlagHolepunch | peer.PEXFlagUTP},
		{IP: net.ParseIP("203.0.113.10"), Port: 6881},
	}}
	sess.handlePEXMessage("192.0.2.1:6881", "192.0.2.1", msg)
	sess.handlePEXMessage("192.0.2.2:6881", "192.0.2.2", msg)
	sess.handlePEXMessage("192.0.2.1:6881", "192.0.2.1", msg) // no duplicate entry

	got := sess.Peers["203.0.113.9:40000"].holepunchRelays
	if got[0] != "192.0.2.2:6881" || got[1] != "192.0.2.1:6881" || got[2] != "" {
		t.Fatalf("relays = %q, want the two senders, most recent first", got)
	}
	if r := sess.Peers["203.0.113.10:6881"].holepunchRelays; r[0] != "" {
		t.Fatalf("peer without the holepunch flag got relays %q", r)
	}
}

// TestRequestHolepunchSendsRendezvous checks the initiator side: after a
// failed dial, a rendezvous for the peer goes to a connected relay that
// listed it, once per holepunchRetryInterval.
func TestRequestHolepunchSendsRendezvous(t *testing.T) {
	sess := newHolepunchTestSession(t)
	socket, err := utp.NewSocket(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = socket.Close() })
	sess.attachUTPSocket(socket)

	relay := newHolepunchPipePeer(t, [20]byte{1})
	silent := newHolepunchPipePeer(t, [20]byte{2})
	addLiveTestPeer(sess, "192.0.2.1:6881", relay, liveConn{outbound: true, endpoint: "192.0.2.1:6881", holepunchID: 11})
	addLiveTestPeer(sess, "192.0.2.2:6881", silent, liveConn{outbound: true, endpoint: "192.0.2.2:6881"})
	const target = "203.0.113.9:40000"
	sess.mu.Lock()
	sess.Peers[target] = &PeerState{IP: "203.0.113.9", Port: 40000, Dialable: true}
	// The most recent relay does not speak ut_holepunch, so the other is used.
	sess.noteHolepunchRelaysLocked("192.0.2.1:6881", []string{target})
	sess.noteHolepunchRelaysLocked("192.0.2.2:6881", []string{target})
	sess.mu.Unlock()

	sess.requestHolepunch(target)
	got := relay.expect(t, "relay")
	if got.extID != 11 || got.msg.Type != peer.HolepunchRendezvous || got.msg.Addr.String() != target {
		t.Fatalf("relay got %+v, want rendezvous for %s on ID 11", got, target)
	}
	silent.expectNone(t, "relay without ut_holepunch")

	sess.requestHolepunch(target)
	relay.expectNone(t, "second request within the retry interval")
}

// TestRequestHolepunchNeedsUTP checks no rendezvous is sent without a uTP
// socket: a holepunched connection can only be made over uTP.
func TestRequestHolepunchNeedsUTP(t *testing.T) {
	sess := newHolepunchTestSession(t)
	relay := newHolepunchPipePeer(t, [20]byte{1})
	addLiveTestPeer(sess, "192.0.2.1:6881", relay, liveConn{outbound: true, endpoint: "192.0.2.1:6881", holepunchID: 11})
	sess.mu.Lock()
	sess.Peers["203.0.113.9:40000"] = &PeerState{IP: "203.0.113.9", Port: 40000}
	sess.noteHolepunchRelaysLocked("192.0.2.1:6881", []string{"203.0.113.9:40000"})
	sess.mu.Unlock()
	sess.requestHolepunch("203.0.113.9:40000")
	relay.expectNone(t, "relay")
}

// TestAdmitPeerCrossedOpenSameAddress checks a simultaneous open whose two
// connections share the address key, as every uTP holepunch does: each end
// keeps the connection opened by the greater peer ID, whichever arrives
// second, and the host count stays at one connection.
func TestAdmitPeerCrossedOpenSameAddress(t *testing.T) {
	const key = "203.0.113.9:40000"
	hostKey, _ := peerHostKey("203.0.113.9")
	low, high := [20]byte{1}, [20]byte{9}
	for _, tc := range []struct {
		name        string
		ours        [20]byte
		firstOut    bool
		secondWins  bool
		description string
	}{
		{"ours greater, outbound first", high, true, false, "our outbound stays"},
		{"ours greater, inbound first", high, false, true, "our outbound replaces the inbound"},
		{"ours smaller, outbound first", low, true, true, "their outbound (our inbound) replaces ours"},
		{"ours smaller, inbound first", low, false, false, "our inbound stays"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := low
			if tc.ours == low {
				remote = high
			}
			sess := &Session{PeerID: tc.ours, activePeers: map[string]*peer.Client{}}
			first := newHolepunchPipePeer(t, remote)

			sess.mu.Lock()
			if reason := sess.admitPeerLocked(key, hostKey, false, remote, tc.firstOut); reason != "" {
				t.Fatalf("first connection refused: %s", reason)
			}
			sess.activePeers[key] = first.client
			reason := sess.admitPeerLocked(key, hostKey, false, remote, !tc.firstOut)
			hostCount := sess.admission.perHost[hostKey]
			sess.mu.Unlock()

			if tc.secondWins {
				if reason != "" {
					t.Fatalf("%s: second connection refused: %s", tc.description, reason)
				}
				// The first connection was closed.
				_ = first.remote.SetReadDeadline(time.Now().Add(time.Second))
				if _, err := first.remote.Read(make([]byte, 1)); !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
					t.Fatalf("%s: replaced connection still open (read: %v)", tc.description, err)
				}
				if owner := sess.admission.peerIDs[remote]; owner.outbound == tc.firstOut {
					t.Fatalf("peer ID still owned by the replaced connection")
				}
			} else if reason == "" {
				t.Fatalf("%s: second connection admitted", tc.description)
			}
			if hostCount != 1 {
				t.Fatalf("host count = %d, want 1", hostCount)
			}
		})
	}
}

// TestAdmitPeerSameAddressDifferentPeerRefused checks the exception is narrow:
// another peer ID, or the same direction, at an active key is still refused.
func TestAdmitPeerSameAddressDifferentPeerRefused(t *testing.T) {
	const key = "203.0.113.9:40000"
	hostKey, _ := peerHostKey("203.0.113.9")
	sess := &Session{PeerID: [20]byte{1}, activePeers: map[string]*peer.Client{}}
	first := newHolepunchPipePeer(t, [20]byte{9})
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if reason := sess.admitPeerLocked(key, hostKey, false, [20]byte{9}, true); reason != "" {
		t.Fatalf("first connection refused: %s", reason)
	}
	sess.activePeers[key] = first.client
	if reason := sess.admitPeerLocked(key, hostKey, false, [20]byte{8}, false); reason != "duplicate_address" {
		t.Fatalf("different peer ID at an active key: %q, want duplicate_address", reason)
	}
	if reason := sess.admitPeerLocked(key, hostKey, false, [20]byte{9}, true); reason != "duplicate_address" {
		t.Fatalf("same direction at an active key: %q, want duplicate_address", reason)
	}
	if reason := sess.admitPeerLocked(key, hostKey, false, [20]byte{}, false); reason != "duplicate_address" {
		t.Fatalf("zero peer ID at an active key: %q, want duplicate_address", reason)
	}
}

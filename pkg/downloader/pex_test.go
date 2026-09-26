package downloader

import (
	"crypto/sha1"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func TestExtensionHandshakeMapAdvertisesPEXUnlessPrivate(t *testing.T) {
	sess := &Session{Torrent: &torrent.Torrent{}}
	sess.mu.Lock()
	extensions := sess.extensionHandshakeMapLocked()
	sess.mu.Unlock()

	if extensions[peer.ExtNameMetadata] != peer.LocalMetadataExtID {
		t.Fatalf("ut_metadata = %d, want %d", extensions[peer.ExtNameMetadata], peer.LocalMetadataExtID)
	}
	if extensions[peer.ExtNamePEX] != peer.LocalPEXExtID {
		t.Fatalf("ut_pex = %d, want %d", extensions[peer.ExtNamePEX], peer.LocalPEXExtID)
	}

	sess.Torrent.Private = true
	sess.mu.Lock()
	extensions = sess.extensionHandshakeMapLocked()
	sess.mu.Unlock()
	if _, ok := extensions[peer.ExtNamePEX]; ok {
		t.Fatal("private torrent advertised ut_pex")
	}
	// BEP 27: once metadata is known, a private torrent's info dict is not offered.
	if _, ok := extensions[peer.ExtNameMetadata]; ok {
		t.Fatal("private torrent with known metadata advertised ut_metadata")
	}

	// A magnet still fetching needs ut_metadata whatever its (unknown) flag says.
	sess.metadataMode = true
	sess.mu.Lock()
	extensions = sess.extensionHandshakeMapLocked()
	sess.mu.Unlock()
	if extensions[peer.ExtNameMetadata] != peer.LocalMetadataExtID {
		t.Fatal("metadata-mode session did not advertise ut_metadata")
	}
}

func TestBuildPEXDeltaAddsAndDrops(t *testing.T) {
	sess := &Session{
		Torrent: &torrent.Torrent{},
		Peers: map[string]*PeerState{
			net.JoinHostPort("127.0.0.1", "1001"): {
				IP:       "127.0.0.1",
				Port:     1001,
				Active:   true,
				Dialable: true,
			},
			net.JoinHostPort("127.0.0.1", "1002"): {
				IP:       "127.0.0.1",
				Port:     1002,
				Active:   true,
				Dialable: true,
			},
			net.JoinHostPort("127.0.0.1", "1004"): {
				IP:       "127.0.0.1",
				Port:     1004,
				Active:   true,
				Dialable: false,
			},
		},
	}
	advertised := map[string]struct{}{
		net.JoinHostPort("127.0.0.1", "1001"): {},
		net.JoinHostPort("127.0.0.1", "1003"): {},
	}

	msg, next, ok := sess.buildPEXDelta("", advertised)
	if !ok {
		t.Fatal("expected PEX delta")
	}
	if len(msg.Added) != 1 || msg.Added[0].Port != 1002 {
		t.Fatalf("added = %+v, want only port 1002", msg.Added)
	}
	if len(msg.Dropped) != 1 || msg.Dropped[0].Port != 1003 {
		t.Fatalf("dropped = %+v, want only port 1003", msg.Dropped)
	}
	if _, ok := next[net.JoinHostPort("127.0.0.1", "1001")]; !ok {
		t.Fatal("next advertised set lost unchanged peer")
	}
	if _, ok := next[net.JoinHostPort("127.0.0.1", "1002")]; !ok {
		t.Fatal("next advertised set did not include added peer")
	}
	if _, ok := next[net.JoinHostPort("127.0.0.1", "1003")]; ok {
		t.Fatal("next advertised set retained dropped peer")
	}
}

func TestBuildPEXDeltaHonorsPrivateTorrent(t *testing.T) {
	sess := &Session{
		Torrent: &torrent.Torrent{Private: true},
		Peers: map[string]*PeerState{
			net.JoinHostPort("127.0.0.1", "1001"): {
				IP:       "127.0.0.1",
				Port:     1001,
				Active:   true,
				Dialable: true,
			},
		},
	}
	advertised := map[string]struct{}{net.JoinHostPort("127.0.0.1", "1001"): {}}
	if msg, next, ok := sess.buildPEXDelta("", advertised); ok || msg != nil || len(next) != 1 {
		t.Fatal("private torrent produced a PEX delta")
	}
}

func TestPEXDiscoversAndConnectsThirdPeer(t *testing.T) {
	tor := &torrent.Torrent{
		Name:        "pex.txt",
		InfoHash:    sha1.Sum([]byte("pex-integration")),
		PieceLength: 32,
		PieceHashes: [][20]byte{sha1.Sum([]byte("01234567890123456789012345678901"))},
		Files:       []torrent.File{{Length: 32, Path: []string{"pex.txt"}}},
	}
	tempDir := t.TempDir()
	st, err := storage.NewStorage(tempDir, []storage.FileInfo{{Path: filepath.Join(tor.Files[0].Path...), Length: 32}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{1}, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	sess.Start()
	defer sess.Close()

	thirdConnected := make(chan struct{}, 1)
	thirdListener, thirdPort := startPEXHandshakePeer(t, thirdConnected, false, nil)
	defer thirdListener.Close()
	thirdAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(thirdPort))

	pexListener, pexPort := startPEXHandshakePeer(t, nil, true, func(conn net.Conn) {
		hsPayload, err := peer.SerializeExtensionHandshakeWithExtensions(map[string]int{
			peer.ExtNameMetadata: peer.LocalMetadataExtID,
			peer.ExtNamePEX:      7,
		}, 0)
		if err != nil {
			t.Errorf("failed to serialize extension handshake: %v", err)
			return
		}
		writeExtendedTestMessage(t, conn, peer.ExtHandshake, hsPayload)

		pexPayload, err := peer.SerializePEXMessage(&peer.PEXMessage{
			Added: []peer.PEXPeer{{IP: net.ParseIP("127.0.0.1"), Port: uint16(thirdPort)}},
		})
		if err != nil {
			t.Errorf("failed to serialize PEX: %v", err)
			return
		}
		writeExtendedTestMessage(t, conn, peer.LocalPEXExtID, pexPayload)
	})
	defer pexListener.Close()

	sess.AddPeerFromDiscovery(net.JoinHostPort("127.0.0.1", strconv.Itoa(pexPort)))

	select {
	case <-thirdConnected:
	case <-time.After(3 * time.Second):
		t.Fatalf("third peer %s was not dialed after PEX", thirdAddr)
	}

	sess.mu.RLock()
	ps, ok := sess.Peers[thirdAddr]
	dialable := ok && ps.Dialable
	sess.mu.RUnlock()
	if !ok {
		t.Fatalf("third peer %s was not added to known peers", thirdAddr)
	}
	if !dialable {
		t.Fatalf("third peer %s was not marked dialable", thirdAddr)
	}
}

func startPEXHandshakePeer(t *testing.T, connected chan<- struct{}, supportsExtensions bool, afterHandshake func(net.Conn)) (net.Listener, int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to parse listener addr: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("failed to parse listener port: %v", err)
	}

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

		hs, err := peer.ParseHandshake(conn)
		if err != nil {
			return
		}
		resp := &peer.Handshake{
			Pstr:     "BitTorrent protocol",
			InfoHash: hs.InfoHash,
			PeerID:   [20]byte{9},
		}
		if supportsExtensions {
			resp.Reserved[5] = 0x10
		}
		if _, err := conn.Write(resp.Serialize()); err != nil {
			return
		}
		if connected != nil {
			select {
			case connected <- struct{}{}:
			default:
			}
		}
		if afterHandshake != nil {
			afterHandshake(conn)
		}
		<-time.After(250 * time.Millisecond)
	}()

	return ln, port
}

func writeExtendedTestMessage(t *testing.T, conn net.Conn, extID byte, payload []byte) {
	t.Helper()
	msgPayload := make([]byte, 1+len(payload))
	msgPayload[0] = extID
	copy(msgPayload[1:], payload)
	if _, err := conn.Write((&peer.Message{ID: peer.MsgExtended, Payload: msgPayload}).Serialize()); err != nil {
		t.Errorf("failed to write extended message: %v", err)
	}
}

// refusingLoopbackPort returns a loopback port nothing listens on, so a dial to
// it is refused at once without leaving the machine.
func refusingLoopbackPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func sessionKnowsPeer(sess *Session, addr string) bool {
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	_, ok := sess.Peers[addr]
	return ok
}

// TestPEXIgnoresTooFrequentMessagesAndDropsFlooder covers a peer streaming
// ut_pex messages back to back, each naming endpoints for us to dial: only the
// first is used, early ones are ignored, and a peer that keeps sending them is
// dropped.
func TestPEXIgnoresTooFrequentMessagesAndDropsFlooder(t *testing.T) {
	sess := newWireTestSession(t, 4, BlockSize)
	sess.mu.Lock()
	sess.started = true
	sess.mu.Unlock()
	w := startWirePeer(t, sess, 7500, fastReserved())
	port := refusingLoopbackPort(t)

	sendPEX := func(ip string) string {
		payload, err := peer.SerializePEXMessage(&peer.PEXMessage{
			Added: []peer.PEXPeer{{IP: net.ParseIP(ip), Port: uint16(port)}},
		})
		if err != nil {
			t.Fatalf("serialize PEX: %v", err)
		}
		w.sendExtended(peer.LocalPEXExtID, payload)
		return net.JoinHostPort(ip, strconv.Itoa(port))
	}

	first := sendPEX("127.0.1.1")
	w.barrier()
	if !sessionKnowsPeer(sess, first) {
		t.Fatal("the first ut_pex message was not used")
	}

	second := sendPEX("127.0.1.2")
	w.barrier()
	if sessionKnowsPeer(sess, second) {
		t.Fatal("a ut_pex message sent right after the previous one was acted on")
	}

	// The message after maxPEXPerInterval within one interval drops the peer.
	for i := 1; i < maxPEXPerInterval; i++ {
		sendPEX(fmt.Sprintf("127.0.1.%d", 10+i))
	}
	w.waitClosed(5 * time.Second)
}

// TestPEXRateLimiterKeepsHonestSenders covers the ut_pex rate rule over a long
// connection: honest senders are never dropped, however long they stay (nor when
// our loop reads a few of their messages back to back after a stall), while a
// sender over maxPEXPerInterval messages per interval is.
func TestPEXRateLimiterKeepsHonestSenders(t *testing.T) {
	start := time.Unix(1_000_000, 0)
	run := func(gaps ...time.Duration) (used int, dropped bool) {
		var l pexRateLimiter
		now := start
		for _, gap := range gaps {
			now = now.Add(gap)
			use, flood := l.admit(now)
			if flood {
				return used, true
			}
			if use {
				used++
			}
		}
		return used, false
	}
	every := func(gap time.Duration, n int) []time.Duration {
		gaps := make([]time.Duration, n)
		for i := range gaps {
			gaps[i] = gap
		}
		return gaps
	}

	for _, gap := range []time.Duration{pexInterval, pexInterval / 2, pexInterval / 3} {
		used, dropped := run(every(gap, 200)...)
		if dropped {
			t.Errorf("a peer sending ut_pex every %v was dropped", gap)
		}
		if gap >= pexInterval/2 && used != 200 {
			t.Errorf("a peer sending ut_pex every %v had %d of 200 messages used", gap, used)
		}
		if gap < pexInterval/2 && used > 100 {
			t.Errorf("a peer sending ut_pex every %v had %d of 200 messages used", gap, used)
		}
	}

	// Three messages read back to back after a stall, now and again.
	var stalls []time.Duration
	for i := 0; i < 20; i++ {
		stalls = append(stalls, 3*pexInterval, 0, 0)
	}
	if _, dropped := run(stalls...); dropped {
		t.Error("a peer whose messages were read back to back after stalls was dropped")
	}

	if _, dropped := run(0, time.Second, time.Second, time.Second); !dropped {
		t.Error("a peer sending four ut_pex messages within one interval was kept")
	}
	// A flooder is dropped however it spreads its messages inside the window.
	if _, dropped := run(every(pexInterval/(maxPEXPerInterval+1), 2*maxPEXPerInterval)...); !dropped {
		t.Error("a peer sending ut_pex faster than the rate was kept")
	}
}

// TestPEXActsOnAtMostFiftyAddedPeers covers a ut_pex message listing far more
// peers than BEP 11 allows: only pexIngestLimit of them are taken.
func TestPEXActsOnAtMostFiftyAddedPeers(t *testing.T) {
	sess := newWireTestSession(t, 1, BlockSize)
	sess.mu.Lock()
	sess.started = true
	sess.mu.Unlock()
	port := refusingLoopbackPort(t)

	msg := &peer.PEXMessage{}
	for i := 0; i < 3*pexIngestLimit; i++ {
		msg.Added = append(msg.Added, peer.PEXPeer{IP: net.IPv4(127, 0, 2, byte(i+1)), Port: uint16(port)})
	}
	sess.handlePEXMessage("127.0.0.1:7601", "127.0.0.1", msg)

	sess.mu.RLock()
	known := len(sess.Peers)
	sess.mu.RUnlock()
	if known != pexIngestLimit {
		t.Fatalf("known peers after one ut_pex message = %d, want %d", known, pexIngestLimit)
	}
}

// TestPEXRejectsAddressesMoreLocalThanSender covers a remote peer using PEX to
// aim our dials at loopback, LAN, link-local (cloud metadata) or non-unicast
// addresses.
func TestPEXRejectsAddressesMoreLocalThanSender(t *testing.T) {
	sess := newWireTestSession(t, 1, BlockSize)
	sess.mu.Lock()
	sess.started = true
	sess.mu.Unlock()

	local := &peer.PEXMessage{Added: []peer.PEXPeer{
		{IP: net.ParseIP("127.0.0.1"), Port: 22},
		{IP: net.ParseIP("::ffff:127.0.0.1"), Port: 22},
		{IP: net.ParseIP("::1"), Port: 22},
		{IP: net.ParseIP("192.168.1.1"), Port: 80},
		{IP: net.ParseIP("10.0.0.1"), Port: 445},
		{IP: net.ParseIP("169.254.169.254"), Port: 80},
		{IP: net.ParseIP("fe80::1"), Port: 80},
	}}
	sess.handlePEXMessage("203.0.113.9:6881", "203.0.113.9", local)

	// Nobody may hand out addresses that are never a unicast peer.
	invalid := &peer.PEXMessage{Added: []peer.PEXPeer{
		{IP: net.ParseIP("224.0.0.1"), Port: 1900},
		{IP: net.ParseIP("255.255.255.255"), Port: 9},
		{IP: net.ParseIP("0.1.2.3"), Port: 9},
		{IP: net.ParseIP("240.0.0.1"), Port: 9},
		{IP: net.ParseIP("ff02::1"), Port: 9},
	}}
	sess.handlePEXMessage("127.0.0.1:6881", "127.0.0.1", invalid)

	sess.mu.RLock()
	defer sess.mu.RUnlock()
	for addr := range sess.Peers {
		t.Errorf("PEX endpoint %s was accepted", addr)
	}
}

// TestBuildPEXDeltaSilentWhileFetchingMetadata covers a magnet whose private
// flag is still unknown: it takes PEX in but tells no one about its peers until
// metadata shows the torrent is public.
func TestBuildPEXDeltaSilentWhileFetchingMetadata(t *testing.T) {
	sess := &Session{
		Torrent:      &torrent.Torrent{},
		metadataMode: true,
		Peers: map[string]*PeerState{
			net.JoinHostPort("127.0.0.1", "1001"): {IP: "127.0.0.1", Port: 1001, Active: true, Dialable: true},
		},
	}
	sess.mu.Lock()
	extensions := sess.extensionHandshakeMapLocked()
	sess.mu.Unlock()
	if extensions[peer.ExtNamePEX] != peer.LocalPEXExtID {
		t.Fatal("metadata-mode session stopped taking PEX in")
	}
	if msg, next, ok := sess.buildPEXDelta("", map[string]struct{}{}); ok || msg != nil || len(next) != 0 {
		t.Fatalf("metadata-mode session advertised peers: %+v", msg)
	}

	sess.mu.Lock()
	sess.metadataMode = false
	sess.mu.Unlock()
	if msg, _, ok := sess.buildPEXDelta("", map[string]struct{}{}); !ok || len(msg.Added) != 1 {
		t.Fatalf("public torrent with metadata did not advertise its peer: %+v", msg)
	}
}

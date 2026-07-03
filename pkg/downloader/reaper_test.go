package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
	"sainttorrent/pkg/tracker"
)

// startStalledMockPeer accepts one connection, completes the handshake, advertises
// all pieces, unchokes on Interested, and then deliberately delivers NO piece data
// while keeping the socket warm with frequent keep-alives. It is the canonical
// "dead weight" peer: it holds its connection slot and resets the read deadline
// forever without ever giving us a block. The single read goroutine sends a
// keep-alive whenever the read deadline elapses, so writes never race.
func startStalledMockPeer(t *testing.T, ln net.Listener, fullBitfield []byte) {
	t.Helper()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		hs, err := peer.ParseHandshake(conn)
		if err != nil {
			return
		}
		var pid [20]byte
		copy(pid[:], "-MOCKstalledpeer0000")
		if _, err := conn.Write((&peer.Handshake{Pstr: "BitTorrent protocol", InfoHash: hs.InfoHash, PeerID: pid}).Serialize()); err != nil {
			return
		}
		if _, err := conn.Write((&peer.Message{ID: peer.MsgBitfield, Payload: fullBitfield}).Serialize()); err != nil {
			return
		}

		for {
			// A short read deadline doubles as the keep-alive cadence: on timeout we
			// emit a keep-alive (4 zero bytes) so the peer looks alive but transfers
			// nothing. A non-timeout error means the session reaped us — exit.
			_ = conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
			msg, err := peer.ParseMessage(conn)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					if _, werr := conn.Write([]byte{0, 0, 0, 0}); werr != nil {
						return
					}
					continue
				}
				return
			}
			if msg == nil {
				continue
			}
			if msg.ID == peer.MsgInterested {
				// Unchoke so the session sends real block requests — which we then
				// ignore. (Requests are silently dropped: that is the stall.)
				if _, werr := conn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize()); werr != nil {
					return
				}
			}
		}
	}()
}

// newStallTestTorrent builds a small single-file torrent (numPieces single-block
// pieces) plus on-disk storage, and returns a session, a full bitfield covering
// every piece, and the source bytes (so a mock peer can serve verifiable blocks).
func newStallTestTorrent(t *testing.T, numPieces int) (*Session, []byte, []byte) {
	t.Helper()
	const pieceLen = BlockSize
	total := pieceLen * numPieces
	data := make([]byte, total)
	for i := range data {
		data[i] = byte(i*5 + 1)
	}
	hashes := make([][20]byte, numPieces)
	for p := 0; p < numPieces; p++ {
		hashes[p] = sha1.Sum(data[p*pieceLen : (p+1)*pieceLen])
	}
	tor := &torrent.Torrent{
		Name:        "stall.bin",
		PieceLength: int64(pieceLen),
		PieceHashes: hashes,
		Files:       []torrent.File{{Length: int64(total), Path: []string{"stall.bin"}}},
	}
	dir := t.TempDir()
	st, err := storage.NewStorage(dir, []storage.FileInfo{{Path: "stall.bin", Length: int64(total)}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	var pid [20]byte
	copy(pid[:], "-ST0001-stalltest123")
	sess, err := NewSession(tor, st, pid, 0, dir)
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	bf := make([]byte, (numPieces+7)/8)
	for i := 0; i < numPieces; i++ {
		bf[i/8] |= 1 << (7 - (i % 8))
	}
	return sess, bf, data
}

// waitForActivePeers polls until the session reports exactly want active peers, or
// fails after the timeout.
func waitForActivePeers(t *testing.T, sess *Session, want int, timeout time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(sess.GetActivePeers()) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: wanted %d active peer(s), still have %d after %v", what, want, len(sess.GetActivePeers()), timeout)
}

// TestStalledPeerIsReaped proves the stall reaper: a peer that holds its connection
// open with keep-alives but never delivers a block is dropped once peerStallTimeout
// elapses, freeing its slot. Before the fix this connection lived forever (the
// keep-alives reset the read deadline and the only request-timeout path was skipped),
// so the slot leaked and — under the shared, manager-wide cap — eventually stalled
// every torrent at once.
func TestStalledPeerIsReaped(t *testing.T) {
	defer swapDuration(&peerStallTimeout, 1*time.Second)()
	// Keep the block-request-timeout path far away so this test isolates the reaper.
	defer swapDuration(&blockRequestTimeout, 1*time.Hour)()

	sess, bf, _ := newStallTestTorrent(t, 4)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	startStalledMockPeer(t, ln, bf)

	sess.Start()
	defer sess.Close()
	sess.WaitVerified() // settle background resume verification (the reaper is exempt while it runs)

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	sess.AddPeerFromDiscovery("127.0.0.1:" + portStr)

	// It must connect first...
	waitForActivePeers(t, sess, 1, 2*time.Second, "peer never connected")
	// ...then be reaped within roughly peerStallTimeout (+ keep-alive cadence + margin).
	waitForActivePeers(t, sess, 0, 3*time.Second, "stalled peer was not reaped")
}

func TestStalledPeerReapIsLogged(t *testing.T) {
	defer swapDuration(&peerStallTimeout, 1*time.Second)()
	defer swapDuration(&blockRequestTimeout, 1*time.Hour)()

	logPath := filepath.Join(t.TempDir(), "debug.log")
	if err := logging.Configure(logging.Config{Path: logPath, Level: logging.LevelDebug}); err != nil {
		t.Fatalf("configure logging: %v", err)
	}
	t.Cleanup(func() { _ = logging.Close() })

	sess, bf, _ := newStallTestTorrent(t, 4)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	startStalledMockPeer(t, ln, bf)

	sess.Start()
	defer sess.Close()
	sess.WaitVerified()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	sess.AddPeerFromDiscovery("127.0.0.1:" + portStr)

	waitForActivePeers(t, sess, 1, 2*time.Second, "peer never connected")
	waitForActivePeers(t, sess, 0, 3*time.Second, "stalled peer was not reaped")

	if err := logging.Close(); err != nil {
		t.Fatalf("close logging: %v", err)
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, `"event":"peer_reaped"`) {
		t.Fatalf("expected peer_reaped log, got:\n%s", text)
	}
}

// TestKeepAliveTriggersRequestTimeout proves fix #2: a peer that unchokes us, takes
// our block requests, then goes silent except for keep-alives still has its
// outstanding requests time out and is dropped. The timeout sweep now runs on the
// keep-alive path (and is no longer skipped when choked), so the peer can't keep its
// slot alive indefinitely by dribbling keep-alives. The reaper is disabled here so
// the disconnect can only come from the request-timeout path.
func TestKeepAliveTriggersRequestTimeout(t *testing.T) {
	defer swapDuration(&peerStallTimeout, 1*time.Hour)() // disable the reaper
	defer swapDuration(&blockRequestTimeout, 200*time.Millisecond)()

	sess, bf, _ := newStallTestTorrent(t, 4)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	startStalledMockPeer(t, ln, bf)

	sess.Start()
	defer sess.Close()
	sess.WaitVerified() // settle background resume verification (the reaper is exempt while it runs)

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	sess.AddPeerFromDiscovery("127.0.0.1:" + portStr)

	waitForActivePeers(t, sess, 1, 2*time.Second, "peer never connected")
	// (maxBlockRequestRetries+1) timeouts at 200ms each, paced by the keep-alive
	// cadence, drops the peer well within this window.
	waitForActivePeers(t, sess, 0, 3*time.Second, "silent-but-keepalive peer was not timed out")
}

// swapDuration sets *p to v and returns a restore func, so a test can shorten a
// package-level timing knob and put it back on cleanup.
func swapDuration(p *time.Duration, v time.Duration) func() {
	old := *p
	*p = v
	return func() { *p = old }
}

// TestMaintainPeerConnectionsSelectsEligible checks the dial-maintenance gating:
// it (re)dials a known, disconnected peer whose backoff has elapsed AND a peer with
// a zero LastAttempt (e.g. one Resume cleared), while leaving alone peers that are
// active, recently attempted, or inbound (keyed by an undialable ephemeral port).
// This refills a freed slot between announces without stranding resumed peers or
// wasting dials on inbound source ports.
func TestMaintainPeerConnectionsSelectsEligible(t *testing.T) {
	sess, _, _ := newStallTestTorrent(t, 4) // fresh torrent: not complete, not metadata mode
	defer func() {
		sess.cancel()
		sess.wg.Wait()
	}()

	// A guaranteed-closed loopback port so the dials we expect refuse instantly
	// instead of hanging on the dial timeout.
	tmpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, portStr, _ := net.SplitHostPort(tmpLn.Addr().String())
	tmpLn.Close()
	port := parsePort(portStr)

	now := time.Now()
	oldAttempt := now.Add(-2 * peerRedialBackoff)    // eligible: backoff elapsed
	recentAttempt := now.Add(-peerRedialBackoff / 2) // not eligible: within backoff

	sess.mu.Lock()
	sess.started = true
	sess.Peers = map[string]*PeerState{
		"eligible": {IP: "127.0.0.1", Port: port, Active: false, LastAttempt: oldAttempt, Dialable: true},
		"zero":     {IP: "127.0.0.1", Port: port, Active: false, LastAttempt: time.Time{}, Dialable: true},
		"recent":   {IP: "127.0.0.1", Port: port, Active: false, LastAttempt: recentAttempt, Dialable: true},
		"active":   {IP: "127.0.0.1", Port: port, Active: true, LastAttempt: oldAttempt, Dialable: true},
		"dialing":  {IP: "127.0.0.1", Port: port, Active: false, LastAttempt: oldAttempt, Dialable: true, Dialing: true},
		"inbound":  {IP: "127.0.0.1", Port: port, Active: false, LastAttempt: oldAttempt, Dialable: false},
	}
	sess.mu.Unlock()

	sess.maintainPeerConnections()

	sess.mu.RLock()
	defer sess.mu.RUnlock()
	if sess.Peers["eligible"].LastAttempt.Equal(oldAttempt) {
		t.Error("eligible peer was not dialed: LastAttempt unchanged")
	}
	if sess.Peers["zero"].LastAttempt.IsZero() {
		t.Error("zero-LastAttempt peer should be dialed now (e.g. after Resume), not stranded")
	}
	if !sess.Peers["recent"].LastAttempt.Equal(recentAttempt) {
		t.Error("recently-attempted peer should not have been redialed (within backoff)")
	}
	if !sess.Peers["active"].LastAttempt.Equal(oldAttempt) {
		t.Error("active peer should not have been redialed")
	}
	if !sess.Peers["dialing"].LastAttempt.Equal(oldAttempt) {
		t.Error("peer with a dial already in flight should not have been redialed")
	}
	if !sess.Peers["inbound"].LastAttempt.Equal(oldAttempt) {
		t.Error("inbound peer must not be dialed: its key is an ephemeral source port")
	}
}

func TestConnectToPeerSlotRaceDoesNotBurnBackoff(t *testing.T) {
	sess, _, _ := newStallTestTorrent(t, 1)
	defer sess.Close()
	sess.globalOutboundSlots = make(chan struct{}, 1)
	sess.globalOutboundSlots <- struct{}{} // another session wins the shared slot

	addr := "127.0.0.1:9"
	sess.mu.Lock()
	sess.Peers[addr] = &PeerState{
		IP:          "127.0.0.1",
		Port:        9,
		LastAttempt: time.Now(),
		Dialable:    true,
		Dialing:     true,
	}
	sess.mu.Unlock()

	sess.connectToPeer(trackerPeer("127.0.0.1", 9))

	sess.mu.RLock()
	ps := sess.Peers[addr]
	sess.mu.RUnlock()
	if ps.Dialing {
		t.Fatal("slot-rejected attempt remained marked as dialing")
	}
	if !ps.LastAttempt.IsZero() {
		t.Fatalf("slot-rejected attempt burned redial backoff: %v", ps.LastAttempt)
	}
}

func TestFailedDialAcrossResumeStaysImmediatelyRetryable(t *testing.T) {
	sess, _, _ := newStallTestTorrent(t, 1)
	defer sess.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port := parsePort(portStr)
	addr := net.JoinHostPort(host, portStr)

	sess.mu.Lock()
	sess.Peers[addr] = &PeerState{
		IP:          host,
		Port:        port,
		LastAttempt: time.Now(),
		Dialable:    true,
		Dialing:     true,
	}
	sess.mu.Unlock()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	done := make(chan struct{})
	go func() {
		sess.connectToPeer(trackerPeer(host, port))
		close(done)
	}()

	var conn net.Conn
	select {
	case conn = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("dial did not reach the mock listener")
	}

	// The connection is still handshaking and therefore not Active. A quick
	// pause/resume clears inactive peers for immediate retry; the eventual handshake
	// failure must not overwrite that with a fresh backoff.
	sess.Pause()
	sess.Resume()
	_ = conn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("failed handshaking dial did not exit")
	}

	sess.mu.RLock()
	ps := sess.Peers[addr]
	sess.mu.RUnlock()
	if ps.Dialing {
		t.Fatal("failed dial remained marked as dialing")
	}
	if !ps.LastAttempt.IsZero() {
		t.Fatalf("failed dial spanning resume burned redial backoff: %v", ps.LastAttempt)
	}
}

// TestMaintainPeerConnectionsSkipsWhenNotDownloading checks the early-return guards:
// no dialing happens when the session is paused or already complete (seeding is
// served by inbound connections and the announce flow, not the download maintenance
// loop).
func TestMaintainPeerConnectionsSkipsWhenNotDownloading(t *testing.T) {
	check := func(t *testing.T, mutate func(s *Session)) {
		t.Helper()
		sess, _, _ := newStallTestTorrent(t, 4)
		defer func() {
			sess.cancel()
			sess.wg.Wait()
		}()
		old := time.Now().Add(-2 * peerRedialBackoff)
		sess.mu.Lock()
		sess.started = true
		sess.Peers = map[string]*PeerState{
			"p": {IP: "127.0.0.1", Port: 9, Active: false, LastAttempt: old},
		}
		mutate(sess)
		sess.mu.Unlock()

		sess.maintainPeerConnections()

		sess.mu.RLock()
		defer sess.mu.RUnlock()
		if !sess.Peers["p"].LastAttempt.Equal(old) {
			t.Error("peer was dialed despite the maintenance loop being gated off")
		}
	}

	t.Run("paused", func(t *testing.T) {
		check(t, func(s *Session) { s.paused = true })
	})
	t.Run("complete", func(t *testing.T) {
		check(t, func(s *Session) {
			for i := range s.PieceStates {
				s.PieceStates[i] = PieceCompleted
			}
			s.recomputeStatsLocked()
		})
	})
}

func parsePort(portStr string) uint16 {
	var p int
	for _, c := range portStr {
		p = p*10 + int(c-'0')
	}
	return uint16(p)
}

// startServingMockPeer accepts one connection and serves the torrent honestly:
// handshake, full bitfield, unchoke on Interested, and the requested block on every
// Request. It is the "good peer" counterpart to startStalledMockPeer.
func startServingMockPeer(t *testing.T, ln net.Listener, data []byte, pieceLen int) {
	t.Helper()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		hs, err := peer.ParseHandshake(conn)
		if err != nil {
			return
		}
		var pid [20]byte
		copy(pid[:], "-MOCKserving00000000")
		if _, err := conn.Write((&peer.Handshake{Pstr: "BitTorrent protocol", InfoHash: hs.InfoHash, PeerID: pid}).Serialize()); err != nil {
			return
		}
		numPieces := (len(data) + pieceLen - 1) / pieceLen
		bf := make([]byte, (numPieces+7)/8)
		for i := 0; i < numPieces; i++ {
			bf[i/8] |= 1 << (7 - (i % 8))
		}
		if _, err := conn.Write((&peer.Message{ID: peer.MsgBitfield, Payload: bf}).Serialize()); err != nil {
			return
		}
		for {
			msg, err := peer.ParseMessage(conn)
			if err != nil {
				return
			}
			if msg == nil {
				continue
			}
			switch msg.ID {
			case peer.MsgInterested:
				if _, err := conn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize()); err != nil {
					return
				}
			case peer.MsgRequest:
				if len(msg.Payload) != 12 {
					continue
				}
				index := binary.BigEndian.Uint32(msg.Payload[0:4])
				begin := binary.BigEndian.Uint32(msg.Payload[4:8])
				length := binary.BigEndian.Uint32(msg.Payload[8:12])
				start := int(index)*pieceLen + int(begin)
				if start < 0 || start+int(length) > len(data) {
					continue
				}
				payload := make([]byte, 8+length)
				binary.BigEndian.PutUint32(payload[0:4], index)
				binary.BigEndian.PutUint32(payload[4:8], begin)
				copy(payload[8:], data[start:start+int(length)])
				if _, err := conn.Write((&peer.Message{ID: peer.MsgPiece, Payload: payload}).Serialize()); err != nil {
					return
				}
			}
		}
	}()
}

// TestLowRateLimitDoesNotWedgeDownload exercises finding #1 at the session level (the
// isolated limiter test couldn't). With a download limit set BELOW the per-pump
// window's worth of bandwidth, the old code synchronously blocked the peer reader in
// pump while it filled all 256 request slots, so no blocks were drained, lastProgressAt
// never advanced, and the new reaper disconnected the peer. The fix reserves bandwidth
// non-blockingly (never starving the reader) and exempts rate-limited downloads from
// the reaper, so the transfer simply proceeds at the capped rate and completes.
func TestLowRateLimitDoesNotWedgeDownload(t *testing.T) {
	// A short reaper makes this a regression guard: under the old blocking fill the
	// peer would be reaped within a second; the fix must let it run to completion.
	defer swapDuration(&peerStallTimeout, 1*time.Second)()

	const numPieces = 2 // 2 single-block pieces = 32 KB
	sess, _, data := newStallTestTorrent(t, numPieces)
	// The bucket starts with one 16 KB block. The second block then needs ~2 seconds
	// at 8 KB/s, deliberately longer than the one-second stall timeout.
	sess.SetDownloadLimit(8 * 1024)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	startServingMockPeer(t, ln, data, BlockSize)

	sess.Start()
	defer sess.Close()
	sess.WaitVerified() // settle background resume verification (the reaper is exempt while it runs)

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	sess.AddPeerFromDiscovery("127.0.0.1:" + portStr)

	// The transfer should complete after the intentional ~2-second limiter wait,
	// rather than treating the newly issued second request as already stalled.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if sess.PercentComplete() == 100.0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("rate-limited download did not complete: %.1f%% done (wedged?)", sess.PercentComplete())
}

func TestRateLimitedStalledPeerIsStillReaped(t *testing.T) {
	defer swapDuration(&peerStallTimeout, 500*time.Millisecond)()
	defer swapDuration(&blockRequestTimeout, 1*time.Hour)()

	sess, bf, _ := newStallTestTorrent(t, 4)
	sess.SetDownloadLimit(64 * 1024)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	startStalledMockPeer(t, ln, bf)

	sess.Start()
	defer sess.Close()
	sess.WaitVerified()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	sess.AddPeerFromDiscovery("127.0.0.1:" + portStr)

	waitForActivePeers(t, sess, 1, 2*time.Second, "rate-limited peer never connected")
	waitForActivePeers(t, sess, 0, 3*time.Second, "rate-limited stalled peer was not reaped")
}

// dialStalledPeer connects TO the session (an inbound connection from the session's
// point of view), then stalls like startStalledMockPeer: bitfield, unchoke, but no
// piece data, only keep-alives.
func dialStalledPeer(t *testing.T, addr string, infoHash [20]byte, bf []byte) {
	t.Helper()
	go func() {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return
		}
		defer conn.Close()
		var pid [20]byte
		copy(pid[:], "-MOCKinbound00000000")
		client := peer.NewClient(conn, infoHash, pid)
		if _, err := client.Handshake(); err != nil {
			return
		}
		if err := client.SendBitfield(bf); err != nil {
			return
		}
		for {
			_ = conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
			msg, err := peer.ParseMessage(conn)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					if err := client.SendKeepAlive(); err != nil {
						return
					}
					continue
				}
				return
			}
			if msg == nil {
				continue
			}
			if msg.ID == peer.MsgInterested {
				_ = client.SendUnchoke()
			}
		}
	}()
}

// TestStalledInboundPeerIsNotReaped covers finding #3: the stall reaper must apply
// only to OUTBOUND connections. An inbound peer holds an inbound slot (not the
// outbound slot the reaper is trying to free) and is keyed by an ephemeral source
// port we can't redial, so reaping it would just drop a productive uploader.
func TestStalledInboundPeerIsNotReaped(t *testing.T) {
	defer swapDuration(&peerStallTimeout, 500*time.Millisecond)()
	defer swapDuration(&blockRequestTimeout, 1*time.Hour)()

	sess, bf, _ := newStallTestTorrent(t, 4)
	sess.Start()
	defer sess.Close()
	sess.WaitVerified() // settle background resume verification (the reaper is exempt while it runs)

	sess.mu.RLock()
	port := sess.Port
	infoHash := sess.Torrent.InfoHash
	sess.mu.RUnlock()
	if port == 0 {
		t.Skip("session did not bind an inbound listener")
	}

	dialStalledPeer(t, fmt.Sprintf("127.0.0.1:%d", port), infoHash, bf)

	waitForActivePeers(t, sess, 1, 2*time.Second, "inbound peer never connected")
	// Stays connected well past the stall timeout: inbound peers are exempt.
	time.Sleep(3 * peerStallTimeout)
	if n := len(sess.GetActivePeers()); n != 1 {
		t.Fatalf("inbound peer was reaped (%d active); inbound connections must be exempt from the reaper", n)
	}
}

func trackerPeer(ip string, port uint16) tracker.Peer {
	return tracker.Peer{IP: net.ParseIP(ip), Port: port}
}

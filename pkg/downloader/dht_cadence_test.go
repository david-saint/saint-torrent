package downloader

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/dht"
	"sainttorrent/pkg/peer"
)

// TestDHTLookupIntervalPerStateA2 pins the lookup cadence to what the session
// needs: every torrent used to look up every 30 s, seeding or not. A magnet
// fetching metadata still does; a download short of peers looks up every
// minute; a seed or a well connected download every 15 minutes; a paused or
// metadata-stalled session skips lookups.
func TestDHTLookupIntervalPerStateA2(t *testing.T) {
	check := func(t *testing.T, sess *Session, wantInterval time.Duration, wantPriority dht.LookupPriority, wantSkip bool) {
		t.Helper()
		sess.mu.RLock()
		interval, priority, skip := sess.dhtLookupIntervalLocked()
		sess.mu.RUnlock()
		if interval != wantInterval || priority != wantPriority || skip != wantSkip {
			t.Fatalf("interval %v priority %v skip %v, want %v priority %v skip %v",
				interval, priority, skip, wantInterval, wantPriority, wantSkip)
		}
	}

	t.Run("metadata", func(t *testing.T) {
		sess, _ := newMagnetTestSession(t, 1)
		check(t, sess, dhtMetadataLookupInterval, dht.LookupPriorityHigh, false)
		sess.mu.Lock()
		sess.metadataCompleted = true
		sess.statusErr = fmt.Errorf("storage unavailable")
		sess.mu.Unlock()
		check(t, sess, dhtMetadataLookupInterval, dht.LookupPriorityHigh, true)
	})
	t.Run("downloading", func(t *testing.T) {
		sess := newWireTestSession(t, 4, 16*1024)
		check(t, sess, dhtDownloadLookupInterval, dht.LookupPriorityNormal, false)

		sess.mu.Lock()
		for i := 0; i < dhtWellConnectedPeers; i++ {
			sess.activePeers[fmt.Sprintf("198.51.100.%d:6881", i)] = &peer.Client{}
		}
		sess.mu.Unlock()
		check(t, sess, dhtIdleLookupInterval, dht.LookupPriorityLow, false)
		sess.mu.Lock()
		clear(sess.activePeers) // Close would close their (absent) connections
		sess.mu.Unlock()

		sess.Pause()
		check(t, sess, dhtMetadataLookupInterval, dht.LookupPriorityHigh, true)
	})
	t.Run("seeding", func(t *testing.T) {
		sess, _ := newSeedingWireTestSession(t, 2, 16*1024)
		check(t, sess, dhtIdleLookupInterval, dht.LookupPriorityLow, false)
	})
}

// TestDHTIntervalJitterA2 checks lookup intervals are spread over ±20%, so
// sessions that started together drift out of phase.
func TestDHTIntervalJitterA2(t *testing.T) {
	const d = 100 * time.Second
	lo, hi := d, time.Duration(0)
	for i := 0; i < 2000; i++ {
		got := jitterDHTInterval(d)
		if got < d*8/10 || got > d*12/10 {
			t.Fatalf("jittered interval %v outside [%v, %v]", got, d*8/10, d*12/10)
		}
		lo, hi = min(lo, got), max(hi, got)
	}
	if lo > d*85/100 || hi < d*115/100 {
		t.Fatalf("jitter spans only [%v, %v] of ±20%%", lo, hi)
	}
}

// TestDHTFirstLookupSpreadA2 covers the startup burst: every restored torrent
// looked up exactly 1 s after start. A magnet fetching metadata still does;
// sessions that start together take consecutive dhtStartupSpreadStep slots,
// and past dhtStartupSpreadMax a random point in that window.
func TestDHTFirstLookupSpreadA2(t *testing.T) {
	resetDHTFirstLookupSlotsA3R(t)
	now := time.Now()

	if got := dhtFirstLookupAfter(true, now); got != dhtFirstLookupDelay {
		t.Fatalf("a metadata fetch waits %v for its first lookup, want %v", got, dhtFirstLookupDelay)
	}
	burst := int(dhtStartupSpreadMax / dhtStartupSpreadStep)
	for i := 0; i < burst; i++ {
		got := dhtFirstLookupAfter(false, now)
		lo := dhtFirstLookupDelay + time.Duration(i)*dhtStartupSpreadStep
		if got < lo || got >= lo+dhtStartupSpreadStep {
			t.Fatalf("session %d of a burst: first lookup after %v, want in [%v, %v)", i, got, lo, lo+dhtStartupSpreadStep)
		}
	}
	lo, hi := time.Duration(1<<62), time.Duration(0)
	for i := 0; i < 500; i++ {
		got := dhtFirstLookupAfter(false, now)
		if got < dhtFirstLookupDelay || got >= dhtFirstLookupDelay+dhtStartupSpreadMax {
			t.Fatalf("past the spread: first lookup after %v, want in [%v, %v)", got, dhtFirstLookupDelay, dhtFirstLookupDelay+dhtStartupSpreadMax)
		}
		lo, hi = min(lo, got), max(hi, got)
	}
	if hi-lo < dhtStartupSpreadMax/2 {
		t.Fatalf("past the spread: first lookups span only %v of %v", hi-lo, dhtStartupSpreadMax)
	}
}

// A torrent added once the startup burst has drained looks up at once, however
// many sessions run: the spread used to grow with every running DHT loop, so a
// new download in a library of a few hundred torrents waited up to a minute for
// its first DHT peers.
func TestDHTFirstLookupOfALaterSessionIsNotDelayedA3R(t *testing.T) {
	resetDHTFirstLookupSlotsA3R(t)
	start := time.Now()
	for i := 0; i < 400; i++ { // a large library restored at startup
		dhtFirstLookupAfter(false, start)
	}
	later := start.Add(10 * time.Minute)
	for i := 0; i < 3; i++ {
		got := dhtFirstLookupAfter(false, later)
		lo := dhtFirstLookupDelay + time.Duration(i)*dhtStartupSpreadStep
		if got < lo || got >= lo+dhtStartupSpreadStep {
			t.Fatalf("session %d added later: first lookup after %v, want in [%v, %v)", i, got, lo, lo+dhtStartupSpreadStep)
		}
	}
}

// resetDHTFirstLookupSlotsA3R frees every first-lookup slot for the test and
// again after it, so slots other tests' sessions took (or this test's own)
// do not delay another test's first lookup.
func resetDHTFirstLookupSlotsA3R(t *testing.T) {
	t.Helper()
	reset := func() {
		dhtFirstLookupSlots.mu.Lock()
		dhtFirstLookupSlots.next = time.Time{}
		dhtFirstLookupSlots.mu.Unlock()
	}
	reset()
	t.Cleanup(reset)
}

// TestDHTLookupAfterResumeA2 checks a resume brings the next lookup forward:
// lookups are skipped while paused, and with 15-minute intervals a resumed
// session would otherwise wait up to that long to find peers again.
func TestDHTLookupAfterResumeA2(t *testing.T) {
	resetDHTFirstLookupSlotsA3R(t)
	defer swapDuration(&dhtFirstLookupDelay, 10*time.Millisecond)()
	defer swapDuration(&dhtDownloadLookupInterval, time.Hour)()
	defer swapDuration(&dhtMetadataLookupInterval, time.Hour)()
	defer swapDuration(&dhtResumeLookupDelay, 20*time.Millisecond)()
	lookups := make(chan struct{}, 16)
	oldLookup := startDHTLookup
	startDHTLookup = func(*dht.DHT, [20]byte, uint16, dht.LookupOptions) { lookups <- struct{}{} }
	defer func() { startDHTLookup = oldLookup }()

	d, err := dht.NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("dht: %v", err)
	}
	defer d.Close()
	sess := newWireTestSession(t, 4, 16*1024)
	sess.AttachDHT(d)
	sess.mu.Lock()
	sess.sharedInbound, sess.Port = true, 6881
	sess.mu.Unlock()
	sess.wg.Add(1)
	go sess.dhtLoop()
	defer sess.Close() // stops the loop before the hook is restored

	waitLookup := func(what string) {
		t.Helper()
		select {
		case <-lookups:
		case <-time.After(2 * time.Second):
			t.Fatalf("no DHT lookup %s", what)
		}
	}
	waitLookup("after start")
	sess.Pause()
	select {
	case <-lookups:
		t.Fatal("a DHT lookup ran while paused")
	case <-time.After(100 * time.Millisecond):
	}
	sess.Resume()
	waitLookup("after resume")

	// Start resumes a started session too, and must wake the loop the same way.
	sess.Pause()
	sess.mu.Lock()
	sess.started = true
	sess.mu.Unlock()
	sess.Start()
	waitLookup("after a resume through Start")
}

// TestDHTLookupAfterPauseStartDuringTick deterministically exercises a
// Pause+Start and Pause+Resume cycle completing while dhtLookupTick is still
// running (after releasing s.mu) before dhtLoop returns to select.
func TestDHTLookupAfterPauseStartDuringTick(t *testing.T) {
	resetDHTFirstLookupSlotsA3R(t)
	defer swapDuration(&dhtFirstLookupDelay, 10*time.Millisecond)()
	defer swapDuration(&dhtDownloadLookupInterval, time.Hour)()
	defer swapDuration(&dhtMetadataLookupInterval, time.Hour)()
	defer swapDuration(&dhtResumeLookupDelay, 20*time.Millisecond)()

	lookups := make(chan struct{}, 16)
	oldLookup := startDHTLookup
	startDHTLookup = func(*dht.DHT, [20]byte, uint16, dht.LookupOptions) { lookups <- struct{}{} }
	defer func() { startDHTLookup = oldLookup }()

	d, err := dht.NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("dht: %v", err)
	}
	defer d.Close()
	sess := newWireTestSession(t, 4, 16*1024)
	sess.AttachDHT(d)
	sess.mu.Lock()
	sess.sharedInbound, sess.Port = true, 6881
	sess.mu.Unlock()

	var tickCount int
	var mu sync.Mutex
	inTick := make(chan struct{}, 4)
	proceed := make(chan chan struct{}, 4)

	oldJitter := jitterDHTInterval
	jitterDHTInterval = func(dur time.Duration) time.Duration {
		mu.Lock()
		tickCount++
		n := tickCount
		mu.Unlock()
		if n <= 2 {
			gate := make(chan struct{})
			proceed <- gate
			inTick <- struct{}{}
			select {
			case <-gate:
			case <-sess.ctx.Done():
			}
		}
		return oldJitter(dur)
	}
	defer func() { jitterDHTInterval = oldJitter }()

	sess.wg.Add(1)
	go sess.dhtLoop()
	defer sess.Close() // stops dhtLoop before hooks are restored

	waitLookup := func(what string) {
		t.Helper()
		select {
		case <-lookups:
		case <-time.After(2 * time.Second):
			t.Fatalf("no DHT lookup %s", what)
		}
	}
	waitInTick := func(what string) chan struct{} {
		t.Helper()
		select {
		case <-inTick:
			return <-proceed
		case <-time.After(2 * time.Second):
			t.Fatalf("dhtLookupTick did not reach post-unlock hook %s", what)
			return nil
		}
	}

	waitLookup("after initial start")
	gate := waitInTick("before Pause+Start")
	sess.Pause()
	sess.mu.Lock()
	sess.started = true
	sess.mu.Unlock()
	sess.Start()
	close(gate)
	waitLookup("after Pause+Start during dhtLookupTick")

	gate = waitInTick("before Pause+Resume")
	sess.Pause()
	sess.Resume()
	close(gate)
	waitLookup("after Pause+Resume during dhtLookupTick")
}

type cadencePacket struct {
	data []byte
	addr *net.UDPAddr
}

type cadenceFakeConn struct {
	local  *net.UDPAddr
	in     chan cadencePacket
	closed chan struct{}
	once   sync.Once
	mu     sync.Mutex
	onSend func(to *net.UDPAddr, q, tid string, args map[string]interface{})
}

func newCadenceFakeConn() *cadenceFakeConn {
	return &cadenceFakeConn{
		local:  &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6881},
		in:     make(chan cadencePacket, 64),
		closed: make(chan struct{}),
	}
}

func (c *cadenceFakeConn) ReadFromUDP(b []byte) (int, *net.UDPAddr, error) {
	select {
	case p := <-c.in:
		return copy(b, p.data), p.addr, nil
	case <-c.closed:
		return 0, nil, net.ErrClosed
	}
}

func (c *cadenceFakeConn) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	parsed, err := bencode.Unmarshal(b)
	if err == nil {
		if dict, ok := parsed.(map[string]interface{}); ok && dict["y"] == "q" {
			q, _ := dict["q"].(string)
			tid, _ := dict["t"].(string)
			args, _ := dict["a"].(map[string]interface{})
			c.mu.Lock()
			cb := c.onSend
			c.mu.Unlock()
			if cb != nil {
				cb(&net.UDPAddr{IP: append(net.IP(nil), addr.IP...), Port: addr.Port}, q, tid, args)
			}
		}
	}
	return len(b), nil
}

func (c *cadenceFakeConn) LocalAddr() net.Addr { return c.local }
func (c *cadenceFakeConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *cadenceFakeConn) reply(to *net.UDPAddr, tid string, r map[string]interface{}) {
	payload, err := bencode.Marshal(map[string]interface{}{"t": tid, "y": "r", "r": r})
	if err != nil {
		return
	}
	select {
	case c.in <- cadencePacket{data: payload, addr: to}:
	case <-c.closed:
	}
}

// TestPauseAndCloseCancelDHTLookup verifies Pause and Close call CancelLookup
// so queued lookups never start and running lookups stop without announcing.
func TestPauseAndCloseCancelDHTLookup(t *testing.T) {
	oldHosts := dht.DefaultBootstrapHosts
	router := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6999}
	dht.DefaultBootstrapHosts = []string{router.String()}
	t.Cleanup(func() { dht.DefaultBootstrapHosts = oldHosts })

	conn := newCadenceFakeConn()
	var nodeID [20]byte
	copy(nodeID[:], "cadence-test-node---")
	peerNode := &net.UDPAddr{IP: net.ParseIP("198.51.100.1"), Port: 6881}
	var peerNodeID [20]byte
	copy(peerNodeID[:], "cadence-peer-node---")
	silentNode := &net.UDPAddr{IP: net.ParseIP("198.18.0.1"), Port: 6881}
	var silentNodeID [20]byte
	copy(silentNodeID[:], "cadence-silent-node-")

	var compactPeerNode [26]byte
	copy(compactPeerNode[:20], peerNodeID[:])
	copy(compactPeerNode[20:24], peerNode.IP.To4())
	compactPeerNode[24], compactPeerNode[25] = byte(peerNode.Port>>8), byte(peerNode.Port)

	var compactSilentNode [26]byte
	copy(compactSilentNode[:20], silentNodeID[:])
	copy(compactSilentNode[20:24], silentNode.IP.To4())
	compactSilentNode[24], compactSilentNode[25] = byte(silentNode.Port>>8), byte(silentNode.Port)

	sess1 := newWireTestSession(t, 4, 16*1024)
	sess1.Torrent.InfoHash[0] = 1
	sess2 := newWireTestSession(t, 4, 16*1024)
	sess2.Torrent.InfoHash[0] = 2
	sess3 := newWireTestSession(t, 4, 16*1024)
	sess3.Torrent.InfoHash[0] = 3

	releaseHolders := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseHolders:
		default:
			close(releaseHolders)
		}
	})
	holdersStarted := make(chan struct{}, 8)
	sess1Running := make(chan struct{}, 1)
	sess3SawGetPeers := make(chan struct{}, 1)

	var mu sync.Mutex
	var gotGetPeers [][20]byte
	var gotAnnounce [][20]byte

	conn.mu.Lock()
	conn.onSend = func(to *net.UDPAddr, q, tid string, args map[string]interface{}) {
		switch q {
		case "find_node":
			conn.reply(to, tid, map[string]interface{}{
				"id":    string(nodeID[:]),
				"nodes": string(compactPeerNode[:]),
			})
		case "ping":
			conn.reply(to, tid, map[string]interface{}{"id": string(peerNodeID[:])})
		case "get_peers":
			ihStr, _ := args["info_hash"].(string)
			var ih [20]byte
			copy(ih[:], ihStr)
			mu.Lock()
			gotGetPeers = append(gotGetPeers, ih)
			mu.Unlock()
			if ih == sess1.Torrent.InfoHash {
				if to.IP.Equal(peerNode.IP) {
					conn.reply(to, tid, map[string]interface{}{
						"id":    string(peerNodeID[:]),
						"token": "tok",
						"nodes": string(compactSilentNode[:]),
					})
					select {
					case sess1Running <- struct{}{}:
					default:
					}
				}
				return // silentNode never answers, holding sess1's lookup in flight
			}
			if ih == sess3.Torrent.InfoHash {
				conn.reply(to, tid, map[string]interface{}{"id": string(peerNodeID[:])})
				select {
				case sess3SawGetPeers <- struct{}{}:
				default:
				}
				return
			}
			if ih[0] == 0xAA {
				select {
				case holdersStarted <- struct{}{}:
				default:
				}
				go func() {
					<-releaseHolders
					conn.reply(to, tid, map[string]interface{}{"id": string(peerNodeID[:])})
				}()
				return
			}
			conn.reply(to, tid, map[string]interface{}{"id": string(peerNodeID[:])})
		case "announce_peer":
			ihStr, _ := args["info_hash"].(string)
			var ih [20]byte
			copy(ih[:], ihStr)
			mu.Lock()
			gotAnnounce = append(gotAnnounce, ih)
			mu.Unlock()
		}
	}
	conn.mu.Unlock()

	d, err := dht.NewDHTWithConn(t.TempDir(), conn)
	if err != nil {
		t.Fatalf("dht: %v", err)
	}
	defer d.Close()

	// Wait for bootstrap probe to admit peerNode.
	deadline := time.After(5 * time.Second)
	for d.NodesCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("DHT never admitted bootstrap peerNode")
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Verify Pause cancels a running lookup before it can announce.
	sess1.AttachDHT(d)
	sess1.mu.Lock()
	sess1.sharedInbound, sess1.Port = true, 6881
	sess1.mu.Unlock()
	sess1.dhtLookupTick()
	select {
	case <-sess1Running:
	case <-time.After(5 * time.Second):
		t.Fatal("sess1 lookup never started")
	}
	sess1.Pause()

	// Saturate all 8 slots, queue sess2 and sess3, then Close sess2 before slots
	// reopen so only sess3 runs when the holders finish.
	for i := 0; i < 8; i++ {
		var holder [20]byte
		holder[0] = 0xAA
		holder[19] = byte(i)
		d.LookupWithOptions(holder, 6881, dht.LookupOptions{Priority: dht.LookupPriorityHigh})
	}
	for i := 0; i < 8; i++ {
		select {
		case <-holdersStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("holder lookups did not saturate slots")
		}
	}

	sess2.AttachDHT(d)
	sess2.mu.Lock()
	sess2.sharedInbound, sess2.Port = true, 6881
	sess2.mu.Unlock()
	sess3.AttachDHT(d)
	sess3.mu.Lock()
	sess3.sharedInbound, sess3.Port = true, 6881
	sess3.mu.Unlock()

	sess2.dhtLookupTick()
	sess3.dhtLookupTick()
	sess2.Close()
	close(releaseHolders)

	select {
	case <-sess3SawGetPeers:
	case <-time.After(5 * time.Second):
		t.Fatal("sess3 lookup never ran after holders finished")
	}
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for _, ih := range gotGetPeers {
		if ih == sess2.Torrent.InfoHash {
			t.Fatal("closed sess2's queued lookup ran after Close")
		}
	}
	for _, ih := range gotAnnounce {
		if ih == sess1.Torrent.InfoHash {
			t.Fatal("paused sess1's running lookup sent announce_peer after Pause")
		}
	}
}

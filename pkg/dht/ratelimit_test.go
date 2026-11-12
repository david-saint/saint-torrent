package dht

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
)

// TestQueryLimiterBurstThenBlock verifies an IP may send libtorrent's burst of
// 50 back-to-back queries, and is blocked on the next one.
func TestQueryLimiterBurstThenBlock(t *testing.T) {
	if queryRateBurst != 50 {
		t.Fatalf("query burst is %d, want libtorrent's 50", queryRateBurst)
	}
	l := newQueryLimiter()
	flooder := [4]byte{198, 51, 100, 7}
	other := [4]byte{203, 0, 113, 9}
	now := int64(time.Hour)

	for i := 0; i < queryRateBurst; i++ {
		if !l.allow(flooder, now) {
			t.Fatalf("query %d of the burst was refused", i+1)
		}
	}
	if l.allow(flooder, now) {
		t.Fatal("a query past the burst was allowed")
	}
	if !l.allow(other, now) {
		t.Fatal("another IP was throttled by the flooder's limit")
	}

	// Blocked: even a polite rate is refused, and each query restarts the block.
	last := now
	for i := 1; i <= 10; i++ {
		last = now + int64(i)*int64(time.Minute)
		if l.allow(flooder, last) {
			t.Fatalf("blocked IP was answered %d minutes in", i)
		}
	}
	if l.allow(flooder, last+queryBlockDuration-1) {
		t.Fatal("block lifted before the IP was silent for the full block duration")
	}
	if !l.allow(flooder, last+queryBlockDuration-1+queryBlockDuration) {
		t.Fatal("block never lifted after the IP went silent")
	}
}

func TestQueryLimiterSustainedRateIsNeverBlocked(t *testing.T) {
	l := newQueryLimiter()
	ip := [4]byte{203, 0, 113, 10}
	now := int64(time.Hour)
	for i := 0; i < 1000; i++ {
		if !l.allow(ip, now) {
			t.Fatalf("query %d at exactly the sustained rate was refused", i)
		}
		now += queryRateInterval
	}
}

// TestQueryLimiterCollisionKeepsBlock verifies an address sharing a slot with a
// blocked IP cannot lift that block.
func TestQueryLimiterCollisionKeepsBlock(t *testing.T) {
	l := newQueryLimiter()
	flooder := [4]byte{198, 51, 100, 7}
	var twin [4]byte
	found := false
	for i := uint32(1); i < 1<<24; i++ {
		c := [4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)}
		if l.slot(c) == l.slot(flooder) {
			twin, found = c, true
			break
		}
	}
	if !found {
		t.Fatal("no colliding address found")
	}

	now := int64(time.Hour)
	for i := 0; i <= queryRateBurst; i++ {
		l.allow(flooder, now)
	}
	if !l.allow(twin, now) {
		t.Fatal("colliding address was refused")
	}
	if l.allow(flooder, now+int64(time.Second)) {
		t.Fatal("colliding traffic lifted the flooder's block")
	}
}

// injectQuery feeds a bencoded query into the read loop as if it came from addr.
func (c *fakeConn) injectQuery(t *testing.T, tid, q string, args map[string]interface{}, addr *net.UDPAddr) {
	t.Helper()
	payload, err := bencode.Marshal(map[string]interface{}{"t": tid, "y": "q", "q": q, "a": args})
	if err != nil {
		t.Fatalf("failed to encode query: %v", err)
	}
	select {
	case c.in <- fakePacket{data: payload, addr: cloneUDPAddr(addr)}:
	case <-time.After(5 * time.Second):
		t.Fatal("read loop never consumed the injected query")
	}
}

// responsesToIP counts KRPC responses sent to any port on ip.
func (c *fakeConn) responsesToIP(ip net.IP) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, p := range c.sent {
		if !p.addr.IP.Equal(ip) {
			continue
		}
		parsed, err := bencode.Unmarshal(p.data)
		if err != nil {
			continue
		}
		if dict, ok := parsed.(map[string]interface{}); ok && dict["y"] == "r" {
			count++
		}
	}
	return count
}

// hasResponse reports whether a response with transaction ID tid was sent.
func (c *fakeConn) hasResponse(tid string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.sent {
		parsed, err := bencode.Unmarshal(p.data)
		if err != nil {
			continue
		}
		if dict, ok := parsed.(map[string]interface{}); ok && dict["y"] == "r" && dict["t"] == tid {
			return true
		}
	}
	return false
}

var drainSeq atomic.Uint32

// drainReadLoop waits until every packet injected so far has been handled: the
// read loop handles packets in order, so once a marker ping injected after
// them (from loopback, exempt from rate limiting) is answered, they all were.
func drainReadLoop(t *testing.T, d *DHT, c *fakeConn) {
	t.Helper()
	marker := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9}
	tid := fmt.Sprintf("mk%d", drainSeq.Add(1))
	id := idInBucket(d.nodeID, 3, 0x7777)
	c.injectQuery(t, tid, "ping", map[string]interface{}{"id": string(id[:])}, marker)
	deadline := time.After(5 * time.Second)
	for !c.hasResponse(tid) {
		select {
		case <-deadline:
			t.Fatal("read loop never answered the marker ping")
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestSpoofedQueryFloodIsRateLimited is the reflection scenario: a flood of
// get_peers "from" one victim IP, on rotating ports, must yield only a small
// burst of responses toward that IP, while other senders are still answered.
func TestSpoofedQueryFloodIsRateLimited(t *testing.T) {
	d, conn := newFakeDHT(t)

	victim := net.ParseIP("198.51.100.7")
	var infoHash [20]byte
	copy(infoHash[:], "reflected-info-hash-")
	for i := 0; i < 200; i++ {
		id := idInBucket(d.nodeID, 40, uint16(i))
		conn.injectQuery(t, "gp", "get_peers", map[string]interface{}{
			"id":        string(id[:]),
			"info_hash": string(infoHash[:]),
		}, &net.UDPAddr{IP: victim, Port: 1000 + i})
	}
	drainReadLoop(t, d, conn)

	// Allow one refill in case the injection straddled a rate interval.
	if got := conn.responsesToIP(victim); got > queryRateBurst+1 {
		t.Fatalf("200 spoofed queries produced %d responses toward the victim, want at most %d", got, queryRateBurst+1)
	}

	honest := &net.UDPAddr{IP: net.ParseIP("203.0.113.20"), Port: 6881}
	honestID := idInBucket(d.nodeID, 41, 1)
	conn.injectQuery(t, "hp", "ping", map[string]interface{}{"id": string(honestID[:])}, honest)
	drainReadLoop(t, d, conn)
	if got := conn.responsesToIP(honest.IP); got != 1 {
		t.Fatalf("an unrelated sender got %d responses, want 1", got)
	}
}

// TestRateLimitExemptions verifies loopback senders are never throttled and a
// blocked IP's responses to our own queries are still delivered.
func TestRateLimitExemptions(t *testing.T) {
	d, conn := newFakeDHT(t)

	local := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 7000}
	id := idInBucket(d.nodeID, 42, 1)
	const flood = 2 * queryRateBurst
	for i := 0; i < flood; i++ {
		conn.injectQuery(t, "lp", "ping", map[string]interface{}{"id": string(id[:])}, local)
	}
	drainReadLoop(t, d, conn)
	// Every answer to the sender plus one to the drain marker, also on loopback.
	if got := conn.responsesToIP(local.IP); got != flood+1 {
		t.Fatalf("loopback sender was throttled: %d responses to %d queries", got, flood+1)
	}

	blocked := &net.UDPAddr{IP: net.ParseIP("198.51.100.8"), Port: 6881}
	blockedID := idInBucket(d.nodeID, 43, 1)
	for i := 0; i < flood; i++ {
		conn.injectQuery(t, "bp", "ping", map[string]interface{}{"id": string(blockedID[:])}, blocked)
	}
	drainReadLoop(t, d, conn)
	if got := conn.responsesToIP(blocked.IP); got > queryRateBurst+1 {
		t.Fatalf("the flooding sender got %d responses, want it blocked after %d", got, queryRateBurst)
	}

	ch := make(chan interface{}, 1)
	d.registerTransaction("ours", blocked, ch)
	defer d.unregisterTransaction("ours")
	conn.injectPingReply(t, "ours", blockedID, blocked)
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("a response to our own query was dropped by the query rate limit")
	}
}

// TestQueriesFromInvalidSourcesAreDropped verifies queries whose source could
// never be a peer get no answer.
func TestQueriesFromInvalidSourcesAreDropped(t *testing.T) {
	d, conn := newFakeDHT(t)
	id := idInBucket(d.nodeID, 44, 1)
	for _, src := range []*net.UDPAddr{
		{IP: net.ParseIP("0.0.0.0"), Port: 6881},
		{IP: net.ParseIP("224.0.0.251"), Port: 5353},
		{IP: net.ParseIP("255.255.255.255"), Port: 6881},
		{IP: net.ParseIP("203.0.113.30"), Port: 0},
		{IP: net.ParseIP("2001:db8::1"), Port: 6881},
	} {
		conn.injectQuery(t, "iv", "ping", map[string]interface{}{"id": string(id[:])}, src)
		drainReadLoop(t, d, conn)
		if got := conn.responsesToIP(src.IP); got != 0 {
			t.Fatalf("query from %s was answered", src)
		}
	}
}

func TestResponseBudgetBurstThenRate(t *testing.T) {
	var b responseBudget
	now := int64(time.Hour)
	sent := 0
	for b.take(1000, now) {
		sent += 1000
	}
	if sent < responseByteBurst-1000 || sent > responseByteBurst {
		t.Fatalf("burst let %d bytes through, want about %d", sent, responseByteBurst)
	}
	now += int64(time.Second)
	sent = 0
	for b.take(1000, now) {
		sent += 1000
	}
	if sent < responseByteRate-1000 || sent > responseByteRate {
		t.Fatalf("one second of refill let %d bytes through, want about %d", sent, responseByteRate)
	}
}

// TestSpoofedRandomSourceFloodIsBudgeted covers the flood the per-IP limiter
// cannot see: every query from a different source address. What we send back
// must stay within the global response budget.
func TestSpoofedRandomSourceFloodIsBudgeted(t *testing.T) {
	d, conn := newFakeDHT(t)
	fillBucket(t, d, 0, 9) // find_node answers then carry 8 contacts

	start := time.Now()
	var target [20]byte
	copy(target[:], "flooded-find-target-")
	for i := 0; i < 4000; i++ {
		id := idInBucket(d.nodeID, 1+i%100, uint16(i))
		src := &net.UDPAddr{IP: net.IPv4(198, 18, byte(i>>8), byte(i)), Port: 6881}
		d.handleQuery("tx", "find_node", map[string]interface{}{"id": string(id[:]), "target": string(target[:])}, src)
	}
	elapsed := time.Since(start)

	conn.mu.Lock()
	sent := 0
	for _, p := range conn.sent {
		sent += len(p.data)
	}
	conn.mu.Unlock()
	// Probe pings are small and few; allow one response of slack besides.
	limit := responseByteBurst + int(elapsed.Seconds()*responseByteRate) + maxQuerySenderProbes*100 + 1024
	if sent > limit {
		t.Fatalf("a random-source flood pushed out %d bytes in %v, budget allows %d", sent, elapsed, limit)
	}
}

// BenchmarkQueryLimiterAllow shows the per-query gate is a few nanoseconds and
// allocation-free on the read loop.
func BenchmarkQueryLimiterAllow(b *testing.B) {
	l := newQueryLimiter()
	b.ReportAllocs()
	now := int64(time.Hour)
	for i := 0; i < b.N; i++ {
		ip := [4]byte{203, byte(i >> 16), byte(i >> 8), byte(i)}
		l.allow(ip, now)
		now += 1000
	}
}

func TestQueryBudgetReserveBurstAndRate(t *testing.T) {
	b := queryBudget{rate: 1000, burst: 2000, ceiling: 4000}
	now := int64(time.Hour)

	if w := b.reserve(2000, now); w != 0 {
		t.Fatalf("within burst waited %v, want 0", w)
	}
	if w := b.reserve(500, now); w != 500*time.Millisecond {
		t.Fatalf("500 bytes past burst waited %v, want 500ms", w)
	}
	// Another 500 bytes queues behind the previous reservation.
	if w := b.reserve(500, now); w != time.Second {
		t.Fatalf("second 500 bytes waited %v, want 1s", w)
	}
}

func TestQueryBudgetTryTakeHardCeiling(t *testing.T) {
	b := queryBudget{rate: 1000, burst: 1000, ceiling: 2000}
	now := int64(time.Hour)

	// Drain burst via reserve; pings/find_node still succeed up to ceiling.
	if w := b.reserve(1000, now); w != 0 {
		t.Fatalf("burst wait = %v, want 0", w)
	}
	if !b.tryTake(1000, now) {
		t.Fatal("tryTake below hard ceiling failed")
	}
	if b.tryTake(1, now) {
		t.Fatal("tryTake past hard ceiling succeeded")
	}
	// After 500ms, 500 bytes refill so tryTake accepts up to 500 bytes again.
	if !b.tryTake(500, now+int64(500*time.Millisecond)) {
		t.Fatal("tryTake after refill failed")
	}
}

// TestSendQueryWaitsForBudgetBeforeTimeout verifies get_peers and announce_peer
// wait for outbound budget before their response timeout starts, so waiting
// behind the rate limit does not cause premature query timeouts.
func TestSendQueryWaitsForBudgetBeforeTimeout(t *testing.T) {
	oldTimeout := dhtLookupQueryTimeout
	dhtLookupQueryTimeout = 80 * time.Millisecond
	t.Cleanup(func() { dhtLookupQueryTimeout = oldTimeout })

	d, conn := newFakeDHT(t)
	d.queryBudget = queryBudget{rate: 1000, burst: 100, ceiling: 5000}

	target := &net.UDPAddr{IP: net.ParseIP("198.51.100.10"), Port: 6881}
	responderID := idInBucket(d.nodeID, 10, 1)
	conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"id": idString(responderID), "token": "tok"}
	})

	// Exhaust burst + 150ms of budget (> dhtLookupQueryTimeout = 80ms) before
	// issuing get_peers and announce_peer. If the timeout started before the
	// budget wait, both would time out before ever sending on UDP.
	d.queryBudget.reserve(250, int64(time.Since(d.started)))

	var infoHash [20]byte
	copy(infoHash[:], "budget-timeout-hash-")

	res, err := d.getPeersQuery(context.Background(), infoHash, target)
	if err != nil || res == nil || res.ID != responderID {
		t.Fatalf("getPeersQuery failed after budget wait: res=%v err=%v", res, err)
	}
	if err := d.announcePeerQuery(context.Background(), infoHash, 51413, "tok", target); err != nil {
		t.Fatalf("announcePeerQuery failed after budget wait: %v", err)
	}

	// Verify budget cancellation aborts the wait immediately without sending.
	d.queryBudget.reserve(2000, int64(time.Since(d.started)))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	before := conn.queriesTo(target, "get_peers")
	if _, err := d.getPeersQuery(ctx, infoHash, target); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled budget wait err = %v, want DeadlineExceeded", err)
	}
	if after := conn.queriesTo(target, "get_peers"); after != before {
		t.Fatalf("cancelled budget wait still transmitted %d queries", after-before)
	}
}

// TestSendQueryNonWaitingDropsPastCeiling verifies pings and bootstrap
// find_node queries never wait on the budget and are dropped past the hard
// ceiling (so a dropped probe simply does not admit the node).
func TestSendQueryNonWaitingDropsPastCeiling(t *testing.T) {
	d, conn := newFakeDHT(t)
	d.queryBudget = queryBudget{rate: 1000, burst: 100, ceiling: 150}

	target := &net.UDPAddr{IP: net.ParseIP("198.51.100.20"), Port: 6881}
	responderID := idInBucket(d.nodeID, 12, 1)
	conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"id": idString(responderID)}
	})

	// Push the budget just past burst (100B) but below ceiling (150B): a ping
	// (~55B) would exceed ceiling, so both ping and find_node must drop
	// immediately without waiting.
	d.queryBudget.reserve(130, int64(time.Since(d.started)))

	start := time.Now()
	if _, err := d.queryNodeID(context.Background(), target); !errors.Is(err, errQueryBudgetExceeded) {
		t.Fatalf("ping past ceiling err = %v, want errQueryBudgetExceeded", err)
	}
	if _, err := d.findNode(context.Background(), d.nodeID, target); !errors.Is(err, errQueryBudgetExceeded) {
		t.Fatalf("find_node past ceiling err = %v, want errQueryBudgetExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("non-waiting queries waited %v past ceiling", elapsed)
	}
	if conn.queriesTo(target, "ping") != 0 || conn.queriesTo(target, "find_node") != 0 {
		t.Fatal("dropped queries still wrote packets to UDP")
	}

	// Below ceiling (even past burst), ping succeeds immediately without waiting.
	d2, conn2 := newFakeDHT(t)
	d2.queryBudget = queryBudget{rate: 1000, burst: 50, ceiling: 500}
	conn2.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"id": idString(responderID)}
	})
	d2.queryBudget.reserve(80, int64(time.Since(d2.started))) // past burst=50, below ceiling=500
	start = time.Now()
	id, err := d2.queryNodeID(context.Background(), target)
	if err != nil || id != responderID {
		t.Fatalf("ping between burst and ceiling failed: err=%v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("ping between burst and ceiling waited %v", elapsed)
	}
}

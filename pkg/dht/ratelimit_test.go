package dht

import (
	"net"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
)

func TestQueryLimiterBurstThenBlock(t *testing.T) {
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

// drainReadLoop waits until every packet injected so far has been handled, by
// sending a loopback ping (exempt from rate limiting) and awaiting its answer.
func drainReadLoop(t *testing.T, d *DHT, c *fakeConn) {
	t.Helper()
	marker := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9}
	before := c.responsesToIP(marker.IP)
	id := idInBucket(d.nodeID, 3, 0x7777)
	c.injectQuery(t, "mk", "ping", map[string]interface{}{"id": string(id[:])}, marker)
	deadline := time.After(5 * time.Second)
	for c.responsesToIP(marker.IP) == before {
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
	for i := 0; i < 50; i++ {
		conn.injectQuery(t, "lp", "ping", map[string]interface{}{"id": string(id[:])}, local)
	}
	drainReadLoop(t, d, conn)
	if got := conn.responsesToIP(local.IP); got < 51 {
		t.Fatalf("loopback sender was throttled: %d responses to 51 queries", got)
	}

	blocked := &net.UDPAddr{IP: net.ParseIP("198.51.100.8"), Port: 6881}
	blockedID := idInBucket(d.nodeID, 43, 1)
	for i := 0; i < 50; i++ {
		conn.injectQuery(t, "bp", "ping", map[string]interface{}{"id": string(blockedID[:])}, blocked)
	}
	drainReadLoop(t, d, conn)

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

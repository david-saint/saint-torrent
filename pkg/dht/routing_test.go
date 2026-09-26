package dht

import (
	"net"
	"strconv"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
)

// checkRoutingIndex verifies nodeAddrs and nodeIPs describe exactly the
// contacts in the buckets, and that no non-loopback IP holds two contacts.
func checkRoutingIndex(t *testing.T, d *DHT) {
	t.Helper()
	d.mu.RLock()
	defer d.mu.RUnlock()

	addrs := make(map[nodeAddrKey][20]byte)
	ips := make(map[[4]byte]int)
	for idx, b := range d.buckets {
		if b == nil {
			continue
		}
		if len(b.nodes) > bucketSize {
			t.Fatalf("bucket %d holds %d contacts", idx, len(b.nodes))
		}
		for _, n := range b.nodes {
			k, ok := nodeAddrKeyOf(n.Addr)
			if !ok {
				t.Fatalf("stored contact has an unusable address %v", n.Addr)
			}
			if bucketIndex(d.nodeID, n.ID) != idx {
				t.Fatalf("contact %x stored in bucket %d", n.ID, idx)
			}
			if _, dup := addrs[k]; dup {
				t.Fatalf("endpoint %v stored twice", n.Addr)
			}
			addrs[k] = n.ID
			ips[k.ip]++
		}
	}
	for ip, n := range ips {
		if n > 1 && onePerIP(ip) {
			t.Fatalf("IP %v holds %d contacts", ip, n)
		}
	}
	if len(addrs) != len(d.nodeAddrs) || len(ips) != len(d.nodeIPs) {
		t.Fatalf("index holds %d endpoints/%d IPs, buckets hold %d/%d", len(d.nodeAddrs), len(d.nodeIPs), len(addrs), len(ips))
	}
	for k, id := range addrs {
		if d.nodeAddrs[k] != id {
			t.Fatalf("index maps %v to %x, bucket holds %x", k, d.nodeAddrs[k], id)
		}
	}
	for ip, n := range ips {
		if d.nodeIPs[ip] != n {
			t.Fatalf("index counts %d contacts on %v, buckets hold %d", d.nodeIPs[ip], ip, n)
		}
	}
}

// queriesToIP counts outbound queries of type q sent to any port on ip.
func (c *fakeConn) queriesToIP(ip net.IP, q string) int {
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
		if dict, ok := parsed.(map[string]interface{}); ok && dict["y"] == "q" && dict["q"] == q {
			count++
		}
	}
	return count
}

// fillBucket stores bucketSize fresh contacts in bucket, each on its own IP.
func fillBucket(t *testing.T, d *DHT, bucket int, ipBase byte) []Node {
	t.Helper()
	for i := 0; i < bucketSize; i++ {
		id := idInBucket(d.nodeID, bucket, uint16(i+1))
		d.addNode(id, &net.UDPAddr{IP: net.IPv4(10, ipBase, byte(bucket), byte(i+1)), Port: 6881})
	}
	nodes := bucketNodes(d, bucket)
	if len(nodes) != bucketSize {
		t.Fatalf("bucket %d holds %d contacts after filling, want %d", bucket, len(nodes), bucketSize)
	}
	return nodes
}

// TestSpoofedQueryFloodDoesNotFillRoutingTable is the reported poisoning: pings
// "from" one victim IP on many ports, with IDs crafted for every bucket, must
// not put a single unverified contact in the table (and so none can be served
// to other nodes), and must cost at most one probe toward the victim at a time.
func TestSpoofedQueryFloodDoesNotFillRoutingTable(t *testing.T) {
	d, conn := newFakeDHT(t)
	victim := net.ParseIP("198.51.100.7")

	for bucket := 0; bucket < 144; bucket++ {
		for j := 0; j < bucketSize; j++ {
			id := idInBucket(d.nodeID, bucket, uint16(j+1))
			addr := &net.UDPAddr{IP: victim, Port: 1024 + bucket*bucketSize + j}
			d.handleQuery("tx", "ping", map[string]interface{}{"id": string(id[:])}, addr)
		}
	}

	if got := d.NodesCount(); got != 0 {
		t.Fatalf("%d spoofed query senders were admitted without answering a probe", got)
	}
	if got := conn.queriesToIP(victim, "ping"); got > 1 {
		t.Fatalf("the flood made us probe the victim %d times, want at most 1 in flight", got)
	}
	var target [20]byte
	copy(target[:], "any-lookup-target---")
	if nodes := d.getCloserNodes(target, 8); len(nodes) != 0 {
		t.Fatalf("find_node would hand out %d unverified contacts", len(nodes))
	}
}

// TestRoutingTableHoldsOneContactPerIP verifies a host that genuinely answers
// under many IDs and ports (a Sybil) still holds a single contact, while
// loopback test networks keep one contact per port.
func TestRoutingTableHoldsOneContactPerIP(t *testing.T) {
	d, _ := newFakeDHT(t)

	sybil := net.ParseIP("198.51.100.9")
	for i := 0; i < 50; i++ {
		d.addNode(idInBucket(d.nodeID, i, 1), &net.UDPAddr{IP: sybil, Port: 2000 + i})
	}
	if got := d.NodesCount(); got != 1 {
		t.Fatalf("one IP holds %d contacts, want 1", got)
	}

	for i := 0; i < 10; i++ {
		d.addNode(idInBucket(d.nodeID, 60+i, 1), &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3000 + i})
	}
	if got := d.NodesCount(); got != 11 {
		t.Fatalf("loopback contacts were collapsed: table holds %d, want 11", got)
	}
	checkRoutingIndex(t, d)
}

// TestVerifiedReplyReplacesStaleIDAtEndpoint verifies an endpoint that answers
// as a new ID (a node restarted with a fresh ID) replaces the stale contact.
func TestVerifiedReplyReplacesStaleIDAtEndpoint(t *testing.T) {
	d, _ := newFakeDHT(t)
	addr := &net.UDPAddr{IP: net.ParseIP("198.51.100.10"), Port: 6881}
	old := idInBucket(d.nodeID, 12, 1)
	fresh := idInBucket(d.nodeID, 30, 1)

	d.addNode(old, addr)
	d.addNode(fresh, addr)

	if nodes := bucketNodes(d, 12); len(nodes) != 0 {
		t.Fatalf("stale ID %x kept its endpoint", nodes[0].ID)
	}
	if nodes := bucketNodes(d, 30); len(nodes) != 1 || nodes[0].ID != fresh {
		t.Fatalf("the ID the endpoint answered with was not admitted: %v", nodes)
	}
	checkRoutingIndex(t, d)
}

// TestFullBucketOfFreshContactsDiscardsNewcomer verifies BEP 5 replacement: a
// bucket whose contacts were all heard from recently is never disturbed, so a
// single lost ping can no longer swap a live node for a newcomer.
func TestFullBucketOfFreshContactsDiscardsNewcomer(t *testing.T) {
	const bucket = 21
	d, conn := newFakeDHT(t)
	nodes := fillBucket(t, d, bucket, 1)

	newcomer := idInBucket(d.nodeID, bucket, 99)
	d.addNode(newcomer, &net.UDPAddr{IP: net.ParseIP("198.51.100.11"), Port: 6881})

	time.Sleep(50 * time.Millisecond)
	if got := conn.queriesTo(nodes[0].Addr, "ping"); got != 0 {
		t.Fatalf("a fresh head was pinged %d times to make room", got)
	}
	after := bucketNodes(d, bucket)
	for i := range nodes {
		if after[i].ID != nodes[i].ID {
			t.Fatalf("bucket changed at %d: %x, want %x", i, after[i].ID, nodes[i].ID)
		}
	}
	checkRoutingIndex(t, d)
}

// TestQuestionableHeadReplacement covers how a questionable head is challenged
// for a newcomer: it keeps its slot if it answers even on the retry, is
// replaced only after two missed pings, and is replaced at once if its address
// answers as a different node.
func TestQuestionableHeadReplacement(t *testing.T) {
	setup := func(t *testing.T, bucket int) (*DHT, *fakeConn, Node, [20]byte, *net.UDPAddr) {
		d, conn := newFakeDHT(t)
		nodes := fillBucket(t, d, bucket, 2)
		head := nodes[0]
		backdateNode(t, d, bucket, head.ID, time.Now().Add(-nodeQuestionableAfter-time.Minute))
		newcomer := idInBucket(d.nodeID, bucket, 99)
		newAddr := &net.UDPAddr{IP: net.ParseIP("198.51.100.12"), Port: 6881}
		d.addNode(newcomer, newAddr)
		return d, conn, head, newcomer, newAddr
	}
	awaitPings := func(t *testing.T, c *fakeConn, addr *net.UDPAddr, n int) string {
		t.Helper()
		deadline := time.After(3 * nodePingTimeout)
		for c.queriesTo(addr, "ping") < n {
			select {
			case <-deadline:
				t.Fatalf("expected %d pings to %s, saw %d", n, addr, c.queriesTo(addr, "ping"))
			case <-time.After(5 * time.Millisecond):
			}
		}
		tid, _ := c.lastQueryTo(addr, "ping")
		return tid
	}
	awaitIdle := func(t *testing.T, d *DHT, bucket int) {
		t.Helper()
		deadline := time.After(3 * nodePingTimeout)
		for {
			d.mu.RLock()
			busy := d.buckets[bucket].pingInProgress
			d.mu.RUnlock()
			if !busy {
				return
			}
			select {
			case <-deadline:
				t.Fatal("head challenge never finished")
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	holds := func(d *DHT, bucket int, id [20]byte) bool {
		for _, n := range bucketNodes(d, bucket) {
			if n.ID == id {
				return true
			}
		}
		return false
	}

	t.Run("answers the retry", func(t *testing.T) {
		t.Parallel()
		const bucket = 22
		d, conn, head, newcomer, _ := setup(t, bucket)
		awaitPings(t, conn, head.Addr, 1)
		tid := awaitPings(t, conn, head.Addr, 2)
		if !holds(d, bucket, head.ID) {
			t.Fatal("head was evicted after a single missed ping")
		}
		conn.injectPingReply(t, tid, head.ID, head.Addr)
		awaitIdle(t, d, bucket)
		if !holds(d, bucket, head.ID) || holds(d, bucket, newcomer) {
			t.Fatal("a head that answered the retry lost its slot")
		}
		checkRoutingIndex(t, d)
	})

	t.Run("misses twice", func(t *testing.T) {
		t.Parallel()
		const bucket = 23
		d, conn, head, newcomer, newAddr := setup(t, bucket)
		awaitPings(t, conn, head.Addr, 2)
		awaitIdle(t, d, bucket)
		if holds(d, bucket, head.ID) {
			t.Fatal("head survived two missed pings")
		}
		nodes := bucketNodes(d, bucket)
		if last := nodes[len(nodes)-1]; last.ID != newcomer || !sameUDPAddr(last.Addr, newAddr) {
			t.Fatalf("newcomer was not admitted in the head's place: tail is %x", last.ID)
		}
		checkRoutingIndex(t, d)
	})

	t.Run("answers as another node", func(t *testing.T) {
		t.Parallel()
		const bucket = 24
		d, conn, head, newcomer, _ := setup(t, bucket)
		tid := awaitPings(t, conn, head.Addr, 1)
		conn.injectPingReply(t, tid, idInBucket(d.nodeID, 90, 1), head.Addr)
		awaitIdle(t, d, bucket)
		if holds(d, bucket, head.ID) || !holds(d, bucket, newcomer) {
			t.Fatal("a head whose address answered as another node was kept")
		}
		if got := conn.queriesTo(head.Addr, "ping"); got != 1 {
			t.Fatalf("a proven-wrong head was pinged %d times, want 1", got)
		}
		checkRoutingIndex(t, d)
	})
}

// TestRepointKeepsOneContactPerIP verifies a verified address change cannot
// move a contact onto an IP that already holds another one, while a move to a
// new port on its own IP (a NAT rebinding) still applies.
func TestRepointKeepsOneContactPerIP(t *testing.T) {
	d, _ := newFakeDHT(t)
	a := idInBucket(d.nodeID, 5, 1)
	b := idInBucket(d.nodeID, 6, 1)
	aAddr := &net.UDPAddr{IP: net.ParseIP("198.51.100.20"), Port: 6881}
	bAddr := &net.UDPAddr{IP: net.ParseIP("198.51.100.21"), Port: 6881}
	d.addNode(a, aAddr)
	d.addNode(b, bAddr)

	if d.repointNode(a, aAddr, &net.UDPAddr{IP: bAddr.IP, Port: 7000}) {
		t.Fatal("a contact was moved onto an IP that already holds another")
	}
	if d.repointNode(a, aAddr, bAddr) {
		t.Fatal("a contact was moved onto another contact's endpoint")
	}
	rebound := &net.UDPAddr{IP: aAddr.IP, Port: 7001}
	if !d.repointNode(a, aAddr, rebound) {
		t.Fatal("a move to a new port on the contact's own IP was refused")
	}
	if !d.HasNodeAddress(rebound.IP, uint16(rebound.Port)) || d.HasNodeAddress(aAddr.IP, uint16(aAddr.Port)) {
		t.Fatal("index was not updated by the re-point")
	}
	checkRoutingIndex(t, d)
}

// TestAddNodeSkipsKnownEndpointsAndIPs verifies a PORT message for an endpoint
// already in the table, or for another port on an IP that already holds a
// contact, costs no probe.
func TestAddNodeSkipsKnownEndpointsAndIPs(t *testing.T) {
	d, conn := newFakeDHT(t)
	known := &net.UDPAddr{IP: net.ParseIP("198.51.100.30"), Port: 6881}
	d.addNode(idInBucket(d.nodeID, 9, 1), known)

	for i := 0; i < 20; i++ {
		d.AddNode(known.IP, uint16(known.Port))
		d.AddNode(known.IP, uint16(7000+i))
	}
	time.Sleep(20 * time.Millisecond)
	if got := conn.queriesToIP(known.IP, "ping"); got != 0 {
		t.Fatalf("PORT messages for a known IP cost %d probes", got)
	}
	if !d.HasNodeAddress(known.IP, uint16(known.Port)) {
		t.Fatal("HasNodeAddress missed a stored endpoint")
	}
	if d.HasNodeAddress(known.IP, 7000) {
		t.Fatal("HasNodeAddress matched an endpoint that is not stored")
	}
}

// TestBootstrapReferralsAreProbedNotInserted verifies a bootstrap router's
// referrals only enter the table after answering a ping of our own, under the
// ID they answer with.
func TestBootstrapReferralsAreProbedNotInserted(t *testing.T) {
	router := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6999}
	old := DefaultBootstrapHosts
	DefaultBootstrapHosts = []string{net.JoinHostPort(router.IP.String(), strconv.Itoa(router.Port))}
	t.Cleanup(func() { DefaultBootstrapHosts = old })

	d, conn := newFakeDHT(t)
	tid := awaitQueryTo(t, conn, router, "find_node")

	referral := &net.UDPAddr{IP: net.ParseIP("198.51.100.40"), Port: 6881}
	claimed := idInBucket(d.nodeID, 10, 1)
	routerID := idInBucket(d.nodeID, 11, 1)
	payload, err := bencode.Marshal(map[string]interface{}{
		"t": tid,
		"y": "r",
		"r": map[string]interface{}{
			"id":    string(routerID[:]),
			"nodes": compactNodes([]Node{{ID: claimed, Addr: referral}}),
		},
	})
	if err != nil {
		t.Fatalf("failed to encode find_node reply: %v", err)
	}
	conn.in <- fakePacket{data: payload, addr: router}

	ping := awaitQueryTo(t, conn, referral, "ping")
	if got := d.NodesCount(); got != 0 {
		t.Fatalf("a bootstrap referral was inserted before answering: %d nodes", got)
	}
	answered := idInBucket(d.nodeID, 20, 1)
	conn.injectPingReply(t, ping, answered, referral)
	deadline := time.After(5 * time.Second)
	for storedAddrFor(d, answered) == nil {
		select {
		case <-deadline:
			t.Fatal("the referral was never admitted after answering")
		case <-time.After(2 * time.Millisecond):
		}
	}
	if storedAddrFor(d, claimed) != nil {
		t.Fatal("the referral was stored under the router's claimed ID")
	}
}

// BenchmarkAddNodeKnownEndpoint measures the PORT-message path for an endpoint
// already in a large routing table: an O(1) index lookup, not a table scan.
func BenchmarkAddNodeKnownEndpoint(b *testing.B) {
	conn := newFakeConn()
	d, err := NewDHTWithConn("", conn)
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	for bucket := 0; bucket < 144; bucket++ {
		for j := 0; j < bucketSize; j++ {
			d.addNode(idInBucket(d.nodeID, bucket, uint16(j+1)), &net.UDPAddr{IP: net.IPv4(10, byte(bucket), byte(j), 1), Port: 6881})
		}
	}
	ip := net.IPv4(10, 143, 7, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.AddNode(ip, 6881)
	}
}

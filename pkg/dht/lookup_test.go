package dht

import (
	"net"
	"strconv"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
)

func idString(id [20]byte) string { return string(id[:]) }

// queried returns every address that received a query of type q.
func (c *fakeConn) queried(q string) []*net.UDPAddr {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*net.UDPAddr
	for _, p := range c.sent {
		parsed, err := bencode.Unmarshal(p.data)
		if err != nil {
			continue
		}
		if dict, ok := parsed.(map[string]interface{}); ok && dict["y"] == "q" && dict["q"] == q {
			out = append(out, p.addr)
		}
	}
	return out
}

// idAtDistance returns an ID whose XOR distance to target is d in the first
// byte, so candidates can be ordered deterministically; salt varies the last
// byte to keep IDs distinct.
func idAtDistance(target [20]byte, d byte, salt byte) [20]byte {
	id := target
	id[0] ^= d
	id[19] ^= salt
	return id
}

// TestLookupStoresResponderUnderReportedID is the reported forgery: a referrer
// pairs a live node's address with an ID next to the info-hash. The node must
// be stored under the ID it reports itself, and the forged ID never.
func TestLookupStoresResponderUnderReportedID(t *testing.T) {
	d, conn := newFakeDHT(t)

	var infoHash [20]byte
	copy(infoHash[:], "lookup-target-hash--")
	start := &net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 6881}
	startID := idInBucket(d.nodeID, 3, 1)
	honest := &net.UDPAddr{IP: net.ParseIP("203.0.113.2"), Port: 6881}
	honestID := idInBucket(d.nodeID, 50, 1)
	forged := infoHash
	forged[19] ^= 1

	conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		switch {
		case q == "get_peers" && sameUDPAddr(to, start):
			return map[string]interface{}{
				"id":    idString(startID),
				"nodes": compactNodes([]Node{{ID: forged, Addr: honest}}),
			}
		case q == "get_peers" && sameUDPAddr(to, honest):
			return map[string]interface{}{"id": idString(honestID)}
		}
		return nil
	})
	d.addNode(startID, start)

	d.lookup(infoHash, 0, LookupOptions{})

	if storedAddrFor(d, forged) != nil {
		t.Fatal("the responder was stored under the ID its referrer claimed")
	}
	if addr := storedAddrFor(d, honestID); addr == nil || !sameUDPAddr(addr, honest) {
		t.Fatalf("the responder was not stored under its own ID: %v", addr)
	}
	if closest := d.getCloserNodes(infoHash, 1); len(closest) == 1 && closest[0].ID == forged {
		t.Fatal("the forged ID became our closest contact to the info-hash")
	}
}

// TestLookupIgnoresResponsesWithoutValidID verifies a get_peers answer without
// a 20-byte responder ID is discarded rather than stored under a mangled ID.
func TestLookupIgnoresResponsesWithoutValidID(t *testing.T) {
	d, conn := newFakeDHT(t)

	var infoHash [20]byte
	copy(infoHash[:], "lookup-target-hash--")
	start := &net.UDPAddr{IP: net.ParseIP("203.0.113.3"), Port: 6881}
	startID := idInBucket(d.nodeID, 4, 1)
	conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		if q == "get_peers" {
			return map[string]interface{}{"id": "short"}
		}
		return nil
	})
	d.addNode(startID, start)
	d.lookup(infoHash, 0, LookupOptions{})

	if got := d.NodesCount(); got != 1 || storedAddrFor(d, startID) == nil {
		t.Fatalf("a response without a valid ID changed the table: %d contacts", got)
	}
}

// TestLookupTraversalResistsHijack is the reported hijack: one responder
// answering with 150 referrals (special-purpose addresses first, then 145
// ports on one victim IP) must not reach any special address, must spend at
// most one query on the victim IP, and must not stop the lookup reaching the
// genuinely closest node.
func TestLookupTraversalResistsHijack(t *testing.T) {
	d, conn := newFakeDHT(t)

	var infoHash [20]byte
	copy(infoHash[:], "hijacked-info-hash--")
	attacker := &net.UDPAddr{IP: net.ParseIP("203.0.113.66"), Port: 6881}
	attackerID := idAtDistance(infoHash, 0x80, 1)
	honest := &net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 6881}
	honestID := idAtDistance(infoHash, 0x81, 1)
	closest := &net.UDPAddr{IP: net.ParseIP("203.0.113.20"), Port: 6881}
	victim := net.ParseIP("198.51.100.7")

	special := []*net.UDPAddr{
		{IP: net.ParseIP("0.0.0.0"), Port: 53},
		{IP: net.ParseIP("127.0.0.1"), Port: 631},
		{IP: net.ParseIP("192.168.1.1"), Port: 1900},
		{IP: net.ParseIP("224.0.0.251"), Port: 5353},
		{IP: net.ParseIP("198.51.100.9"), Port: 0},
	}
	var flood []Node
	for i, a := range special {
		flood = append(flood, Node{ID: idAtDistance(infoHash, 0, byte(10+i)), Addr: a})
	}
	for i := 0; i < 145; i++ {
		flood = append(flood, Node{ID: idAtDistance(infoHash, 0, byte(100+i)), Addr: &net.UDPAddr{IP: victim, Port: 1000 + i}})
	}

	conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		if q != "get_peers" {
			return nil
		}
		switch {
		case sameUDPAddr(to, attacker):
			return map[string]interface{}{"id": idString(attackerID), "nodes": compactNodes(flood)}
		case sameUDPAddr(to, honest):
			return map[string]interface{}{"id": idString(honestID), "nodes": compactNodes([]Node{{ID: infoHash, Addr: closest}})}
		case sameUDPAddr(to, closest):
			return map[string]interface{}{"id": idString(infoHash), "token": "tok"}
		case to.IP.Equal(victim):
			return map[string]interface{}{"id": idString(idAtDistance(infoHash, 0, byte(to.Port)))}
		}
		return nil
	})
	d.addNode(attackerID, attacker)
	d.addNode(honestID, honest)

	d.lookup(infoHash, 0, LookupOptions{})

	for _, a := range special {
		if got := conn.queriesTo(a, "get_peers"); got != 0 {
			t.Fatalf("a public responder steered %d queries to %s", got, a)
		}
	}
	if got := conn.queriesToIP(victim, "get_peers"); got > 1 {
		t.Fatalf("one responder spent %d queries on a single victim IP", got)
	}
	if got := conn.queriesTo(closest, "get_peers"); got != 1 {
		t.Fatalf("the genuinely closest node was queried %d times, want 1", got)
	}
	if got := len(conn.queried("get_peers")); got > 2*dhtLookupK {
		t.Fatalf("the lookup spent %d queries on a four-host network", got)
	}
}

// TestLookupResponderCannotFloodCandidateSet is the denial variant of the
// hijack: one responder answers with 150 referrals on distinct IPs, every one
// closer to the target than any honest node and none of them usable. They
// must not push the honest referral out of the bounded candidate set, so the
// genuinely closest node is still queried.
func TestLookupResponderCannotFloodCandidateSet(t *testing.T) {
	d, conn := newFakeDHT(t)

	var infoHash [20]byte
	copy(infoHash[:], "flooded-lookup-hash-")
	attacker := &net.UDPAddr{IP: net.ParseIP("203.0.113.66"), Port: 6881}
	attackerID := idAtDistance(infoHash, 0x80, 1)
	honest := &net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 6881}
	honestID := idAtDistance(infoHash, 0x81, 1)
	closest := &net.UDPAddr{IP: net.ParseIP("203.0.113.20"), Port: 6881}
	closestID := idAtDistance(infoHash, 0x01, 0)

	var junk []Node
	for i := 0; i < 150; i++ {
		junk = append(junk, Node{
			ID:   idAtDistance(infoHash, 0, byte(i+1)),
			Addr: &net.UDPAddr{IP: net.IPv4(198, 51, byte(100+i/100), byte(i%100+1)), Port: 6881},
		})
	}
	conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		if q != "get_peers" {
			return nil
		}
		switch {
		case sameUDPAddr(to, attacker):
			return map[string]interface{}{"id": idString(attackerID), "nodes": compactNodes(junk)}
		case sameUDPAddr(to, honest):
			return map[string]interface{}{"id": idString(honestID), "nodes": compactNodes([]Node{{ID: closestID, Addr: closest}})}
		case sameUDPAddr(to, closest):
			return map[string]interface{}{"id": idString(closestID), "token": "tok"}
		}
		// The junk answers at once but unusably, so the test runs fast.
		return map[string]interface{}{"id": "junk"}
	})
	d.addNode(attackerID, attacker)
	d.addNode(honestID, honest)

	d.lookup(infoHash, 0, LookupOptions{})

	if got := conn.queriesTo(closest, "get_peers"); got != 1 {
		t.Fatalf("one responder's referrals kept the genuinely closest node from being queried (%d queries)", got)
	}
	if got := len(conn.queried("get_peers")); got > 3+dhtLookupK {
		t.Fatalf("one responder's referrals cost %d queries, want at most one round", got)
	}
}

// TestLookupQueriesClosestAndConverges verifies the traversal queries the
// closest candidates first and stops once the K closest have answered, instead
// of crawling every referral in arrival order.
func TestLookupQueriesClosestAndConverges(t *testing.T) {
	d, conn := newFakeDHT(t)

	var infoHash [20]byte
	copy(infoHash[:], "converging-info-hash")
	seed := &net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 6881}
	seedID := idAtDistance(infoHash, 0xFF, 1)

	// Referrals arrive farthest first, so a FIFO crawl would query them in
	// the wrong order.
	var referrals []Node
	for i := 40; i >= 1; i-- {
		referrals = append(referrals, Node{
			ID:   idAtDistance(infoHash, byte(i), 0),
			Addr: &net.UDPAddr{IP: net.IPv4(198, 51, 100, byte(i)), Port: 6881},
		})
	}
	conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		if q != "get_peers" {
			return nil
		}
		if sameUDPAddr(to, seed) {
			return map[string]interface{}{"id": idString(seedID), "nodes": compactNodes(referrals)}
		}
		return map[string]interface{}{"id": idString(idAtDistance(infoHash, to.IP.To4()[3], 0))}
	})
	d.addNode(seedID, seed)

	d.lookup(infoHash, 0, LookupOptions{})

	for i := 1; i <= 40; i++ {
		addr := &net.UDPAddr{IP: net.IPv4(198, 51, 100, byte(i)), Port: 6881}
		got := conn.queriesTo(addr, "get_peers")
		if i <= dhtLookupK && got != 1 {
			t.Fatalf("one of the %d closest candidates (distance %d) was queried %d times", dhtLookupK, i, got)
		}
		if i > dhtLookupK && got != 0 {
			t.Fatalf("the lookup kept querying past convergence: distance %d queried %d times", i, got)
		}
	}
}

// TestLookupAnnouncesOnlyToClosestK verifies announce_peer goes to the K
// closest responders that returned a token, not to every responder.
func TestLookupAnnouncesOnlyToClosestK(t *testing.T) {
	d, conn := newFakeDHT(t)

	var infoHash [20]byte
	copy(infoHash[:], "announced-info-hash-")
	seed := &net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 6881}
	seedID := idAtDistance(infoHash, 0xFF, 1)
	var referrals []Node
	for i := 1; i <= 20; i++ {
		referrals = append(referrals, Node{
			ID:   idAtDistance(infoHash, byte(i), 0),
			Addr: &net.UDPAddr{IP: net.IPv4(198, 51, 100, byte(i)), Port: 6881},
		})
	}
	conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		switch {
		case q == "announce_peer":
			return map[string]interface{}{"id": idString(idAtDistance(infoHash, to.IP.To4()[3], 0))}
		case q != "get_peers":
			return nil
		case sameUDPAddr(to, seed):
			// The seed returns a token too, like all its referrals: 21 token
			// holders in all.
			return map[string]interface{}{"id": idString(seedID), "token": "tok", "nodes": compactNodes(referrals)}
		}
		return map[string]interface{}{"id": idString(idAtDistance(infoHash, to.IP.To4()[3], 0)), "token": "tok"}
	})
	d.addNode(seedID, seed)

	d.lookup(infoHash, 51413, LookupOptions{Announce: true})

	// Announces are sent from tracked goroutines once the lookup converges;
	// wait for them, then give any stray extra ones a moment to show up.
	deadline := time.After(5 * time.Second)
	for len(conn.queried("announce_peer")) < dhtLookupK {
		select {
		case <-deadline:
			t.Fatalf("announced to %d nodes, want the %d closest", len(conn.queried("announce_peer")), dhtLookupK)
		case <-time.After(2 * time.Millisecond):
		}
	}
	time.Sleep(50 * time.Millisecond)

	announced := conn.queried("announce_peer")
	if len(announced) != dhtLookupK {
		t.Fatalf("announced to %d nodes, want the %d closest", len(announced), dhtLookupK)
	}
	for _, a := range announced {
		ip := a.IP.To4()
		if ip[0] != 198 || int(ip[3]) > dhtLookupK {
			t.Fatalf("announced to %s, which is not among the %d closest", a, dhtLookupK)
		}
	}
}

// TestLookupReferralScope verifies referrals are filtered by the responder's
// own scope: a loopback or LAN responder may refer its neighbours, a public
// one may not.
func TestLookupReferralScope(t *testing.T) {
	cases := []struct {
		name      string
		responder string
		referral  string
		allowed   bool
	}{
		{"loopback refers loopback", "127.0.0.1", "127.0.0.1", true},
		{"lan refers lan", "192.168.1.20", "192.168.1.30", true},
		{"public refers lan", "203.0.113.5", "192.168.1.30", false},
		{"public refers loopback", "203.0.113.5", "127.0.0.1", false},
		{"public refers public", "203.0.113.5", "198.51.100.30", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, conn := newFakeDHT(t)
			var infoHash [20]byte
			copy(infoHash[:], "scoped-info-hash----")
			responder := &net.UDPAddr{IP: net.ParseIP(tc.responder), Port: 7001}
			referral := &net.UDPAddr{IP: net.ParseIP(tc.referral), Port: 7002}
			responderID := idAtDistance(infoHash, 0x80, 1)
			conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
				if q != "get_peers" {
					return nil
				}
				if sameUDPAddr(to, responder) {
					return map[string]interface{}{"id": idString(responderID), "nodes": compactNodes([]Node{{ID: infoHash, Addr: referral}})}
				}
				return map[string]interface{}{"id": idString(infoHash)}
			})
			d.addNode(responderID, responder)
			d.lookup(infoHash, 0, LookupOptions{})
			if got := conn.queriesTo(referral, "get_peers") == 1; got != tc.allowed {
				t.Fatalf("referral %s from %s queried = %v, want %v", referral, responder, got, tc.allowed)
			}
		})
	}
}

// TestLookupWaitsForSlowBootstrapReferral covers a lookup on an empty table:
// bootstrap referrals must answer a probe before they are admitted, so on a
// slow link the first contact can arrive after the one second the lookup used
// to wait. The lookup must still start from it rather than give up until the
// next round.
func TestLookupWaitsForSlowBootstrapReferral(t *testing.T) {
	router := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6998}
	old := DefaultBootstrapHosts
	DefaultBootstrapHosts = []string{net.JoinHostPort(router.IP.String(), strconv.Itoa(router.Port))}
	t.Cleanup(func() { DefaultBootstrapHosts = old })

	d, conn := newFakeDHT(t)
	var infoHash [20]byte
	copy(infoHash[:], "slow-bootstrap-hash-")
	// The constructor's bootstrap query goes unanswered, so the table is
	// still empty when the lookup starts and runs its own bootstrap.
	awaitQueryTo(t, conn, router, "find_node")
	d.LookupWithOptions(infoHash, 0, LookupOptions{})
	deadline := time.After(5 * time.Second)
	for conn.queriesTo(router, "find_node") < 2 {
		select {
		case <-deadline:
			t.Fatal("the lookup never bootstrapped its empty table")
		case <-time.After(2 * time.Millisecond):
		}
	}
	tid, _ := conn.lastQueryTo(router, "find_node")
	referral := &net.UDPAddr{IP: net.ParseIP("198.51.100.70"), Port: 6881}
	referralID := idAtDistance(infoHash, 0x80, 1)
	payload, err := bencode.Marshal(map[string]interface{}{
		"t": tid,
		"y": "r",
		"r": map[string]interface{}{
			"id":    idString(idInBucket(d.nodeID, 11, 1)),
			"nodes": compactNodes([]Node{{ID: referralID, Addr: referral}}),
		},
	})
	if err != nil {
		t.Fatalf("failed to encode find_node reply: %v", err)
	}
	conn.in <- fakePacket{data: payload, addr: router}

	ping := awaitQueryTo(t, conn, referral, "ping")
	time.Sleep(1500 * time.Millisecond)
	conn.injectPingReply(t, ping, referralID, referral)
	awaitQueryTo(t, conn, referral, "get_peers")
}

func compactPeer(ip net.IP, port int) string {
	var b [6]byte
	copy(b[:4], ip.To4())
	b[4], b[5] = byte(port>>8), byte(port)
	return string(b[:])
}

// drainDiscovered returns every peer published on PeerChan so far.
func drainDiscovered(d *DHT) []DiscoveredPeer {
	var out []DiscoveredPeer
	for {
		select {
		case p := <-d.PeerChan():
			out = append(out, p)
		default:
			return out
		}
	}
}

// TestLookupFiltersCapsAndDedupsValues is the reported dial flood: a public
// responder's values naming loopback, LAN, multicast, broadcast and port-0
// endpoints must never reach the dialer, one response yields at most
// dhtMaxValuesPerResponse peers, and a peer repeated by another responder is
// published once.
func TestLookupFiltersCapsAndDedupsValues(t *testing.T) {
	d, conn := newFakeDHT(t)

	var infoHash [20]byte
	copy(infoHash[:], "valued-info-hash----")
	seed := &net.UDPAddr{IP: net.ParseIP("203.0.113.5"), Port: 6881}
	seedID := idAtDistance(infoHash, 0x80, 1)
	second := &net.UDPAddr{IP: net.ParseIP("203.0.113.6"), Port: 6881}
	secondID := idAtDistance(infoHash, 0x01, 1)

	special := []string{
		compactPeer(net.ParseIP("127.0.0.1"), 22),
		compactPeer(net.ParseIP("192.168.1.1"), 80),
		compactPeer(net.ParseIP("10.0.0.1"), 445),
		compactPeer(net.ParseIP("169.254.169.254"), 80),
		compactPeer(net.ParseIP("224.0.0.1"), 80),
		compactPeer(net.ParseIP("255.255.255.255"), 80),
		compactPeer(net.ParseIP("0.0.0.0"), 1),
		compactPeer(net.ParseIP("198.51.100.200"), 0),
	}
	public := func(i int) string { return compactPeer(net.IPv4(198, 51, byte(i/200), byte(i%200)), 6881) }
	var seedValues []interface{}
	for _, v := range special {
		seedValues = append(seedValues, v)
	}
	for i := 0; i < 150; i++ {
		seedValues = append(seedValues, public(i))
	}
	var secondValues []interface{}
	for i := 0; i < 20; i++ {
		secondValues = append(secondValues, public(i)) // already published
	}
	for i := 1000; i < 1005; i++ {
		secondValues = append(secondValues, public(i))
	}

	conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		if q != "get_peers" {
			return nil
		}
		switch {
		case sameUDPAddr(to, seed):
			return map[string]interface{}{
				"id":     idString(seedID),
				"values": seedValues,
				"nodes":  compactNodes([]Node{{ID: secondID, Addr: second}}),
			}
		case sameUDPAddr(to, second):
			return map[string]interface{}{"id": idString(secondID), "values": secondValues}
		}
		return nil
	})
	d.addNode(seedID, seed)

	d.lookup(infoHash, 0, LookupOptions{})

	peers := drainDiscovered(d)
	seen := make(map[string]bool)
	for _, p := range peers {
		key := compactPeer(p.IP, int(p.Port))
		if seen[key] {
			t.Fatalf("peer %s:%d was published twice", p.IP, p.Port)
		}
		seen[key] = true
		for _, v := range special {
			if key == v {
				t.Fatalf("special-purpose endpoint %s:%d from a public node reached the dialer", p.IP, p.Port)
			}
		}
	}
	// The seed's first 100 values include the 8 special ones, so 92 public
	// peers survive from it, plus the second responder's 5 new ones.
	if want := dhtMaxValuesPerResponse - len(special) + 5; len(peers) != want {
		t.Fatalf("published %d peers, want %d", len(peers), want)
	}
}

// TestLookupLoopbackResponderMayHandOutLoopbackPeers keeps local test swarms
// working: a loopback DHT node may hand out loopback peers.
func TestLookupLoopbackResponderMayHandOutLoopbackPeers(t *testing.T) {
	d, conn := newFakeDHT(t)

	var infoHash [20]byte
	copy(infoHash[:], "local-info-hash-----")
	seed := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 7001}
	seedID := idAtDistance(infoHash, 0x80, 1)
	conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		if q == "get_peers" && sameUDPAddr(to, seed) {
			return map[string]interface{}{"id": idString(seedID), "values": []interface{}{compactPeer(net.ParseIP("127.0.0.1"), 6881)}}
		}
		return nil
	})
	d.addNode(seedID, seed)

	d.lookup(infoHash, 0, LookupOptions{})

	peers := drainDiscovered(d)
	if len(peers) != 1 || !peers[0].IP.Equal(net.ParseIP("127.0.0.1")) || peers[0].Port != 6881 {
		t.Fatalf("loopback responder's loopback peer was not published: %v", peers)
	}
}

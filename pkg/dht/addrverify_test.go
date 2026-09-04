package dht

import (
	"crypto/sha1"
	"net"
	"testing"
	"time"
)

func lookupEntry(t *testing.T, d *DHT, id [20]byte) (Node, int, bool) {
	t.Helper()
	d.mu.RLock()
	defer d.mu.RUnlock()
	idx := bucketIndex(d.nodeID, id)
	b := d.buckets[idx]
	if b == nil {
		return Node{}, -1, false
	}
	for i, n := range b.nodes {
		if n.ID == id {
			return n, i, true
		}
	}
	return Node{}, -1, false
}

// TestInboundQueryDoesNotRepointKnownID is the issue #92 reproduction: seeding a
// node at 10.0.0.1:6881 via addNode and then delivering a query claiming that ID
// from 203.0.113.9:1 through handleQuery must leave the entry, its LastSeen and
// its bucket position untouched for every query type.
func TestInboundQueryDoesNotRepointKnownID(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	id := sha1.Sum([]byte("known-node-92"))
	honest := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	attacker := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
	d.addNode(id, honest)

	before, posBefore, ok := lookupEntry(t, d, id)
	if !ok {
		t.Fatal("seeded node missing from routing table")
	}
	if !sameUDPAddr(before.Addr, honest) {
		t.Fatalf("seeded node at wrong address: %s", before.Addr)
	}

	infoHash := sha1.Sum([]byte("some-torrent"))
	queries := map[string]map[string]interface{}{
		"ping":      {"id": string(id[:])},
		"find_node": {"id": string(id[:]), "target": string(id[:])},
		"get_peers": {"id": string(id[:]), "info_hash": string(infoHash[:])},
		"announce_peer": {
			"id":        string(id[:]),
			"info_hash": string(infoHash[:]),
			"token":     d.generateToken(attacker),
			"port":      int64(6881),
		},
	}
	for q, args := range queries {
		d.handleQuery("t-"+q, q, args, attacker)
	}

	after, posAfter, ok := lookupEntry(t, d, id)
	if !ok {
		t.Fatal("seeded node vanished from routing table after spoofed queries")
	}
	if !sameUDPAddr(after.Addr, honest) {
		t.Fatalf("routing-table entry re-pointed to %s with no verification", after.Addr)
	}
	if !after.LastSeen.Equal(before.LastSeen) {
		t.Fatalf("LastSeen refreshed by unverified query: %v -> %v", before.LastSeen, after.LastSeen)
	}
	if posAfter != posBefore {
		t.Fatalf("bucket position moved %d -> %d on unverified query", posBefore, posAfter)
	}
}

// TestSameAddressQueryRefreshesEntry checks a genuine liveness signal still
// works: a query from the stored address refreshes LastSeen and moves the entry
// to the bucket tail.
func TestSameAddressQueryRefreshesEntry(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	newBucketMate := func(last byte) [20]byte {
		var id [20]byte
		copy(id[:], d.nodeID[:])
		id[0] ^= 128
		id[19] = last
		return id
	}
	idA := newBucketMate(1)
	idB := newBucketMate(2)
	addrA := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	addrB := &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 6882}
	d.addNode(idA, addrA)
	d.addNode(idB, addrB)

	stale := time.Now().Add(-1 * time.Hour)
	d.mu.Lock()
	idx := bucketIndex(d.nodeID, idA)
	for i, n := range d.buckets[idx].nodes {
		if n.ID == idA {
			d.buckets[idx].nodes[i].LastSeen = stale
		}
	}
	d.mu.Unlock()

	sameAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	d.handleQuery("t1", "ping", map[string]interface{}{"id": string(idA[:])}, sameAddr)

	after, pos, ok := lookupEntry(t, d, idA)
	if !ok {
		t.Fatal("node missing after same-address query")
	}
	if after.LastSeen.Before(time.Now().Add(-1 * time.Minute)) {
		t.Fatalf("LastSeen not refreshed by same-address query: %v", after.LastSeen)
	}
	d.mu.RLock()
	tail := len(d.buckets[idx].nodes) - 1
	d.mu.RUnlock()
	if pos != tail {
		t.Fatalf("same-address query did not move entry to tail: pos %d, tail %d", pos, tail)
	}
}

// TestVerifiedAddressChangeAdoptsDeadNode queries over loopback between two live
// DHT instances: a node whose old address is unreachable and whose new address
// answers a ping with the same ID is re-pointed after verification.
func TestVerifiedAddressChangeAdoptsDeadNode(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	moved, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start moved DHT: %v", err)
	}
	defer moved.Close()
	if moved.Port() == 0 {
		t.Fatal("moved DHT has no listen port")
	}

	dead := &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 6881}
	d.addNode(moved.nodeID, dead)

	live := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(moved.Port())}
	d.handleQuery("t1", "ping", map[string]interface{}{"id": string(moved.nodeID[:])}, live)

	deadline := time.Now().Add(10 * time.Second)
	for {
		got, _, ok := lookupEntry(t, d, moved.nodeID)
		if ok && sameUDPAddr(got.Addr, live) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("entry was not re-pointed to verified new address; still %v", got.Addr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestVerifiedAddressChangeKeepsLiveNode checks the other half of the policy:
// when the stored address still answers a ping, a spoofed new address is
// discarded and the entry keeps pointing at the old address.
func TestVerifiedAddressChangeKeepsLiveNode(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	live, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start live DHT: %v", err)
	}
	defer live.Close()

	liveAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(live.Port())}
	d.addNode(live.nodeID, liveAddr)

	spoof := &net.UDPAddr{IP: net.ParseIP("192.0.2.9"), Port: 6881}
	d.handleQuery("t1", "ping", map[string]interface{}{"id": string(live.nodeID[:])}, spoof)

	deadline := time.Now().Add(6 * time.Second)
	for {
		d.addrVerMu.Lock()
		pending := len(d.addrVerifying)
		d.addrVerMu.Unlock()
		got, _, ok := lookupEntry(t, d, live.nodeID)
		if !ok {
			t.Fatal("entry vanished during verification")
		}
		if sameUDPAddr(got.Addr, spoof) {
			t.Fatalf("entry re-pointed to unverified address %s", spoof)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("verification did not finish in time")
		}
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)
	got, _, _ := lookupEntry(t, d, live.nodeID)
	if !sameUDPAddr(got.Addr, liveAddr) {
		t.Fatalf("entry moved to %s although old address answers", got.Addr)
	}
}

// TestAddrVerificationIsBounded checks that a burst of spoofed queries for one
// ID triggers a single in-flight verification, that distinct spoofed IDs are
// capped, and that a failed candidate is not retried immediately.
func TestAddrVerificationIsBounded(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	id := sha1.Sum([]byte("bounded-node"))
	oldAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	newAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
	d.addNode(id, oldAddr)
	for i := 0; i < 20; i++ {
		d.handleQuery("t", "ping", map[string]interface{}{"id": string(id[:])}, newAddr)
	}
	d.addrVerMu.Lock()
	if n := len(d.addrVerifying); n != 1 {
		d.addrVerMu.Unlock()
		t.Fatalf("burst for one ID triggered %d verifications, want 1", n)
	}
	d.addrVerMu.Unlock()

	d2, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d2.Close()

	for k := 0; k < 100; k++ {
		var fid [20]byte
		copy(fid[:], d2.nodeID[:])
		fid[k%19] ^= 128
		fid[19] = byte(k + 1)
		d2.addNode(fid, &net.UDPAddr{IP: net.ParseIP("10.1.0.1"), Port: 6881})
		d2.handleQuery("t", "ping", map[string]interface{}{"id": string(fid[:])},
			&net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1000 + k})
	}
	d2.addrVerMu.Lock()
	n := len(d2.addrVerifying)
	d2.addrVerMu.Unlock()
	if n != maxAddrVerifications {
		t.Fatalf("flood of distinct IDs left %d pending verifications, want cap %d", n, maxAddrVerifications)
	}

	d3, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d3.Close()

	fid := sha1.Sum([]byte("cooldown-node"))
	d3.addNode(fid, &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881})
	bad := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 2}
	d3.handleQuery("t", "ping", map[string]interface{}{"id": string(fid[:])}, bad)

	deadline := time.Now().Add(12 * time.Second)
	for {
		d3.addrVerMu.Lock()
		pending := len(d3.addrVerifying)
		_, cooled := d3.addrVerFailNext[addrVerKey{id: fid, addr: bad.String()}]
		d3.addrVerMu.Unlock()
		if pending == 0 && cooled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("failed candidate never cooled down (pending=%d cooled=%v)", pending, cooled)
		}
		time.Sleep(50 * time.Millisecond)
	}

	d3.handleQuery("t2", "ping", map[string]interface{}{"id": string(fid[:])}, bad)
	d3.addrVerMu.Lock()
	stillPending := len(d3.addrVerifying)
	d3.addrVerMu.Unlock()
	if stillPending != 0 {
		t.Fatalf("failed candidate retried immediately: %d pending", stillPending)
	}
}

// TestAnnouncePeerBadTokenAddsNothing checks handleQuery ordering: an
// announce_peer with an invalid token never touches the routing table, while a
// valid one admits the sender.
func TestAnnouncePeerBadTokenAddsNothing(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	infoHash := sha1.Sum([]byte("some-torrent"))
	sender := sha1.Sum([]byte("bad-token-sender"))
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5000}

	d.handleQuery("bad", "announce_peer", map[string]interface{}{
		"id":        string(sender[:]),
		"info_hash": string(infoHash[:]),
		"token":     "wrong-token",
		"port":      int64(6881),
	}, addr)
	if c := d.NodesCount(); c != 0 {
		t.Fatalf("announce_peer with bad token touched routing table: %d nodes", c)
	}

	d.handleQuery("good", "announce_peer", map[string]interface{}{
		"id":        string(sender[:]),
		"info_hash": string(infoHash[:]),
		"token":     d.generateToken(addr),
		"port":      int64(6881),
	}, addr)
	if _, _, ok := lookupEntry(t, d, sender); !ok {
		t.Fatal("announce_peer with valid token did not admit sender")
	}
}

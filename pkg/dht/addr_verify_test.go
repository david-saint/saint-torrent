package dht

import (
	"crypto/sha1"
	"net"
	"testing"
	"time"
)

func bucket0IDs(nodeID [20]byte, n int) [][20]byte {
	ids := make([][20]byte, n)
	for i := 0; i < n; i++ {
		var id [20]byte
		copy(id[:], nodeID[:])
		id[0] ^= 128
		id[19] ^= byte(i + 1)
		ids[i] = id
	}
	return ids
}

func nodeInTable(d *DHT, id [20]byte) (Node, int, bool) {
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

func pendingVerifyState(d *DHT, id [20]byte) (inFlight bool, failUntil time.Time, ok bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	v, ok := d.pendingAddrVerify[id]
	if !ok {
		return false, time.Time{}, false
	}
	return v.inFlight, v.failUntil, true
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for condition")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func startTestDHT(t *testing.T) *DHT {
	t.Helper()
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func seedVictimMidBucket(t *testing.T, d *DHT) (victim [20]byte, honest *net.UDPAddr, pos int, seen time.Time) {
	t.Helper()
	ids := bucket0IDs(d.nodeID, 3)
	d.addNode(ids[0], &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6882})
	honest = &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	d.addNode(ids[1], honest)
	d.addNode(ids[2], &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6883})
	victim = ids[1]
	n, pos, ok := nodeInTable(d, victim)
	if !ok {
		t.Fatal("victim not inserted")
	}
	if pos != 1 {
		t.Fatalf("expected victim at bucket index 1, got %d", pos)
	}
	if !sameUDPAddr(n.Addr, honest) {
		t.Fatalf("expected honest addr %s, got %s", honest, n.Addr)
	}
	return victim, honest, pos, n.LastSeen
}

func TestHandleQueryDoesNotRepointUnverifiedAddress(t *testing.T) {
	target := string(make([]byte, 20))
	infoHash := string(make([]byte, 20))
	attacker := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}

	tests := []struct {
		name string
		q    string
		args func(id [20]byte, d *DHT) map[string]interface{}
	}{
		{
			name: "ping",
			q:    "ping",
			args: func(id [20]byte, _ *DHT) map[string]interface{} {
				return map[string]interface{}{"id": string(id[:])}
			},
		},
		{
			name: "find_node",
			q:    "find_node",
			args: func(id [20]byte, _ *DHT) map[string]interface{} {
				return map[string]interface{}{"id": string(id[:]), "target": target}
			},
		},
		{
			name: "get_peers",
			q:    "get_peers",
			args: func(id [20]byte, _ *DHT) map[string]interface{} {
				return map[string]interface{}{"id": string(id[:]), "info_hash": infoHash}
			},
		},
		{
			name: "announce_peer",
			q:    "announce_peer",
			args: func(id [20]byte, d *DHT) map[string]interface{} {
				return map[string]interface{}{
					"id":        string(id[:]),
					"info_hash": infoHash,
					"token":     d.generateToken(attacker),
					"port":      int64(6881),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := startTestDHT(t)
			victim, honest, pos, seen := seedVictimMidBucket(t, d)
			d.handleQuery("tx", tt.q, tt.args(victim, d), attacker)

			n, pos2, ok := nodeInTable(d, victim)
			if !ok {
				t.Fatal("victim disappeared from routing table")
			}
			if pos2 != pos {
				t.Errorf("bucket position changed: %d -> %d", pos, pos2)
			}
			if n.LastSeen != seen {
				t.Errorf("LastSeen was refreshed without a same-address query")
			}
			if !sameUDPAddr(n.Addr, honest) {
				t.Errorf("re-pointed to %s without verification; want %s", n.Addr, honest)
			}
		})
	}
}

func TestHandleQuerySameAddressRefreshes(t *testing.T) {
	d := startTestDHT(t)
	ids := bucket0IDs(d.nodeID, 2)
	addr0 := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	addr1 := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6882}
	d.addNode(ids[0], addr0)
	d.addNode(ids[1], addr1)

	oldSeen := time.Now().Add(-time.Hour)
	d.mu.Lock()
	idx := bucketIndex(d.nodeID, ids[0])
	b := d.buckets[idx]
	for i, n := range b.nodes {
		if n.ID == ids[0] {
			b.nodes[i].LastSeen = oldSeen
		}
	}
	d.mu.Unlock()

	_, pos, ok := nodeInTable(d, ids[0])
	if !ok || pos != 0 {
		t.Fatalf("setup: expected ids[0] at head, pos=%d ok=%v", pos, ok)
	}

	d.handleQuery("tx", "ping", map[string]interface{}{"id": string(ids[0][:])}, addr0)

	n, pos2, ok := nodeInTable(d, ids[0])
	if !ok {
		t.Fatal("node missing after same-address query")
	}
	if pos2 != 1 {
		t.Errorf("expected node moved to bucket tail (index 1), got %d", pos2)
	}
	if !n.LastSeen.After(oldSeen) {
		t.Errorf("LastSeen was not refreshed: %v", n.LastSeen)
	}
	if !sameUDPAddr(n.Addr, addr0) {
		t.Errorf("address changed on same-address refresh: %s", n.Addr)
	}
}

func TestHandleQueryInsertsNewNodeID(t *testing.T) {
	d := startTestDHT(t)
	id := sha1.Sum([]byte("brand-new-node"))
	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 6881}
	d.handleQuery("tx", "ping", map[string]interface{}{"id": string(id[:])}, addr)
	n, _, ok := nodeInTable(d, id)
	if !ok {
		t.Fatal("new node ID from well-formed ping was not inserted")
	}
	if !sameUDPAddr(n.Addr, addr) {
		t.Errorf("inserted address %s, want %s", n.Addr, addr)
	}
}

func TestHandleQueryMalformedDoesNotAddSender(t *testing.T) {
	d := startTestDHT(t)
	id := sha1.Sum([]byte("malformed-sender"))
	addr := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
	args := map[string]interface{}{"id": string(id[:])}

	d.handleQuery("tx", "find_node", args, addr)
	d.handleQuery("tx", "get_peers", args, addr)
	d.handleQuery("tx", "announce_peer", args, addr)
	d.handleQuery("tx", "not_a_query", args, addr)

	if c := d.NodesCount(); c != 0 {
		t.Fatalf("malformed/unknown queries added %d routing-table nodes", c)
	}
}

func TestAnnouncePeerBadTokenDoesNotAddSender(t *testing.T) {
	d := startTestDHT(t)
	id := sha1.Sum([]byte("announce-attacker"))
	addr := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
	infoHash := sha1.Sum([]byte("torrent"))
	d.handleQuery("tx", "announce_peer", map[string]interface{}{
		"id":        string(id[:]),
		"info_hash": string(infoHash[:]),
		"token":     "bogus",
		"port":      int64(6881),
	}, addr)
	if c := d.NodesCount(); c != 0 {
		t.Fatalf("announce_peer with invalid token added %d nodes", c)
	}
	if _, _, ok := nodeInTable(d, id); ok {
		t.Fatal("sender was admitted to the routing table despite a bad token")
	}
}

func TestAnnouncePeerBadTokenDoesNotTouchExistingEntry(t *testing.T) {
	d := startTestDHT(t)
	victim, honest, pos, seen := seedVictimMidBucket(t, d)
	attacker := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
	infoHash := sha1.Sum([]byte("torrent"))
	d.handleQuery("tx", "announce_peer", map[string]interface{}{
		"id":        string(victim[:]),
		"info_hash": string(infoHash[:]),
		"token":     "bogus",
		"port":      int64(6881),
	}, attacker)

	n, pos2, ok := nodeInTable(d, victim)
	if !ok {
		t.Fatal("existing node removed by bad-token announce_peer")
	}
	if pos2 != pos || n.LastSeen != seen || !sameUDPAddr(n.Addr, honest) {
		t.Errorf("bad-token announce_peer mutated routing entry: pos %d->%d addr %s lastSeen changed %v", pos, pos2, n.Addr, n.LastSeen != seen)
	}
	if inFlight, _, pending := pendingVerifyState(d, victim); pending && inFlight {
		t.Fatal("bad-token announce_peer started address verification")
	}
}

func TestAddrChangeAdoptedWhenOldUnreachable(t *testing.T) {
	victim := startTestDHT(t)
	live, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start live DHT: %v", err)
	}
	defer live.Close()

	oldAddr := &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 6881}
	victim.addNode(live.nodeID, oldAddr)
	newAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(live.Port())}
	victim.handleQuery("tx", "ping", map[string]interface{}{"id": string(live.nodeID[:])}, newAddr)

	waitUntil(t, pingTimeout*2+5*time.Second, func() bool {
		n, _, ok := nodeInTable(victim, live.nodeID)
		return ok && sameUDPAddr(n.Addr, newAddr)
	})
}

func TestAddrChangeNotAdoptedWhenOldAnswers(t *testing.T) {
	victim := startTestDHT(t)
	honest, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start honest DHT: %v", err)
	}
	defer honest.Close()

	honestAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(honest.Port())}
	victim.addNode(honest.nodeID, honestAddr)
	attacker := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
	victim.handleQuery("tx", "ping", map[string]interface{}{"id": string(honest.nodeID[:])}, attacker)

	waitUntil(t, pingTimeout+5*time.Second, func() bool {
		inFlight, _, ok := pendingVerifyState(victim, honest.nodeID)
		return !ok || !inFlight
	})

	n, _, ok := nodeInTable(victim, honest.nodeID)
	if !ok {
		t.Fatal("honest node missing after verification")
	}
	if !sameUDPAddr(n.Addr, honestAddr) {
		t.Errorf("re-pointed live node to %s", n.Addr)
	}
}

func TestAddrChangeRejectedWhenNewIDMismatches(t *testing.T) {
	victim := startTestDHT(t)
	other, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start other DHT: %v", err)
	}
	defer other.Close()

	claimed := sha1.Sum([]byte("claimed-id"))
	oldAddr := &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 6881}
	victim.addNode(claimed, oldAddr)
	newAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(other.Port())}
	victim.handleQuery("tx", "ping", map[string]interface{}{"id": string(claimed[:])}, newAddr)

	waitUntil(t, pingTimeout*2+5*time.Second, func() bool {
		inFlight, _, ok := pendingVerifyState(victim, claimed)
		return !ok || !inFlight
	})

	n, _, ok := nodeInTable(victim, claimed)
	if !ok {
		t.Fatal("claimed node missing")
	}
	if !sameUDPAddr(n.Addr, oldAddr) {
		t.Errorf("adopted address %s that replied with a different node ID", n.Addr)
	}
}

func TestAddrChangeVerificationBounded(t *testing.T) {
	d := startTestDHT(t)
	ids := bucket0IDs(d.nodeID, 1)
	honest := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	d.addNode(ids[0], honest)

	for i := 0; i < 50; i++ {
		spoof := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1 + i}
		d.handleQuery("tx", "ping", map[string]interface{}{"id": string(ids[0][:])}, spoof)
	}

	d.mu.RLock()
	inFlight := 0
	for id, v := range d.pendingAddrVerify {
		if v.inFlight {
			inFlight++
		}
		if id != ids[0] {
			t.Errorf("unexpected pending verification for another id")
		}
	}
	pending := len(d.pendingAddrVerify)
	d.mu.RUnlock()
	if pending != 1 {
		t.Errorf("expected 1 pending candidate for the spoofed id, got %d", pending)
	}
	if inFlight > 1 {
		t.Errorf("expected at most 1 in-flight verification, got %d", inFlight)
	}

	var inserted [][20]byte
	for b := 1; b <= maxPendingAddrVerify+8; b++ {
		var id [20]byte
		copy(id[:], d.nodeID[:])
		byteIdx := b / 8
		bit := 7 - uint(b%8)
		id[byteIdx] ^= 1 << bit
		id[19] ^= byte(b)
		d.addNode(id, &net.UDPAddr{IP: net.ParseIP("10.0.1.1"), Port: 2000 + b})
		if _, _, ok := nodeInTable(d, id); !ok {
			t.Fatalf("failed to insert node for bucket bit %d", b)
		}
		inserted = append(inserted, id)
	}

	for i, id := range inserted {
		spoof := &net.UDPAddr{IP: net.ParseIP("203.0.113.8"), Port: 1 + i}
		d.handleQuery("tx", "ping", map[string]interface{}{"id": string(id[:])}, spoof)
	}

	d.mu.RLock()
	total := len(d.pendingAddrVerify)
	perID := make(map[[20]byte]int)
	for id, v := range d.pendingAddrVerify {
		if v.inFlight {
			perID[id]++
		}
	}
	d.mu.RUnlock()
	if total > maxPendingAddrVerify {
		t.Errorf("pending address verifications grew to %d, cap is %d", total, maxPendingAddrVerify)
	}
	for id, n := range perID {
		if n > 1 {
			t.Errorf("node %x has %d in-flight verifications", id, n)
		}
	}
}

func TestFailedAddrChangeNotRetriedImmediately(t *testing.T) {
	d := startTestDHT(t)
	id := sha1.Sum([]byte("cooldown-node"))
	oldAddr := &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 6881}
	d.addNode(id, oldAddr)
	candidate := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
	d.handleQuery("tx", "ping", map[string]interface{}{"id": string(id[:])}, candidate)

	waitUntil(t, pingTimeout*2+5*time.Second, func() bool {
		inFlight, failUntil, ok := pendingVerifyState(d, id)
		return ok && !inFlight && failUntil.After(time.Now())
	})

	d.handleQuery("tx", "ping", map[string]interface{}{"id": string(id[:])}, candidate)
	inFlight, failUntil, ok := pendingVerifyState(d, id)
	if !ok {
		t.Fatal("expected failed candidate to remain in cooldown")
	}
	if inFlight {
		t.Fatal("failed candidate was retried immediately")
	}
	if !failUntil.After(time.Now()) {
		t.Fatal("cooldown expired unexpectedly")
	}

	n, _, found := nodeInTable(d, id)
	if !found || !sameUDPAddr(n.Addr, oldAddr) {
		t.Errorf("failed verification mutated stored address to %v", n.Addr)
	}
}

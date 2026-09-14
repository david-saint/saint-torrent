package dht

import (
	"net"
	"testing"
	"time"
)

// backdateNode rewinds a stored node's LastSeen so a refresh is observable
// without waiting on the clock.
func backdateNode(t *testing.T, d *DHT, bucket int, id [20]byte, when time.Time) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	b := d.buckets[bucket]
	if b == nil {
		t.Fatalf("bucket %d is empty", bucket)
	}
	for i := range b.nodes {
		if b.nodes[i].ID == id {
			b.nodes[i].LastSeen = when
			return
		}
	}
	t.Fatalf("node %x not found in bucket %d", id, bucket)
}

func pendingAddrChangeCount(d *DHT) int {
	d.addrMu.Lock()
	defer d.addrMu.Unlock()
	return len(d.addrChanges)
}

func addrCooldownCount(d *DHT) int {
	d.addrMu.Lock()
	defer d.addrMu.Unlock()
	return len(d.addrCooldowns)
}

// expireIDCooldown rewinds the short per-ID cooldown so the window elapsing can
// be observed without waiting on the clock. The long per-candidate cooldowns are
// left untouched.
func expireIDCooldown(t *testing.T, d *DHT, id [20]byte) {
	t.Helper()
	d.addrMu.Lock()
	defer d.addrMu.Unlock()
	key := addrChangeKey{id: id}
	if _, ok := d.addrCooldowns[key]; !ok {
		t.Fatalf("no short cooldown recorded for %x", id)
	}
	d.addrCooldowns[key] = time.Now().Add(-time.Second)
}

// TestHandleQueryDoesNotRepointNodeOnUnverifiedAddressChange is the reported
// reproduction: a node seeded at 10.0.0.1:6881 must survive an inbound query
// that claims its ID from 203.0.113.9:1, for every query type.
func TestHandleQueryDoesNotRepointNodeOnUnverifiedAddressChange(t *testing.T) {
	const bucket = 5
	honestAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	attackerAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
	past := time.Now().Add(-time.Hour)

	cases := []struct {
		name  string
		query string
		args  func(d *DHT, honest [20]byte) map[string]interface{}
	}{
		{
			name:  "ping",
			query: "ping",
			args: func(d *DHT, honest [20]byte) map[string]interface{} {
				return map[string]interface{}{"id": string(honest[:])}
			},
		},
		{
			name:  "find_node",
			query: "find_node",
			args: func(d *DHT, honest [20]byte) map[string]interface{} {
				target := d.nodeID
				return map[string]interface{}{"id": string(honest[:]), "target": string(target[:])}
			},
		},
		{
			name:  "get_peers",
			query: "get_peers",
			args: func(d *DHT, honest [20]byte) map[string]interface{} {
				var infoHash [20]byte
				copy(infoHash[:], "info-hash-for-test--")
				return map[string]interface{}{"id": string(honest[:]), "info_hash": string(infoHash[:])}
			},
		},
		{
			name:  "announce_peer",
			query: "announce_peer",
			args: func(d *DHT, honest [20]byte) map[string]interface{} {
				var infoHash [20]byte
				copy(infoHash[:], "info-hash-for-test--")
				return map[string]interface{}{
					"id":        string(honest[:]),
					"info_hash": string(infoHash[:]),
					"token":     d.generateToken(attackerAddr),
					"port":      int64(51413),
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := newFakeDHT(t)

			honest := idInBucket(d.nodeID, bucket, 1)
			neighbour := idInBucket(d.nodeID, bucket, 2)
			d.addNode(honest, honestAddr)
			d.addNode(neighbour, &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 6881})
			backdateNode(t, d, bucket, honest, past)

			d.handleQuery("tx", tc.query, tc.args(d, honest), attackerAddr)

			nodes := bucketNodes(d, bucket)
			if len(nodes) != 2 {
				t.Fatalf("expected 2 nodes in bucket %d, got %d", bucket, len(nodes))
			}
			if nodes[0].ID != honest {
				t.Fatalf("bucket position changed: head is %x, want %x", nodes[0].ID, honest)
			}
			if !sameUDPAddr(nodes[0].Addr, honestAddr) {
				t.Fatalf("entry was re-pointed to %s, want %s", nodes[0].Addr, honestAddr)
			}
			if !nodes[0].LastSeen.Equal(past) {
				t.Fatalf("LastSeen was refreshed by an unverified sighting: %v", nodes[0].LastSeen)
			}
		})
	}
}

// TestHandleQueryFromSameAddressRefreshesEntry verifies a genuine liveness
// signal still refreshes LastSeen and promotes the entry to the bucket tail.
func TestHandleQueryFromSameAddressRefreshesEntry(t *testing.T) {
	const bucket = 6
	d, _ := newFakeDHT(t)

	honestAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	honest := idInBucket(d.nodeID, bucket, 1)
	neighbour := idInBucket(d.nodeID, bucket, 2)
	d.addNode(honest, honestAddr)
	d.addNode(neighbour, &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 6881})

	past := time.Now().Add(-time.Hour)
	backdateNode(t, d, bucket, honest, past)

	d.handleQuery("tx", "ping", map[string]interface{}{"id": string(honest[:])}, honestAddr)

	nodes := bucketNodes(d, bucket)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes in bucket %d, got %d", bucket, len(nodes))
	}
	if nodes[1].ID != honest {
		t.Fatalf("same-address query did not move the entry to the tail: tail is %x", nodes[1].ID)
	}
	if !sameUDPAddr(nodes[1].Addr, honestAddr) {
		t.Fatalf("address changed unexpectedly: %s", nodes[1].Addr)
	}
	if !nodes[1].LastSeen.After(past) {
		t.Fatalf("same-address query did not refresh LastSeen")
	}
}

// TestHandleQueryNewNodeStillFollowsBucketRules verifies an unseen node ID from
// an inbound query is still appended while the bucket has room.
func TestHandleQueryNewNodeStillFollowsBucketRules(t *testing.T) {
	const bucket = 8
	d, _ := newFakeDHT(t)

	for i := 0; i < 8; i++ {
		id := idInBucket(d.nodeID, bucket, uint16(i+1))
		addr := &net.UDPAddr{IP: net.ParseIP("10.0.1.1"), Port: 7000 + i}
		d.handleQuery("tx", "ping", map[string]interface{}{"id": string(id[:])}, addr)
	}

	nodes := bucketNodes(d, bucket)
	if len(nodes) != 8 {
		t.Fatalf("expected 8 nodes appended to bucket %d, got %d", bucket, len(nodes))
	}
	for i, n := range nodes {
		want := idInBucket(d.nodeID, bucket, uint16(i+1))
		if n.ID != want {
			t.Fatalf("node %d out of insertion order: got %x, want %x", i, n.ID, want)
		}
	}

	// A ninth distinct ID must not grow the bucket past k=8.
	extra := idInBucket(d.nodeID, bucket, 99)
	d.handleQuery("tx", "ping", map[string]interface{}{"id": string(extra[:])}, &net.UDPAddr{IP: net.ParseIP("10.0.1.1"), Port: 7100})
	if got := len(bucketNodes(d, bucket)); got != 8 {
		t.Fatalf("k-bucket limit violated: %d nodes", got)
	}
}

// TestAddressChangeVerificationIsBounded verifies a burst of spoofed queries for
// one ID triggers a single verification ping, and that a flood of distinct IDs
// cannot grow the pending-candidate set past its cap.
func TestAddressChangeVerificationIsBounded(t *testing.T) {
	t.Run("one ping per node id", func(t *testing.T) {
		const bucket = 7
		d, conn := newFakeDHT(t)

		honestAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
		honest := idInBucket(d.nodeID, bucket, 1)
		d.addNode(honest, honestAddr)

		attackerBase := net.ParseIP("203.0.113.9")
		for i := 0; i < 20; i++ {
			addr := &net.UDPAddr{IP: attackerBase, Port: 1 + i}
			d.handleQuery("tx", "ping", map[string]interface{}{"id": string(honest[:])}, addr)
		}

		deadline := time.After(3 * time.Second)
		for conn.queriesTo(honestAddr, "ping") == 0 {
			select {
			case <-deadline:
				t.Fatal("verification ping to the stored address was never sent")
			case <-time.After(10 * time.Millisecond):
			}
		}
		if got := conn.queriesTo(honestAddr, "ping"); got != 1 {
			t.Fatalf("expected exactly 1 verification ping for 20 spoofed queries, got %d", got)
		}
		if got := pendingAddrChangeCount(d); got != 1 {
			t.Fatalf("expected 1 pending candidate, got %d", got)
		}
	})

	t.Run("pending candidates are capped", func(t *testing.T) {
		d, _ := newFakeDHT(t)

		const flood = maxPendingAddrChanges * 2
		for i := 0; i < flood; i++ {
			id := idInBucket(d.nodeID, i, 1)
			d.addNode(id, &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881})
		}
		for i := 0; i < flood; i++ {
			id := idInBucket(d.nodeID, i, 1)
			addr := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1 + i}
			d.handleQuery("tx", "ping", map[string]interface{}{"id": string(id[:])}, addr)
		}

		if got := pendingAddrChangeCount(d); got != maxPendingAddrChanges {
			t.Fatalf("pending candidate set holds %d verifications, want exactly the cap %d", got, maxPendingAddrChanges)
		}
	})

	t.Run("failed candidate is not retried immediately", func(t *testing.T) {
		const bucket = 9
		d, _ := newFakeDHT(t)

		honestAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
		honest := idInBucket(d.nodeID, bucket, 1)
		d.addNode(honest, honestAddr)
		candidate := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}

		if !d.beginAddressVerification(honest, candidate) {
			t.Fatal("first verification should have been admitted")
		}
		d.endAddressVerification(honest, candidate, addrChangeFailed)
		if d.beginAddressVerification(honest, candidate) {
			t.Fatal("a candidate was retried during its cooldown")
		}
	})

	// Completed attempts sit in the cooldown map, not the in-flight set, so a
	// full round of recent checks cannot starve an unrelated genuine move.
	t.Run("cooling attempts do not starve a fresh id", func(t *testing.T) {
		d, _ := newFakeDHT(t)

		for i := 0; i < maxPendingAddrChanges; i++ {
			id := idInBucket(d.nodeID, i, 1)
			candidate := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1 + i}
			if !d.beginAddressVerification(id, candidate) {
				t.Fatalf("verification %d should have been admitted", i)
			}
			d.endAddressVerification(id, candidate, addrChangeFailed)
		}
		if got := pendingAddrChangeCount(d); got != 0 {
			t.Fatalf("completed verifications still hold %d in-flight slots", got)
		}

		fresh := idInBucket(d.nodeID, 130, 1)
		freshCandidate := &net.UDPAddr{IP: net.ParseIP("198.51.100.7"), Port: 4242}
		if !d.beginAddressVerification(fresh, freshCandidate) {
			t.Fatalf("%d cooling attempts blocked an unrelated address change", maxPendingAddrChanges)
		}
	})

	// A verified adoption leaves no cooldown, so a node that legitimately moves
	// again straight away is still followed.
	t.Run("adoption leaves no cooldown", func(t *testing.T) {
		d, _ := newFakeDHT(t)

		id := idInBucket(d.nodeID, 14, 1)
		candidate := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
		if !d.beginAddressVerification(id, candidate) {
			t.Fatal("first verification should have been admitted")
		}
		d.endAddressVerification(id, candidate, addrChangeAdopted)

		if got := addrCooldownCount(d); got != 0 {
			t.Fatalf("a verified adoption left %d cooldown entries behind", got)
		}
		next := &net.UDPAddr{IP: net.ParseIP("198.51.100.7"), Port: 2}
		if !d.beginAddressVerification(id, next) {
			t.Fatal("a second genuine move was blocked right after a verified adoption")
		}
	})

	// The cooldown is two-tier: a short per-ID window bounds probing, while the
	// exact spoofed candidate stays barred for the long window.
	t.Run("short id window releases a new candidate but not the old one", func(t *testing.T) {
		d, _ := newFakeDHT(t)

		id := idInBucket(d.nodeID, 15, 1)
		spoofed := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
		genuine := &net.UDPAddr{IP: net.ParseIP("198.51.100.7"), Port: 2}

		if !d.beginAddressVerification(id, spoofed) {
			t.Fatal("first verification should have been admitted")
		}
		d.endAddressVerification(id, spoofed, addrChangeFailed)

		if d.beginAddressVerification(id, genuine) {
			t.Fatal("the short per-ID window did not bound repeat probing")
		}

		expireIDCooldown(t, d, id)
		if !d.beginAddressVerification(id, genuine) {
			t.Fatal("a genuine move from a new address stayed blocked past the short window")
		}
		d.endAddressVerification(id, genuine, addrChangeFailed)

		expireIDCooldown(t, d, id)
		if d.beginAddressVerification(id, spoofed) {
			t.Fatal("the same spoofed candidate was retried inside its long cooldown")
		}
	})
}

// unusedLoopbackAddr returns a loopback UDP address with nothing bound to it.
func unusedLoopbackAddr(t *testing.T) *net.UDPAddr {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("failed to reserve a loopback port: %v", err)
	}
	addr := conn.LocalAddr().(*net.UDPAddr)
	unused := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: addr.Port}
	if err := conn.Close(); err != nil {
		t.Fatalf("failed to release loopback port: %v", err)
	}
	return unused
}

func storedAddrFor(d *DHT, id [20]byte) *net.UDPAddr {
	d.mu.RLock()
	defer d.mu.RUnlock()
	b := d.buckets[bucketIndex(d.nodeID, id)]
	if b == nil {
		return nil
	}
	for _, n := range b.nodes {
		if n.ID == id {
			return n.Addr
		}
	}
	return nil
}

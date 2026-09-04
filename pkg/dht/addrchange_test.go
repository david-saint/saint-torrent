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

		if got := pendingAddrChangeCount(d); got > maxPendingAddrChanges {
			t.Fatalf("pending candidate set grew to %d, cap is %d", got, maxPendingAddrChanges)
		}
	})

	t.Run("failed candidate is not retried immediately", func(t *testing.T) {
		const bucket = 9
		d, _ := newFakeDHT(t)

		honestAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
		honest := idInBucket(d.nodeID, bucket, 1)
		d.addNode(honest, honestAddr)

		if !d.beginAddressVerification(honest) {
			t.Fatal("first verification should have been admitted")
		}
		d.endAddressVerification(honest)
		if d.beginAddressVerification(honest) {
			t.Fatal("a candidate was retried during its cooldown")
		}
	})
}

// TestAddressChangeAdoptedOnlyAfterVerification exercises the full verification
// path against live DHT instances over loopback: a stored address that has gone
// silent is replaced by a candidate that answers a ping with the same node ID,
// while a stored address that still answers keeps its slot.
func TestAddressChangeAdoptedOnlyAfterVerification(t *testing.T) {
	t.Run("adopted when old address is silent", func(t *testing.T) {
		victim, err := NewDHT(t.TempDir(), 0)
		if err != nil {
			t.Fatalf("failed to start victim DHT: %v", err)
		}
		defer victim.Close()

		mover, err := NewDHT(t.TempDir(), 0)
		if err != nil {
			t.Fatalf("failed to start mover DHT: %v", err)
		}
		defer mover.Close()

		oldAddr := unusedLoopbackAddr(t)
		newAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(mover.Port())}
		if newAddr.Port == 0 {
			t.Fatal("mover DHT has no listen port")
		}

		victim.addNode(mover.nodeID, oldAddr)
		victim.handleQuery("tx", "ping", map[string]interface{}{"id": string(mover.nodeID[:])}, newAddr)

		deadline := time.After(15 * time.Second)
		for {
			addr := storedAddrFor(victim, mover.nodeID)
			if addr == nil {
				t.Fatal("node vanished from the routing table")
			}
			if sameUDPAddr(addr, newAddr) {
				return
			}
			select {
			case <-deadline:
				t.Fatalf("address change was never adopted; entry still at %s", addr)
			case <-time.After(20 * time.Millisecond):
			}
		}
	})

	t.Run("rejected when old address still answers", func(t *testing.T) {
		victim, err := NewDHT(t.TempDir(), 0)
		if err != nil {
			t.Fatalf("failed to start victim DHT: %v", err)
		}
		defer victim.Close()

		incumbent, err := NewDHT(t.TempDir(), 0)
		if err != nil {
			t.Fatalf("failed to start incumbent DHT: %v", err)
		}
		defer incumbent.Close()

		oldAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(incumbent.Port())}
		if oldAddr.Port == 0 {
			t.Fatal("incumbent DHT has no listen port")
		}
		attackerAddr := unusedLoopbackAddr(t)

		victim.addNode(incumbent.nodeID, oldAddr)
		bucket := bucketIndex(victim.nodeID, incumbent.nodeID)
		past := time.Now().Add(-time.Hour)
		backdateNode(t, victim, bucket, incumbent.nodeID, past)

		victim.handleQuery("tx", "ping", map[string]interface{}{"id": string(incumbent.nodeID[:])}, attackerAddr)

		// The incumbent's real ping reply is what ends the verification, so wait
		// for the refreshed LastSeen rather than a fixed delay.
		deadline := time.After(15 * time.Second)
		for {
			nodes := bucketNodes(victim, bucket)
			if len(nodes) != 1 {
				t.Fatalf("expected 1 node in bucket %d, got %d", bucket, len(nodes))
			}
			if !sameUDPAddr(nodes[0].Addr, oldAddr) {
				t.Fatalf("entry was re-pointed to %s despite the old address answering", nodes[0].Addr)
			}
			if nodes[0].LastSeen.After(past) {
				return
			}
			select {
			case <-deadline:
				t.Fatal("incumbent was never re-confirmed by its ping reply")
			case <-time.After(20 * time.Millisecond):
			}
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

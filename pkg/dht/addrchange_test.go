package dht

import (
	"crypto/sha1"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
)

func TestHandleQueryDoesNotRepointUnverifiedAddress(t *testing.T) {
	honest := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	attacker := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
	infoHash := sha1.Sum([]byte("torrent"))

	queries := []struct {
		name string
		q    string
	}{
		{name: "ping", q: "ping"},
		{name: "find_node", q: "find_node"},
		{name: "get_peers", q: "get_peers"},
		{name: "announce_peer", q: "announce_peer"},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			d, err := NewDHT(t.TempDir(), 0)
			if err != nil {
				t.Fatalf("failed to start DHT: %v", err)
			}
			defer d.Close()

			id := bucketPeerID(d.nodeID, 1)
			fillerA := bucketPeerID(d.nodeID, 2)
			fillerB := bucketPeerID(d.nodeID, 3)
			d.addNode(fillerA, &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 6881})
			d.addNode(id, honest)
			d.addNode(fillerB, &net.UDPAddr{IP: net.ParseIP("10.0.0.3"), Port: 6881})

			stale := time.Now().Add(-time.Hour)
			setNodeLastSeen(t, d, id, stale)
			before, posBefore, ok := routingEntry(d, id)
			if !ok {
				t.Fatal("expected seeded node in routing table")
			}
			if posBefore != 1 {
				t.Fatalf("expected seeded node at bucket index 1, got %d", posBefore)
			}

			args := map[string]interface{}{"id": string(id[:])}
			switch tc.q {
			case "find_node":
				args["target"] = string(infoHash[:])
			case "get_peers":
				args["info_hash"] = string(infoHash[:])
			case "announce_peer":
				args["info_hash"] = string(infoHash[:])
				args["token"] = d.generateToken(attacker)
				args["port"] = int64(6881)
			}
			d.handleQuery("tx", tc.q, args, attacker)

			after, posAfter, ok := routingEntry(d, id)
			if !ok {
				t.Fatal("node disappeared from routing table")
			}
			if !sameUDPAddr(after.Addr, honest) {
				t.Fatalf("entry re-pointed to %s without verification", after.Addr)
			}
			if posAfter != posBefore {
				t.Fatalf("bucket position changed from %d to %d", posBefore, posAfter)
			}
			if !after.LastSeen.Equal(stale) {
				t.Fatalf("LastSeen refreshed from %v to %v", stale, after.LastSeen)
			}
			if !sameUDPAddr(before.Addr, after.Addr) {
				t.Fatalf("address changed from %s to %s", before.Addr, after.Addr)
			}
		})
	}
}

func TestHandleQuerySameAddressRefreshesEntry(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	id := bucketPeerID(d.nodeID, 1)
	fillerA := bucketPeerID(d.nodeID, 2)
	fillerB := bucketPeerID(d.nodeID, 3)
	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	d.addNode(fillerA, &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 6881})
	d.addNode(id, addr)
	d.addNode(fillerB, &net.UDPAddr{IP: net.ParseIP("10.0.0.3"), Port: 6881})

	stale := time.Now().Add(-time.Hour)
	setNodeLastSeen(t, d, id, stale)
	_, posBefore, ok := routingEntry(d, id)
	if !ok || posBefore != 1 {
		t.Fatalf("expected node at bucket index 1, got ok=%v pos=%d", ok, posBefore)
	}

	d.handleQuery("tx", "ping", map[string]interface{}{"id": string(id[:])}, addr)

	after, posAfter, ok := routingEntry(d, id)
	if !ok {
		t.Fatal("node disappeared from routing table")
	}
	if !sameUDPAddr(after.Addr, addr) {
		t.Fatalf("same-address query changed stored address to %s", after.Addr)
	}
	if !after.LastSeen.After(stale) {
		t.Fatalf("LastSeen was not refreshed: %v", after.LastSeen)
	}
	if posAfter != 2 {
		t.Fatalf("expected node moved to bucket tail (index 2), got %d", posAfter)
	}
}

func TestHandleQueryNewNodeFollowsBucketRules(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	id := bucketPeerID(d.nodeID, 1)
	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	d.handleQuery("tx", "ping", map[string]interface{}{"id": string(id[:])}, addr)

	got, pos, ok := routingEntry(d, id)
	if !ok {
		t.Fatal("new node ID was not inserted")
	}
	if !sameUDPAddr(got.Addr, addr) {
		t.Fatalf("inserted address %s, want %s", got.Addr, addr)
	}
	if pos != 0 {
		t.Fatalf("expected sole new node at index 0, got %d", pos)
	}
}

func TestAnnouncePeerInvalidTokenDoesNotAddSender(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	id := bucketPeerID(d.nodeID, 1)
	addr := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
	infoHash := sha1.Sum([]byte("torrent"))
	d.handleQuery("tx", "announce_peer", map[string]interface{}{
		"id":        string(id[:]),
		"info_hash": string(infoHash[:]),
		"token":     "not-a-valid-token",
		"port":      int64(6881),
	}, addr)

	if c := d.NodesCount(); c != 0 {
		t.Fatalf("announce_peer with invalid token added %d routing-table nodes", c)
	}
	if _, _, ok := routingEntry(d, id); ok {
		t.Fatal("invalid-token announce_peer inserted the sender")
	}
}

func TestAddrChangeAdoptsUnreachableOldAddress(t *testing.T) {
	victim, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start victim DHT: %v", err)
	}
	defer victim.Close()

	live, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start live DHT: %v", err)
	}
	defer live.Close()

	id := live.nodeID
	oldAddr := &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1}
	victim.addNode(id, oldAddr)

	newAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(live.Port())}
	victim.handleQuery("tx", "ping", map[string]interface{}{"id": string(id[:])}, newAddr)

	deadline := time.After(routingTablePingTimeout + 3*time.Second)
	for {
		got, _, ok := routingEntry(victim, id)
		if ok && sameUDPAddr(got.Addr, newAddr) {
			return
		}
		select {
		case <-deadline:
			if !ok {
				t.Fatal("node disappeared before adoption")
			}
			t.Fatalf("did not adopt reachable new address, still %s", got.Addr)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestAddrChangeKeepsReachableOldAddress(t *testing.T) {
	victim, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start victim DHT: %v", err)
	}
	defer victim.Close()

	oldLive, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start old-address DHT: %v", err)
	}
	defer oldLive.Close()

	newLive, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start new-address DHT: %v", err)
	}
	defer newLive.Close()

	id := oldLive.nodeID
	oldAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(oldLive.Port())}
	victim.addNode(id, oldAddr)

	newAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(newLive.Port())}
	victim.handleQuery("tx", "ping", map[string]interface{}{"id": string(id[:])}, newAddr)

	waitAddrChangeIdle(t, victim, 3*time.Second)

	got, _, ok := routingEntry(victim, id)
	if !ok {
		t.Fatal("node disappeared from routing table")
	}
	if !sameUDPAddr(got.Addr, oldAddr) {
		t.Fatalf("reachable old address was replaced with %s", got.Addr)
	}
}

func TestAddrChangeVerificationIsBounded(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	id, oldAddr, pings, stop := startPingNode(t)
	defer stop()
	d.addNode(id, oldAddr)

	for i := 0; i < 32; i++ {
		spoof := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1000 + i}
		d.handleQuery("tx", "ping", map[string]interface{}{"id": string(id[:])}, spoof)
	}

	waitAddrChangeIdle(t, d, 3*time.Second)
	if n := pings.Load(); n != 1 {
		t.Fatalf("burst for one ID triggered %d pings, want 1", n)
	}

	got, _, ok := routingEntry(d, id)
	if !ok || !sameUDPAddr(got.Addr, oldAddr) {
		t.Fatal("burst replaced the stored address")
	}

	d.handleQuery("tx", "ping", map[string]interface{}{
		"id": string(id[:]),
	}, &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1000})
	waitAddrChangeIdle(t, d, time.Second)
	if n := pings.Load(); n != 1 {
		t.Fatalf("failed candidate retried immediately: ping count %d", n)
	}

	for i := 0; i < maxAddrChangeInFlight+16; i++ {
		nid := distinctNodeID(d.nodeID, i)
		stored := &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 2000 + i}
		spoof := &net.UDPAddr{IP: net.ParseIP("203.0.113.8"), Port: 2000 + i}
		d.addNode(nid, stored)
		d.handleQuery("tx", "ping", map[string]interface{}{"id": string(nid[:])}, spoof)
	}

	d.mu.Lock()
	inFlight := len(d.addrChangeInFlight)
	failed := len(d.addrChangeFailed)
	cooled := len(d.addrChangeCooldown)
	d.mu.Unlock()
	if inFlight > maxAddrChangeInFlight {
		t.Fatalf("in-flight verifications %d exceed cap %d", inFlight, maxAddrChangeInFlight)
	}
	if failed > maxAddrChangeFailed {
		t.Fatalf("failed-candidate set %d exceeds cap %d", failed, maxAddrChangeFailed)
	}
	if cooled > maxAddrChangeFailed {
		t.Fatalf("per-ID cooldown set %d exceeds cap %d", cooled, maxAddrChangeFailed)
	}
}

func distinctNodeID(local [20]byte, n int) [20]byte {
	id := sha1.Sum([]byte{byte(n), byte(n >> 8), byte(n >> 16)})
	if id == local {
		id[0] ^= 1
	}
	return id
}

func bucketPeerID(local [20]byte, n byte) [20]byte {
	var id [20]byte
	copy(id[:], local[:])
	id[0] ^= 128
	id[19] ^= n
	return id
}

func routingEntry(d *DHT, id [20]byte) (Node, int, bool) {
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

func setNodeLastSeen(t *testing.T, d *DHT, id [20]byte, seen time.Time) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	idx := bucketIndex(d.nodeID, id)
	b := d.buckets[idx]
	if b == nil {
		t.Fatal("bucket missing")
	}
	for i, n := range b.nodes {
		if n.ID == id {
			n.LastSeen = seen
			b.nodes[i] = n
			return
		}
	}
	t.Fatal("node missing")
}

func waitAddrChangeIdle(t *testing.T, d *DHT, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		d.mu.RLock()
		n := len(d.addrChangeInFlight)
		d.mu.RUnlock()
		if n == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("address-change verification still in flight after %s", timeout)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func startPingNode(t *testing.T) (id [20]byte, addr *net.UDPAddr, pings *atomic.Int32, stop func()) {
	t.Helper()
	id = sha1.Sum([]byte("ping-node"))
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var count atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 2048)
		for {
			n, src, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			parsed, err := bencode.Unmarshal(buf[:n])
			if err != nil {
				continue
			}
			dict, ok := parsed.(map[string]interface{})
			if !ok {
				continue
			}
			if dict["q"] == "ping" {
				count.Add(1)
			}
			txn, _ := dict["t"].(string)
			payload, err := bencode.Marshal(map[string]interface{}{
				"t": txn,
				"y": "r",
				"r": map[string]interface{}{"id": string(id[:])},
			})
			if err == nil {
				_, _ = conn.WriteToUDP(payload, src)
			}
		}
	}()
	return id, conn.LocalAddr().(*net.UDPAddr), &count, func() {
		_ = conn.Close()
		<-done
	}
}

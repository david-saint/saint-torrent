package dht

import (
	"crypto/sha1"
	"fmt"
	"net"
	"testing"
	"time"
)

// TestDHTQueryAddressChangeDoesNotOverwriteInPlace verifies checklist item 6(a):
// seed a node at 10.0.0.1:6881 via addNode, deliver a query claiming that ID from 203.0.113.9:1
// through handleQuery, and assert the entry still points at 10.0.0.1:6881 and its bucket position is unchanged.
func TestDHTQueryAddressChangeDoesNotOverwriteInPlace(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	// Seed victim node at 10.0.0.1:6881
	victimID := sha1.Sum([]byte("victim_node"))
	honestAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	d.addNode(victimID, honestAddr)

	// Seed another node into the same bucket after victim, so victim is at index 0 and second is at index 1
	otherID := victimID
	otherID[19] ^= 1 // toggling lowest bit keeps same bucketIndex
	otherAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 6881}
	d.addNode(otherID, otherAddr)

	idx := bucketIndex(d.nodeID, victimID)
	d.mu.RLock()
	b := d.buckets[idx]
	if b == nil || len(b.nodes) != 2 {
		d.mu.RUnlock()
		t.Fatalf("expected 2 nodes in bucket %d, got %v", idx, b)
	}
	if b.nodes[0].ID != victimID {
		d.mu.RUnlock()
		t.Fatalf("expected victim at index 0, got %x", b.nodes[0].ID)
	}
	origLastSeen := b.nodes[0].LastSeen
	d.mu.RUnlock()

	time.Sleep(5 * time.Millisecond)

	// Deliver attacker query claiming victimID from 203.0.113.9:1
	attackerAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
	d.handleQuery("tx1", "ping", map[string]interface{}{
		"id": string(victimID[:]),
	}, attackerAddr)

	// Assert entry still points at 10.0.0.1:6881, bucket position is unchanged (index 0), and LastSeen is unchanged
	d.mu.RLock()
	defer d.mu.RUnlock()
	b = d.buckets[idx]
	if len(b.nodes) != 2 {
		t.Fatalf("expected 2 nodes in bucket, got %d", len(b.nodes))
	}
	if b.nodes[0].ID != victimID {
		t.Errorf("expected victim node to remain at index 0 (unchanged bucket position), but got %x", b.nodes[0].ID)
	}
	if !b.nodes[0].Addr.IP.Equal(honestAddr.IP) || b.nodes[0].Addr.Port != honestAddr.Port {
		t.Errorf("expected entry to still point at %s, got %s", honestAddr, b.nodes[0].Addr)
	}
	if !b.nodes[0].LastSeen.Equal(origLastSeen) {
		t.Errorf("expected LastSeen to remain unchanged (%v), got %v", origLastSeen, b.nodes[0].LastSeen)
	}
}

// TestDHTQuerySameAddressRefreshesLastSeenAndMovesToTail verifies checklist item 6(b):
// a query from the same address as the stored entry refreshes LastSeen and moves the entry to the tail.
func TestDHTQuerySameAddressRefreshesLastSeenAndMovesToTail(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	id1 := sha1.Sum([]byte("node_1"))
	addr1 := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5001}
	d.addNode(id1, addr1)

	id2 := id1
	id2[19] ^= 1
	addr2 := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5002}
	d.addNode(id2, addr2)

	idx := bucketIndex(d.nodeID, id1)
	d.mu.RLock()
	b := d.buckets[idx]
	if b == nil || len(b.nodes) != 2 {
		d.mu.RUnlock()
		t.Fatalf("expected 2 nodes in bucket %d", idx)
	}
	if b.nodes[0].ID != id1 || b.nodes[1].ID != id2 {
		d.mu.RUnlock()
		t.Fatalf("unexpected initial bucket order: [0]=%x, [1]=%x", b.nodes[0].ID, b.nodes[1].ID)
	}
	origLastSeen := b.nodes[0].LastSeen
	d.mu.RUnlock()

	time.Sleep(10 * time.Millisecond)

	// Deliver query from same address addr1
	d.handleQuery("tx2", "ping", map[string]interface{}{
		"id": string(id1[:]),
	}, addr1)

	d.mu.RLock()
	defer d.mu.RUnlock()
	b = d.buckets[idx]
	if b.nodes[0].ID != id2 || b.nodes[1].ID != id1 {
		t.Errorf("expected id1 to be moved to tail (index 1), got [0]=%x, [1]=%x", b.nodes[0].ID, b.nodes[1].ID)
	}
	if !b.nodes[1].LastSeen.After(origLastSeen) {
		t.Errorf("expected LastSeen to be updated after query, was %v, now %v", origLastSeen, b.nodes[1].LastSeen)
	}
	if !b.nodes[1].Addr.IP.Equal(addr1.IP) || b.nodes[1].Addr.Port != addr1.Port {
		t.Errorf("expected addr to remain %s, got %s", addr1, b.nodes[1].Addr)
	}
}

// TestDHTAddressChangeVerificationAdoptionAndRetention verifies checklist item 6(c):
// with two live DHT instances over loopback, a node whose old address is unreachable and whose
// new address answers a ping with the same ID is re-pointed after verification, and one whose
// old address still answers is not.
func TestDHTAddressChangeVerificationAdoptionAndRetention(t *testing.T) {
	t.Run("AdoptionWhenOldAddressUnreachable", func(t *testing.T) {
		local, err := NewDHT(t.TempDir(), 0)
		if err != nil {
			t.Fatalf("failed to start local DHT: %v", err)
		}
		defer local.Close()

		remote, err := NewDHT(t.TempDir(), 0)
		if err != nil {
			t.Fatalf("failed to start remote DHT: %v", err)
		}
		defer remote.Close()

		remotePort := remote.Port()
		if remotePort == 0 {
			t.Fatal("remote DHT has no port")
		}

		// Allocate an ephemeral UDP port and close it immediately so it is unreachable
		deadConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
		if err != nil {
			t.Fatalf("failed to listen UDP: %v", err)
		}
		deadAddr := deadConn.LocalAddr().(*net.UDPAddr)
		deadConn.Close()

		// Seed remote's nodeID at deadAddr in local's routing table
		local.addNode(remote.nodeID, deadAddr)
		if !local.HasNodeAddress(deadAddr.IP, uint16(deadAddr.Port)) {
			t.Fatalf("expected dead address %s in routing table", deadAddr)
		}

		// Deliver query claiming remote.nodeID from remote's real live address
		realAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(remotePort)}
		local.handleQuery("tx_adopt", "ping", map[string]interface{}{
			"id": string(remote.nodeID[:]),
		}, realAddr)

		// Verification runs asynchronously on a tracked goroutine.
		// It pings deadAddr (fails/times out), then pings realAddr (succeeds with matching ID).
		// Wait with deadline for realAddr to be adopted.
		deadline := time.After(5 * time.Second)
		for !local.HasNodeAddress(realAddr.IP, remotePort) {
			select {
			case <-deadline:
				t.Fatalf("timed out waiting for address adoption to %s; routing table nodes: %d", realAddr, local.NodesCount())
			case <-time.After(50 * time.Millisecond):
			}
		}

		if local.HasNodeAddress(deadAddr.IP, uint16(deadAddr.Port)) {
			t.Errorf("expected dead address %s to no longer be in routing table", deadAddr)
		}
	})

	t.Run("RetentionWhenOldAddressStillAnswers", func(t *testing.T) {
		local, err := NewDHT(t.TempDir(), 0)
		if err != nil {
			t.Fatalf("failed to start local DHT: %v", err)
		}
		defer local.Close()

		honestRemote, err := NewDHT(t.TempDir(), 0)
		if err != nil {
			t.Fatalf("failed to start honest remote DHT: %v", err)
		}
		defer honestRemote.Close()

		honestPort := honestRemote.Port()
		if honestPort == 0 {
			t.Fatal("honest remote DHT has no port")
		}
		honestAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(honestPort)}

		// Seed honestRemote in local's routing table
		local.addNode(honestRemote.nodeID, honestAddr)
		if !local.HasNodeAddress(honestAddr.IP, honestPort) {
			t.Fatalf("expected honest address %s in routing table", honestAddr)
		}

		// Deliver spoofed query claiming honestRemote.nodeID from an attacker address
		attackerAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.88"), Port: 6881}
		local.handleQuery("tx_spoof", "ping", map[string]interface{}{
			"id": string(honestRemote.nodeID[:]),
		}, attackerAddr)

		// Wait for verification goroutine to finish (inFlightVerifications empty)
		deadline := time.After(3 * time.Second)
		for {
			local.mu.RLock()
			_, inFlight := local.inFlightVerifications[honestRemote.nodeID]
			local.mu.RUnlock()
			if !inFlight {
				break
			}
			select {
			case <-deadline:
				t.Fatal("timed out waiting for verification to complete")
			case <-time.After(20 * time.Millisecond):
			}
		}

		// Honest address must still be retained; attacker address must not be adopted
		if !local.HasNodeAddress(honestAddr.IP, honestPort) {
			t.Errorf("expected honest address %s to be retained", honestAddr)
		}
		if local.HasNodeAddress(attackerAddr.IP, uint16(attackerAddr.Port)) {
			t.Errorf("expected attacker address %s to be rejected", attackerAddr)
		}
	})
}

// TestDHTVerificationBounded verifies checklist item 6(d):
// at most one in-flight verification per node ID, global cap on concurrent pending verifications,
// and failed candidates are not retried immediately.
func TestDHTVerificationBounded(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	// 1. At most one in-flight verification per node ID
	nodeID := sha1.Sum([]byte("bounded_node"))
	initialAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881}
	d.addNode(nodeID, initialAddr)

	// Burst 20 queries with different candidate addresses for the same nodeID
	for i := 1; i <= 20; i++ {
		candidateAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 1000 + i}
		d.handleQuery(fmt.Sprintf("burst_%d", i), "ping", map[string]interface{}{
			"id": string(nodeID[:]),
		}, candidateAddr)
	}

	d.mu.RLock()
	inFlightForID := len(d.inFlightVerifications)
	d.mu.RUnlock()
	if inFlightForID != 1 {
		t.Errorf("expected exactly 1 in-flight verification for nodeID burst, got %d", inFlightForID)
	}

	// 2. Global cap on concurrent pending verifications
	d2, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d2.Close()

	// Seed 100 distinct nodes
	var nodeIDs [100][20]byte
	for i := 0; i < 100; i++ {
		nodeIDs[i] = sha1.Sum([]byte(fmt.Sprintf("node_cap_%d", i)))
		// Ensure different from d2.nodeID
		if nodeIDs[i] == d2.nodeID {
			nodeIDs[i][0] ^= 1
		}
		d2.addNode(nodeIDs[i], &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 2000 + i})
	}

	// Flood address change queries for all 100 nodes
	for i := 0; i < 100; i++ {
		d2.handleQuery(fmt.Sprintf("flood_%d", i), "ping", map[string]interface{}{
			"id": string(nodeIDs[i][:]),
		}, &net.UDPAddr{IP: net.ParseIP("203.0.113.2"), Port: 3000 + i})
	}

	d2.mu.RLock()
	totalInFlight := len(d2.inFlightVerifications)
	d2.mu.RUnlock()
	if totalInFlight > maxInFlightVerifications {
		t.Errorf("in-flight verifications exceeded cap: got %d, max is %d", totalInFlight, maxInFlightVerifications)
	}

	// 3. Failed candidate cooldown: not retried immediately
	d3, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d3.Close()

	liveNode, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start live node: %v", err)
	}
	defer liveNode.Close()

	liveAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(liveNode.Port())}
	d3.addNode(liveNode.nodeID, liveAddr)

	// Send candidate query that fails (because incumbent liveNode answers)
	candidateAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.99"), Port: 9999}
	d3.handleQuery("cooldown_1", "ping", map[string]interface{}{
		"id": string(liveNode.nodeID[:]),
	}, candidateAddr)

	// Wait for verification to fail and candidate to be recorded in failedCandidates
	candKey := makeCandidateKey(liveNode.nodeID, candidateAddr)
	deadline := time.After(3 * time.Second)
	for {
		d3.mu.RLock()
		_, failed := d3.failedCandidates[candKey]
		_, inFlight := d3.inFlightVerifications[liveNode.nodeID]
		d3.mu.RUnlock()
		if failed && !inFlight {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for candidate to fail verification")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Send another query from the same candidate address and ID
	d3.handleQuery("cooldown_2", "ping", map[string]interface{}{
		"id": string(liveNode.nodeID[:]),
	}, candidateAddr)

	// It must NOT launch a new verification
	d3.mu.RLock()
	_, inFlight := d3.inFlightVerifications[liveNode.nodeID]
	d3.mu.RUnlock()
	if inFlight {
		t.Error("failed candidate in cooldown was immediately retried for verification")
	}
}

// TestDHTAnnouncePeerInvalidTokenDoesNotAddNode verifies checklist item 6(e):
// an announce_peer query with an invalid token never admits the sender to the routing table,
// whereas a valid token does admit the sender.
func TestDHTAnnouncePeerInvalidTokenDoesNotAddNode(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5000}
	infoHash := sha1.Sum([]byte("test_torrent"))
	nodeID := sha1.Sum([]byte("malicious_node"))

	// 1. Send announce_peer with invalid token
	d.handleQuery("t1", "announce_peer", map[string]interface{}{
		"id":        string(nodeID[:]),
		"info_hash": string(infoHash[:]),
		"token":     "invalid_token",
		"port":      int64(5000),
	}, addr)

	if d.NodesCount() != 0 {
		t.Fatalf("expected 0 nodes in routing table after invalid token, got %d", d.NodesCount())
	}
	if d.HasNodeAddress(addr.IP, 5000) {
		t.Fatal("expected sender with invalid token not to be present in routing table")
	}

	// 2. Send announce_peer with valid token
	validToken := d.generateToken(addr)
	d.handleQuery("t2", "announce_peer", map[string]interface{}{
		"id":        string(nodeID[:]),
		"info_hash": string(infoHash[:]),
		"token":     validToken,
		"port":      int64(5000),
	}, addr)

	if d.NodesCount() != 1 {
		t.Fatalf("expected 1 node in routing table after valid token, got %d", d.NodesCount())
	}
	if !d.HasNodeAddress(addr.IP, 5000) {
		t.Fatal("expected sender with valid token to be present in routing table")
	}
}

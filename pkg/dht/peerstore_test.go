package dht

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// checkPeerStore verifies the store's derived state: the recency list holds
// exactly the stored swarms, in non-increasing announce order, and ipHashes
// matches the swarms' contents.
func checkPeerStore(t *testing.T, s *peerStore) {
	t.Helper()
	seen := 0
	var prev *swarm
	for sw := s.head; sw != nil; sw = sw.next {
		if sw.prev != prev {
			t.Fatalf("recency list back-link broken at %x", sw.hash)
		}
		if s.swarms[sw.hash] != sw {
			t.Fatalf("listed swarm %x is not in the map", sw.hash)
		}
		if prev != nil && sw.lastAnnounce.After(prev.lastAnnounce) {
			t.Fatalf("recency list out of order at %x", sw.hash)
		}
		if len(sw.peers) == 0 || len(sw.peers) > maxPeersPerHash {
			t.Fatalf("swarm %x holds %d peers", sw.hash, len(sw.peers))
		}
		prev = sw
		seen++
	}
	if s.tail != prev {
		t.Fatal("recency list tail is not its last element")
	}
	if seen != len(s.swarms) {
		t.Fatalf("recency list holds %d swarms, map holds %d", seen, len(s.swarms))
	}
	want := make(map[[4]byte]int)
	for _, sw := range s.swarms {
		perIP := make(map[[4]byte]int)
		for _, p := range sw.peers {
			perIP[p.ip]++
		}
		for ip, n := range perIP {
			if n > maxPeersPerIPPerHash {
				t.Fatalf("IP %v holds %d entries in swarm %x", ip, n, sw.hash)
			}
			want[ip]++
		}
	}
	if len(want) != len(s.ipHashes) {
		t.Fatalf("ipHashes tracks %d IPs, swarms hold %d", len(s.ipHashes), len(want))
	}
	for ip, n := range want {
		if s.ipHashes[ip] != n {
			t.Fatalf("ipHashes[%v] = %d, want %d", ip, s.ipHashes[ip], n)
		}
	}
}

func testHash(i int) [20]byte {
	var h [20]byte
	copy(h[:], "stored-hash-")
	binary.BigEndian.PutUint32(h[16:], uint32(i))
	return h
}

func testIP(i int) [4]byte {
	return [4]byte{203, byte(i >> 16), byte(i >> 8), byte(i)}
}

func storedPortsFor(s *peerStore, hash [20]byte, ip [4]byte) int {
	sw := s.swarms[hash]
	if sw == nil {
		return 0
	}
	n := 0
	for _, p := range sw.peers {
		if p.ip == ip {
			n++
		}
	}
	return n
}

// TestAnnounceFromOneIPCannotFillSwarm is the reported flush: one token holder
// announcing 50 ports for an info-hash must not push out the honest announcer
// or fill the values we hand out.
func TestAnnounceFromOneIPCannotFillSwarm(t *testing.T) {
	d, _ := newFakeDHT(t)

	var infoHash [20]byte
	copy(infoHash[:], "swarm-under-attack--")
	honest := &net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 6881}
	attacker := &net.UDPAddr{IP: net.ParseIP("192.0.2.66"), Port: 6881}

	announce := func(from *net.UDPAddr, port int) {
		id := idInBucket(d.nodeID, 30, uint16(port))
		d.handleQuery("tx", "announce_peer", map[string]interface{}{
			"id":        string(id[:]),
			"info_hash": string(infoHash[:]),
			"token":     d.generateToken(from),
			"port":      int64(port),
		}, from)
	}
	announce(honest, 51413)
	for port := 1000; port < 1050; port++ {
		announce(attacker, port)
	}

	values := d.getPeersForInfoHash(infoHash, nil)
	if len(values) != 1+maxPeersPerIPPerHash {
		t.Fatalf("get_peers returns %d values, want the honest peer plus %d attacker ports", len(values), maxPeersPerIPPerHash)
	}
	fromAttacker, honestKept := 0, false
	for _, v := range values {
		cp := v.(string)
		ip := net.IP([]byte(cp[:4]))
		switch {
		case ip.Equal(attacker.IP):
			fromAttacker++
			if port := binary.BigEndian.Uint16([]byte(cp[4:])); port < 1050-maxPeersPerIPPerHash {
				t.Fatalf("attacker kept stale port %d instead of its newest ones", port)
			}
		case ip.Equal(honest.IP):
			honestKept = true
		}
	}
	if !honestKept {
		t.Fatal("the honest announcer was evicted by one IP's announces")
	}
	if fromAttacker != maxPeersPerIPPerHash {
		t.Fatalf("attacker holds %d values, want %d", fromAttacker, maxPeersPerIPPerHash)
	}
	d.peersMu.Lock()
	checkPeerStore(t, d.peers)
	d.peersMu.Unlock()
}

// TestAnnounceFromOneIPCannotFlushStore verifies one IP announcing hundreds of
// fresh info-hashes cannot evict the swarms other announcers stored.
func TestAnnounceFromOneIPCannotFlushStore(t *testing.T) {
	s := newPeerStore()
	now := time.Now()
	for i := 0; i < 100; i++ {
		if !s.announce(testHash(i), testIP(i), 6881, now) {
			t.Fatalf("honest announce %d refused", i)
		}
	}

	attacker := [4]byte{192, 0, 2, 66}
	accepted := 0
	for i := 0; i < 600; i++ {
		if s.announce(testHash(100000+i), attacker, 6881, now) {
			accepted++
		}
	}
	if accepted != maxHashesPerIP {
		t.Fatalf("one IP stored peers for %d info-hashes, want the cap %d", accepted, maxHashesPerIP)
	}
	for i := 0; i < 100; i++ {
		if s.swarms[testHash(i)] == nil {
			t.Fatalf("honest swarm %d was flushed", i)
		}
	}
	// Re-announcing a swarm it already holds stays allowed at the cap.
	if !s.announce(testHash(100000), attacker, 6882, now) {
		t.Fatal("an IP at its info-hash cap could not refresh a swarm it holds")
	}
	checkPeerStore(t, s)
}

// TestPeerStoreEvictsLeastRecentlyAnnounced verifies a full store makes room by
// dropping the swarm announced to longest ago, not an arbitrary one.
func TestPeerStoreEvictsLeastRecentlyAnnounced(t *testing.T) {
	s := newPeerStore()
	now := time.Now()
	for i := 0; i < maxStoredHashes; i++ {
		s.announce(testHash(i), testIP(i), 6881, now.Add(time.Duration(i)*time.Millisecond))
	}
	later := now.Add(time.Second)
	s.announce(testHash(0), testIP(0), 6881, later)
	s.announce(testHash(maxStoredHashes), testIP(maxStoredHashes), 6881, later)

	if len(s.swarms) != maxStoredHashes {
		t.Fatalf("store holds %d swarms, want %d", len(s.swarms), maxStoredHashes)
	}
	if s.swarms[testHash(0)] == nil {
		t.Fatal("the swarm announced to most recently was evicted")
	}
	if s.swarms[testHash(1)] != nil {
		t.Fatal("the least recently announced swarm survived eviction")
	}
	checkPeerStore(t, s)
}

// TestPeerStoreExpiry verifies expired peers are neither returned nor counted
// against their IP's info-hash cap.
func TestPeerStoreExpiry(t *testing.T) {
	s := newPeerStore()
	now := time.Now()
	ip := [4]byte{192, 0, 2, 7}
	for i := 0; i < maxHashesPerIP; i++ {
		s.announce(testHash(i), ip, 6881, now)
	}
	if s.announce(testHash(999), ip, 6881, now) {
		t.Fatal("an IP past its info-hash cap was accepted")
	}

	later := now.Add(peerStoreTTL + time.Second)
	if got := s.get(testHash(0), later, nil); got != nil {
		t.Fatalf("expired peers were returned: %d", len(got))
	}
	if !s.announce(testHash(999), ip, 6881, later) {
		t.Fatal("expired entries still counted against the IP's info-hash cap")
	}
	if len(s.swarms) != 1 {
		t.Fatalf("store holds %d swarms after expiry, want 1", len(s.swarms))
	}
	checkPeerStore(t, s)
}

// TestPeerStoreSwarmCap verifies a full swarm evicts its oldest peer and keeps
// the IP accounting straight when that peer was its IP's only entry.
func TestPeerStoreSwarmCap(t *testing.T) {
	s := newPeerStore()
	now := time.Now()
	hash := testHash(1)
	for i := 0; i < maxPeersPerHash; i++ {
		s.announce(hash, testIP(i), 6881, now.Add(time.Duration(i)*time.Millisecond))
	}
	// testIP(0) holds the oldest entry and re-announces a second port.
	s.announce(hash, testIP(0), 6882, now.Add(time.Second))
	if got := len(s.swarms[hash].peers); got != maxPeersPerHash {
		t.Fatalf("swarm holds %d peers, want %d", got, maxPeersPerHash)
	}
	if got := storedPortsFor(s, hash, testIP(0)); got != 1 {
		t.Fatalf("re-announcing IP holds %d entries, want 1", got)
	}
	checkPeerStore(t, s)
}

// BenchmarkPeerStoreAnnounceNewHashFullStore measures announcing fresh
// info-hashes into a store already holding maxStoredHashes full swarms: the
// eviction is O(1), not a scan of every stored entry.
func BenchmarkPeerStoreAnnounceNewHashFullStore(b *testing.B) {
	s := newPeerStore()
	now := time.Now()
	for h := 0; h < maxStoredHashes; h++ {
		for p := 0; p < maxPeersPerHash; p++ {
			s.announce(testHash(h), testIP(h*maxPeersPerHash+p), 6881, now)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.announce(testHash(maxStoredHashes+i), testIP(1<<22+i), 6881, now)
	}
}

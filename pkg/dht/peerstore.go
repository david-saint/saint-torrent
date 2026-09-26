package dht

import (
	"encoding/binary"
	"net"
	"time"
)

const (
	// peerStoreTTL is how long an announced peer is kept without a re-announce.
	peerStoreTTL = 30 * time.Minute
	// maxStoredHashes caps how many info-hashes we store peers for.
	maxStoredHashes = 500
	// maxPeersPerHash caps the peers stored, and returned, per info-hash.
	maxPeersPerHash = 50
	// maxPeersPerIPPerHash caps one IP's entries in a single swarm, so one
	// token holder cannot fill a swarm's values by announcing many ports. It is
	// a few rather than one so hosts sharing an address behind CGNAT still fit.
	maxPeersPerIPPerHash = 4
	// maxHashesPerIP caps how many info-hashes one IP may hold entries in.
	// Entries outlive the 10-20 minute token window, so this also bounds how
	// many new info-hashes one token holder can push into the store per window.
	maxHashesPerIP = 20
)

type storedPeer struct {
	seen time.Time
	// order is the store's announce count when this entry was last announced.
	// Recency is judged by it, not by seen, which ties on coarse clocks
	// (Windows ticks in milliseconds): with ties, one slot kept being replaced
	// and an IP's older ports outlived its newest ones.
	order uint64
	ip    [4]byte
	port  uint16
}

// swarm holds the peers announced for one info-hash. Swarms are linked in
// announce order, most recently announced first, so eviction is O(1).
type swarm struct {
	hash         [20]byte
	peers        []storedPeer
	lastAnnounce time.Time
	prev, next   *swarm
}

// peerStore holds announce_peer registrations. Every operation is O(1)
// amortised apart from scans of a single swarm, which maxPeersPerHash bounds.
// Callers serialise access (DHT.peersMu).
type peerStore struct {
	swarms     map[[20]byte]*swarm
	head, tail *swarm
	// ipHashes counts, per IP, the swarms that hold at least one of its entries.
	ipHashes map[[4]byte]int
	// announces counts announces, ordering entries by recency (storedPeer.order).
	announces uint64
}

func newPeerStore() *peerStore {
	return &peerStore{
		swarms:   make(map[[20]byte]*swarm),
		ipHashes: make(map[[4]byte]int),
	}
}

// announce records ip:port for hash, reporting false when the store refused it.
func (s *peerStore) announce(hash [20]byte, ip [4]byte, port uint16, now time.Time) bool {
	s.expireStale(now)

	sw := s.swarms[hash]
	if sw != nil {
		s.pruneSwarm(sw, now)
		sw = s.swarms[hash]
	}
	if (sw == nil || !sw.hasIP(ip)) && s.ipHashes[ip] >= maxHashesPerIP {
		return false
	}
	if sw == nil {
		if len(s.swarms) >= maxStoredHashes && s.tail != nil {
			s.removeSwarm(s.tail)
		}
		sw = &swarm{hash: hash}
		s.swarms[hash] = sw
	} else {
		s.unlink(sw)
	}
	s.pushFront(sw)
	sw.lastAnnounce = now
	s.announces++
	order := s.announces

	sameIP, oldestSameIP, oldest := 0, -1, -1
	for i := range sw.peers {
		p := &sw.peers[i]
		if p.ip == ip && p.port == port {
			p.seen, p.order = now, order
			return true
		}
		if p.ip == ip {
			sameIP++
			if oldestSameIP < 0 || p.order < sw.peers[oldestSameIP].order {
				oldestSameIP = i
			}
		}
		if oldest < 0 || p.order < sw.peers[oldest].order {
			oldest = i
		}
	}
	switch {
	case sameIP >= maxPeersPerIPPerHash:
		// The IP's newest port replaces its oldest; nobody else loses a slot.
		sw.peers[oldestSameIP].port = port
		sw.peers[oldestSameIP].seen = now
		sw.peers[oldestSameIP].order = order
		return true
	case len(sw.peers) >= maxPeersPerHash:
		s.dropPeer(sw, oldest)
	}
	// Re-check rather than trust sameIP: the eviction above may have removed
	// this IP's only entry and released its count already.
	if !sw.hasIP(ip) {
		s.ipHashes[ip]++
	}
	sw.peers = append(sw.peers, storedPeer{seen: now, order: order, ip: ip, port: port})
	return true
}

// get returns up to maxPeersPerHash live peers for hash as compact values.
// When asker is set, peers it may not be told about under netpolicy's scope
// rule are left out, as closestHeardNodes does for contacts: a public asker
// never learns the loopback or LAN peers that announced to us from this host
// or its network.
func (s *peerStore) get(hash [20]byte, now time.Time, asker *net.UDPAddr) []interface{} {
	sw := s.swarms[hash]
	if sw == nil {
		return nil
	}
	s.pruneSwarm(sw, now)
	if s.swarms[hash] == nil {
		return nil
	}
	list := make([]interface{}, 0, len(sw.peers))
	for _, p := range sw.peers {
		if asker != nil && !endpointAllowed(nodeAddrKey{ip: p.ip, port: p.port}, asker) {
			continue
		}
		var comp [6]byte
		copy(comp[0:4], p.ip[:])
		binary.BigEndian.PutUint16(comp[4:6], p.port)
		list = append(list, string(comp[:]))
	}
	return list
}

func (sw *swarm) hasIP(ip [4]byte) bool {
	for i := range sw.peers {
		if sw.peers[i].ip == ip {
			return true
		}
	}
	return false
}

// expireStale drops whole swarms nobody has announced to within the TTL,
// oldest first. Each swarm is removed once, so this is O(1) amortised.
func (s *peerStore) expireStale(now time.Time) {
	for s.tail != nil && now.Sub(s.tail.lastAnnounce) > peerStoreTTL {
		s.removeSwarm(s.tail)
	}
}

// pruneSwarm drops sw's expired peers and removes sw once it is empty.
func (s *peerStore) pruneSwarm(sw *swarm, now time.Time) {
	for i := len(sw.peers) - 1; i >= 0; i-- {
		if now.Sub(sw.peers[i].seen) > peerStoreTTL {
			s.dropPeer(sw, i)
		}
	}
	if len(sw.peers) == 0 {
		s.removeSwarm(sw)
	}
}

// dropPeer removes sw.peers[i], releasing the IP's swarm count when it was the
// IP's last entry in sw. Order within a swarm does not matter.
func (s *peerStore) dropPeer(sw *swarm, i int) {
	ip := sw.peers[i].ip
	last := len(sw.peers) - 1
	sw.peers[i] = sw.peers[last]
	sw.peers = sw.peers[:last]
	if !sw.hasIP(ip) {
		if s.ipHashes[ip] <= 1 {
			delete(s.ipHashes, ip)
		} else {
			s.ipHashes[ip]--
		}
	}
}

func (s *peerStore) removeSwarm(sw *swarm) {
	for len(sw.peers) > 0 {
		s.dropPeer(sw, len(sw.peers)-1)
	}
	s.unlink(sw)
	delete(s.swarms, sw.hash)
}

func (s *peerStore) pushFront(sw *swarm) {
	sw.prev = nil
	sw.next = s.head
	if s.head != nil {
		s.head.prev = sw
	}
	s.head = sw
	if s.tail == nil {
		s.tail = sw
	}
}

func (s *peerStore) unlink(sw *swarm) {
	if sw.prev != nil {
		sw.prev.next = sw.next
	} else if s.head == sw {
		s.head = sw.next
	}
	if sw.next != nil {
		sw.next.prev = sw.prev
	} else if s.tail == sw {
		s.tail = sw.prev
	}
	sw.prev, sw.next = nil, nil
}

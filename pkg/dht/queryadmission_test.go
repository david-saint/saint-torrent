package dht

import (
	"net"
	"sync"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
)

// fakePacket is one datagram captured on, or injected into, a fakeConn.
type fakePacket struct {
	data []byte
	addr *net.UDPAddr
}

// fakeConn is a PacketConn that records everything the DHT sends and never
// answers, so routing-table behaviour can be asserted without real sockets.
type fakeConn struct {
	local  *net.UDPAddr
	mu     sync.Mutex
	sent   []fakePacket
	in     chan fakePacket
	closed chan struct{}
	once   sync.Once
}

func newFakeConn() *fakeConn {
	return &fakeConn{
		local:  &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6881},
		in:     make(chan fakePacket, 16),
		closed: make(chan struct{}),
	}
}

func (c *fakeConn) ReadFromUDP(b []byte) (int, *net.UDPAddr, error) {
	select {
	case p := <-c.in:
		return copy(b, p.data), p.addr, nil
	case <-c.closed:
		return 0, nil, net.ErrClosed
	}
}

func (c *fakeConn) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	stored := &net.UDPAddr{IP: append(net.IP(nil), addr.IP...), Port: addr.Port, Zone: addr.Zone}
	c.mu.Lock()
	c.sent = append(c.sent, fakePacket{data: append([]byte(nil), b...), addr: stored})
	c.mu.Unlock()
	return len(b), nil
}

func (c *fakeConn) LocalAddr() net.Addr { return c.local }

func (c *fakeConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// queriesTo counts outbound queries of the given type sent to addr.
func (c *fakeConn) queriesTo(addr *net.UDPAddr, q string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	count := 0
	for _, p := range c.sent {
		if !sameUDPAddr(p.addr, addr) {
			continue
		}
		parsed, err := bencode.Unmarshal(p.data)
		if err != nil {
			continue
		}
		dict, ok := parsed.(map[string]interface{})
		if !ok {
			continue
		}
		if y, _ := dict["y"].(string); y != "q" {
			continue
		}
		if got, _ := dict["q"].(string); got == q {
			count++
		}
	}
	return count
}

// lastQueryTo returns the transaction ID of the most recent outbound query of
// the given type sent to addr.
func (c *fakeConn) lastQueryTo(addr *net.UDPAddr, q string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := len(c.sent) - 1; i >= 0; i-- {
		p := c.sent[i]
		if !sameUDPAddr(p.addr, addr) {
			continue
		}
		parsed, err := bencode.Unmarshal(p.data)
		if err != nil {
			continue
		}
		dict, ok := parsed.(map[string]interface{})
		if !ok {
			continue
		}
		if y, _ := dict["y"].(string); y != "q" {
			continue
		}
		if got, _ := dict["q"].(string); got != q {
			continue
		}
		tid, _ := dict["t"].(string)
		return tid, true
	}
	return "", false
}

// injectPingReply feeds a bencoded ping response into the read loop as if it
// had arrived from addr, resolving the transaction tid.
func (c *fakeConn) injectPingReply(t *testing.T, tid string, id [20]byte, addr *net.UDPAddr) {
	t.Helper()
	payload, err := bencode.Marshal(map[string]interface{}{
		"t": tid,
		"y": "r",
		"r": map[string]interface{}{"id": string(id[:])},
	})
	if err != nil {
		t.Fatalf("failed to encode ping reply: %v", err)
	}
	select {
	case c.in <- fakePacket{data: payload, addr: &net.UDPAddr{IP: append(net.IP(nil), addr.IP...), Port: addr.Port}}:
	case <-time.After(5 * time.Second):
		t.Fatal("read loop never consumed the injected reply")
	}
}

func newFakeDHT(t *testing.T) (*DHT, *fakeConn) {
	t.Helper()
	conn := newFakeConn()
	d, err := NewDHTWithConn(t.TempDir(), conn)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	t.Cleanup(d.Close)
	return d, conn
}

// idInBucket returns an ID that lands in the given bucket of a table owned by
// base. Only buckets below 144 are safe here, since salt perturbs the last bits.
func idInBucket(base [20]byte, bucket int, salt uint16) [20]byte {
	id := base
	id[bucket/8] ^= 1 << (7 - uint(bucket%8))
	id[18] ^= byte(salt >> 8)
	id[19] ^= byte(salt)
	return id
}

func bucketNodes(d *DHT, bucket int) []Node {
	d.mu.RLock()
	defer d.mu.RUnlock()
	b := d.buckets[bucket]
	if b == nil {
		return nil
	}
	return append([]Node(nil), b.nodes...)
}

// TestAnnouncePeerWithInvalidTokenDoesNotAddSender verifies an unvalidated
// announce_peer never reaches the routing table, while a valid one still does.
func TestAnnouncePeerWithInvalidTokenDoesNotAddSender(t *testing.T) {
	d, _ := newFakeDHT(t)

	addr := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 6881}
	var infoHash [20]byte
	copy(infoHash[:], "info-hash-for-test--")
	senderID := idInBucket(d.nodeID, 11, 1)

	args := map[string]interface{}{
		"id":        string(senderID[:]),
		"info_hash": string(infoHash[:]),
		"token":     "not-a-valid-token",
		"port":      int64(51413),
	}
	d.handleQuery("tx", "announce_peer", args, addr)

	if got := d.NodesCount(); got != 0 {
		t.Fatalf("announce_peer with a bad token added %d nodes", got)
	}
	if peers := d.getPeersForInfoHash(infoHash); len(peers) != 0 {
		t.Fatalf("announce_peer with a bad token registered %d peers", len(peers))
	}

	args["token"] = d.generateToken(addr)
	d.handleQuery("tx", "announce_peer", args, addr)
	if got := d.NodesCount(); got != 1 {
		t.Fatalf("announce_peer with a valid token should add the sender, got %d nodes", got)
	}
}

// TestMalformedQueryDoesNotAddSender verifies a sender is admitted only once the
// query it sent is known to be well-formed.
func TestMalformedQueryDoesNotAddSender(t *testing.T) {
	addr := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 6881}

	cases := []struct {
		name  string
		query string
		args  func(id [20]byte) map[string]interface{}
	}{
		{
			name:  "find_node without target",
			query: "find_node",
			args: func(id [20]byte) map[string]interface{} {
				return map[string]interface{}{"id": string(id[:])}
			},
		},
		{
			name:  "get_peers with short info_hash",
			query: "get_peers",
			args: func(id [20]byte) map[string]interface{} {
				return map[string]interface{}{"id": string(id[:]), "info_hash": "too-short"}
			},
		},
		{
			name:  "unknown query type",
			query: "not_a_query",
			args: func(id [20]byte) map[string]interface{} {
				return map[string]interface{}{"id": string(id[:])}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := newFakeDHT(t)
			senderID := idInBucket(d.nodeID, 12, 1)
			d.handleQuery("tx", tc.query, tc.args(senderID), addr)
			if got := d.NodesCount(); got != 0 {
				t.Fatalf("malformed query added %d nodes", got)
			}
		})
	}
}

// TestAnnouncePeerWithInvalidTokenDoesNotDisturbExistingNode verifies a
// token-less announce_peer claiming a known node ID from a foreign address
// leaves that node's bucket position, address and LastSeen untouched, and never
// starts an address-change verification.
func TestAnnouncePeerWithInvalidTokenDoesNotDisturbExistingNode(t *testing.T) {
	const bucket = 18
	d, conn := newFakeDHT(t)

	victimAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 6881}
	attackerAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 1}
	victim := idInBucket(d.nodeID, bucket, 2)

	d.addNode(idInBucket(d.nodeID, bucket, 1), &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6881})
	d.addNode(victim, victimAddr)
	d.addNode(idInBucket(d.nodeID, bucket, 3), &net.UDPAddr{IP: net.ParseIP("10.0.0.3"), Port: 6881})

	past := time.Now().Add(-time.Hour)
	backdateNode(t, d, bucket, victim, past)

	var infoHash [20]byte
	copy(infoHash[:], "info-hash-for-test--")
	d.handleQuery("tx", "announce_peer", map[string]interface{}{
		"id":        string(victim[:]),
		"info_hash": string(infoHash[:]),
		"token":     "not-a-valid-token",
		"port":      int64(51413),
	}, attackerAddr)

	nodes := bucketNodes(d, bucket)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes in bucket %d, got %d", bucket, len(nodes))
	}
	if nodes[1].ID != victim {
		t.Fatalf("victim moved within the bucket: index 1 holds %x", nodes[1].ID)
	}
	if !sameUDPAddr(nodes[1].Addr, victimAddr) {
		t.Fatalf("victim was re-pointed to %s, want %s", nodes[1].Addr, victimAddr)
	}
	if !nodes[1].LastSeen.Equal(past) {
		t.Fatalf("victim LastSeen was refreshed by an unauthenticated announce: %v", nodes[1].LastSeen)
	}
	if got := pendingAddrChangeCount(d); got != 0 {
		t.Fatalf("an unauthenticated announce started %d address-change verifications", got)
	}
	if got := conn.queriesTo(victimAddr, "ping"); got != 0 {
		t.Fatalf("an unauthenticated announce emitted %d probes to the stored address", got)
	}
}

package dht

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"net"
	"os"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
)

func TestXORDistanceAndBucketIndex(t *testing.T) {
	id1 := sha1.Sum([]byte("node1"))
	id2 := sha1.Sum([]byte("node2"))

	dist := xorDistance(id1, id2)
	var expectedDist [20]byte
	for i := 0; i < 20; i++ {
		expectedDist[i] = id1[i] ^ id2[i]
	}

	if dist != expectedDist {
		t.Errorf("incorrect XOR distance: expected %x, got %x", expectedDist, dist)
	}

	// Test bucketIndex with exact equal IDs
	if idx := bucketIndex(id1, id1); idx != 159 {
		t.Errorf("expected bucket index 159 for identical IDs, got %d", idx)
	}

	// Test bucket index for slightly differing IDs
	id3 := id1
	id3[19] ^= 1 // toggle last bit
	if idx := bucketIndex(id1, id3); idx != 159 {
		t.Errorf("expected bucket index 159 (last bit difference), got %d", idx)
	}

	id4 := id1
	id4[0] ^= 128 // toggle first bit
	if idx := bucketIndex(id1, id4); idx != 0 {
		t.Errorf("expected bucket index 0 (first bit difference), got %d", idx)
	}
}

func TestCompactNodes(t *testing.T) {
	id1 := sha1.Sum([]byte("node1"))
	addr1 := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6881}

	nodes := []Node{
		{ID: id1, Addr: addr1},
	}

	compacted := compactNodes(nodes)
	if len(compacted) != 26 {
		t.Fatalf("expected compacted length to be 26, got %d", len(compacted))
	}

	parsed := parseCompactNodes(compacted)
	if len(parsed) != 1 {
		t.Fatalf("expected 1 parsed node, got %d", len(parsed))
	}

	if parsed[0].ID != id1 {
		t.Errorf("parsed ID mismatch: expected %x, got %x", id1, parsed[0].ID)
	}

	if !parsed[0].Addr.IP.Equal(addr1.IP) || parsed[0].Addr.Port != addr1.Port {
		t.Errorf("parsed address mismatch: expected %s, got %s", addr1, parsed[0].Addr)
	}
}

func TestDHTPersistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dht_persistence_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dht1, err := NewDHT(tempDir, 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}

	id1 := sha1.Sum([]byte("fake_node_1"))
	addr1 := &net.UDPAddr{IP: net.ParseIP("192.168.1.100"), Port: 5000}
	dht1.addNode(id1, addr1)

	dht1.Close() // this should save nodes to state file

	// Create a new DHT instance on the same directory
	dht2, err := NewDHT(tempDir, 0)
	if err != nil {
		t.Fatalf("failed to start second DHT: %v", err)
	}
	defer dht2.Close()

	if dht2.nodeID != dht1.nodeID {
		t.Errorf("node ID was not persisted across runs: %x vs %x", dht1.nodeID, dht2.nodeID)
	}

	closer := dht2.getCloserNodes(id1, 1)
	if len(closer) != 1 || closer[0].ID != id1 {
		t.Errorf("routing table nodes were not persisted successfully")
	}
}

func TestDHTRoutingTableLimits(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dht_limits_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	d, err := NewDHT(tempDir, 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	// Fill bucket 0 with 10 dummy nodes. Since bucket size limit is 8,
	// only up to 8 nodes should be stored.
	for i := 0; i < 10; i++ {
		// Generate an ID that differs from d.nodeID at the first bit so it falls in bucket 0
		var id [20]byte
		copy(id[:], d.nodeID[:])
		id[0] ^= 128
		id[19] ^= byte(i + 1) // ensure unique ID

		addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1000 + i}
		d.addNode(id, addr)
	}

	closer := d.getCloserNodes(d.nodeID, 20)
	if len(closer) > 8 {
		t.Errorf("k-bucket constraint violated: found %d nodes, expected max 8", len(closer))
	}
}

func TestDHTResponseTransactionValidatesSourceAddress(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	ch := make(chan interface{}, 1)
	expected := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 10001}
	spoofed := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 10002}
	d.registerTransaction("aa", expected, ch)
	defer d.unregisterTransaction("aa")

	d.handleResponse("aa", map[string]interface{}{"id": string(d.nodeID[:])}, spoofed)
	select {
	case <-ch:
		t.Fatal("spoofed response from wrong address was accepted")
	default:
	}

	d.handleResponse("aa", map[string]interface{}{"id": string(d.nodeID[:])}, expected)
	select {
	case <-ch:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected response from registered address to be accepted")
	}
}

func TestDHTAnnouncePeerValidatesPort(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5000}
	infoHash := sha1.Sum([]byte("torrent"))
	nodeID := sha1.Sum([]byte("node"))
	baseArgs := map[string]interface{}{
		"id":        string(nodeID[:]),
		"info_hash": string(infoHash[:]),
		"token":     d.generateToken(addr),
	}

	args := make(map[string]interface{})
	for k, v := range baseArgs {
		args[k] = v
	}
	args["port"] = int64(70000)
	d.handleQuery("tx", "announce_peer", args, addr)
	if peers := d.getPeersForInfoHash(infoHash); len(peers) != 0 {
		t.Fatalf("expected invalid port announce to be ignored, got %d peers", len(peers))
	}

	args = make(map[string]interface{})
	for k, v := range baseArgs {
		args[k] = v
	}
	args["port"] = int64(51413)
	d.handleQuery("tx", "announce_peer", args, addr)
	peers := d.getPeersForInfoHash(infoHash)
	if len(peers) != 1 {
		t.Fatalf("expected valid announce to register peer, got %d", len(peers))
	}
	cp, ok := peers[0].(string)
	if !ok || len(cp) != 6 {
		t.Fatalf("unexpected compact peer value: %#v", peers[0])
	}
	if got := binary.BigEndian.Uint16([]byte(cp[4:6])); got != 51413 {
		t.Fatalf("expected registered port 51413, got %d", got)
	}
}

func TestDHTGetPeersParsesValuesAndNodesTogether(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("failed to listen UDP: %v", err)
	}
	defer server.Close()

	nodeID := sha1.Sum([]byte("node"))
	peerCompact := string([]byte{127, 0, 0, 1, 0x1a, 0xe1})
	nodesCompact := compactNodes([]Node{{ID: sha1.Sum([]byte("other")), Addr: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000}}})

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 2048)
		n, addr, err := server.ReadFromUDP(buf)
		if err != nil {
			return
		}
		parsed, err := bencode.Unmarshal(buf[:n])
		if err != nil {
			return
		}
		request := parsed.(map[string]interface{})
		txn := request["t"].(string)
		payload, err := bencode.Marshal(map[string]interface{}{
			"t": txn,
			"y": "r",
			"r": map[string]interface{}{
				"id":     string(nodeID[:]),
				"token":  "tok",
				"values": []interface{}{peerCompact},
				"nodes":  nodesCompact,
			},
		})
		if err == nil {
			_, _ = server.WriteToUDP(payload, addr)
		}
	}()

	ctx, cancel := contextWithTestTimeout()
	defer cancel()
	res, err := d.getPeersQuery(ctx, sha1.Sum([]byte("torrent")), server.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("getPeersQuery failed: %v", err)
	}
	<-done

	if len(res.Peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(res.Peers))
	}
	if len(res.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(res.Nodes))
	}
}

func contextWithTestTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

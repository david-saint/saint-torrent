package dht

import (
	"crypto/sha1"
	"net"
	"testing"
)

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

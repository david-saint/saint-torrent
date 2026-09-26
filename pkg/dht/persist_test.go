package dht

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"sainttorrent/pkg/bencode"
)

// writeNodesFile writes a .dht_nodes file in the format older versions saved,
// including their persisted node_id.
func writeNodesFile(t *testing.T, path string, nodeID [20]byte, contacts map[[20]byte]string) {
	t.Helper()
	var list []interface{}
	for id, addr := range contacts {
		list = append(list, map[string]interface{}{"id": string(id[:]), "addr": addr})
	}
	data, err := bencode.Marshal(map[string]interface{}{"node_id": string(nodeID[:]), "nodes": list})
	if err != nil {
		t.Fatalf("failed to encode nodes file: %v", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("failed to write nodes file: %v", err)
	}
}

// TestSavedNodeIDIsIgnored verifies a node_id left by an older version is not
// adopted, while its saved contacts are still used to bootstrap.
func TestSavedNodeIDIsIgnored(t *testing.T) {
	dir := t.TempDir()
	var oldID [20]byte
	copy(oldID[:], "persisted-node-id!!!")
	var contact [20]byte
	copy(contact[:], "saved-contact-id----")
	writeNodesFile(t, filepath.Join(dir, ".dht_nodes"), oldID, map[[20]byte]string{contact: "198.51.100.50:6881"})

	d, err := NewDHTWithConn(dir, newFakeConn())
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	if d.nodeID == oldID {
		t.Fatal("the node ID saved by an earlier run was reused")
	}
	if !d.HasNodeAddress(net.ParseIP("198.51.100.50"), 6881) {
		t.Fatal("saved contacts were not loaded")
	}
}

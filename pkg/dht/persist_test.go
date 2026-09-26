package dht

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
)

// writeLegacyNodesFile writes a .dht_nodes file in the format older versions
// saved, including their persisted node_id.
func writeLegacyNodesFile(t *testing.T, path string, nodeID [20]byte, contacts map[[20]byte]string) {
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
	writeLegacyNodesFile(t, filepath.Join(dir, ".dht_nodes"), oldID, map[[20]byte]string{contact: "198.51.100.50:6881"})

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

// servedNodes returns the contacts carried by our response with transaction tid.
func servedNodes(t *testing.T, c *fakeConn, tid string) []Node {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.sent {
		parsed, err := bencode.Unmarshal(p.data)
		if err != nil {
			continue
		}
		dict, ok := parsed.(map[string]interface{})
		if !ok || dict["y"] != "r" || dict["t"] != tid {
			continue
		}
		r, _ := dict["r"].(map[string]interface{})
		nodes, _ := r["nodes"].(string)
		served, ok := parseCompactNodes(nodes)
		if !ok {
			t.Fatalf("response %q carries a malformed node list", tid)
		}
		return served
	}
	t.Fatalf("no response with transaction %q was sent", tid)
	return nil
}

// TestSavedContactsAreNotServedUntilHeardFrom verifies contacts loaded from a
// nodes file, which may predate one-contact-per-IP admission or be planted,
// still seed our own lookups but are not handed to other nodes until they
// answer us (a query from their address is not enough), and that addresses
// that can never be a node are not loaded.
func TestSavedContactsAreNotServedUntilHeardFrom(t *testing.T) {
	dir := t.TempDir()
	var contact, multicast, broadcast, zero [20]byte
	copy(contact[:], "saved-contact-id----")
	copy(multicast[:], "saved-multicast-id--")
	copy(broadcast[:], "saved-broadcast-id--")
	copy(zero[:], "saved-zero-net-id---")
	addr := &net.UDPAddr{IP: net.ParseIP("198.51.100.51"), Port: 6881}
	writeLegacyNodesFile(t, filepath.Join(dir, nodesFileName), contact, map[[20]byte]string{
		contact:   addr.String(),
		multicast: "224.0.0.251:5353",
		broadcast: "255.255.255.255:6881",
		zero:      "0.0.0.0:6881",
	})
	conn := newFakeConn()
	d, err := NewDHTWithConn(dir, conn)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	if got := d.NodesCount(); got != 1 || !d.HasNodeAddress(addr.IP, uint16(addr.Port)) {
		t.Fatalf("want only the unicast contact loaded, table holds %d", got)
	}
	if seeds := d.getCloserNodes(contact, 8); len(seeds) != 1 || seeds[0].ID != contact {
		t.Fatalf("a saved contact no longer seeds our own lookups: %v", seeds)
	}

	asker := &net.UDPAddr{IP: net.ParseIP("203.0.113.40"), Port: 6881}
	askerID := idInBucket(d.nodeID, 5, 1)
	ask := func(tid string) []Node {
		d.handleQuery(tid+"f", "find_node", map[string]interface{}{"id": string(askerID[:]), "target": string(contact[:])}, asker)
		d.handleQuery(tid+"g", "get_peers", map[string]interface{}{"id": string(askerID[:]), "info_hash": string(contact[:])}, asker)
		return append(servedNodes(t, conn, tid+"f"), servedNodes(t, conn, tid+"g")...)
	}
	if served := ask("a"); len(served) != 0 {
		t.Fatalf("a saved contact not heard from this run was handed out: %x", served[0].ID)
	}

	// A query claiming to come from it proves nothing, since UDP is easily
	// spoofed; it only makes us ping the contact.
	d.handleQuery("sp", "ping", map[string]interface{}{"id": string(contact[:])}, addr)
	if served := ask("b"); len(served) != 0 {
		t.Fatal("a spoofed query from a saved contact's address got it handed out")
	}
	ping := awaitQueryTo(t, conn, addr, "ping")

	// It answers our ping, so it may be handed out again.
	conn.injectPingReply(t, ping, contact, addr)
	deadline := time.After(5 * time.Second)
	for seeds := d.getCloserNodes(contact, 1); seeds[0].LastSeen.IsZero(); seeds = d.getCloserNodes(contact, 1) {
		select {
		case <-deadline:
			t.Fatal("the saved contact's answer was never recorded")
		case <-time.After(2 * time.Millisecond):
		}
	}
	served := ask("c")
	if len(served) != 2 || served[0].ID != contact || served[1].ID != contact {
		t.Fatalf("a saved contact that answered us is still withheld: %v", served)
	}
}

// symlinkOrSkip creates link -> target, skipping where symlinks need privileges.
func symlinkOrSkip(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
}

// TestSaveNodesReplacesPlantedSymlink is the reported clobber: another user
// links .dht_nodes in a shared download directory to a file of ours. Saving
// must replace the link with our own private file, never write through it.
func TestSaveNodesReplacesPlantedSymlink(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(t.TempDir(), "bashrc")
	if err := os.WriteFile(victim, []byte("precious"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, nodesFileName)
	symlinkOrSkip(t, victim, path)

	d, err := NewDHTWithConn(dir, newFakeConn())
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	d.addNode(idInBucket(d.nodeID, 7, 1), &net.UDPAddr{IP: net.ParseIP("198.51.100.60"), Port: 6881})
	d.Close()

	if got, err := os.ReadFile(victim); err != nil || string(got) != "precious" {
		t.Fatalf("saving wrote through the planted symlink: %q, %v", got, err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("nodes file missing after save: %v", err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("nodes file is still %v after save", info.Mode())
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
		t.Fatalf("nodes file mode is %v, want 0600", info.Mode().Perm())
	}

	reloaded, err := NewDHTWithConn(dir, newFakeConn())
	if err != nil {
		t.Fatalf("failed to restart DHT: %v", err)
	}
	defer reloaded.Close()
	if !reloaded.HasNodeAddress(net.ParseIP("198.51.100.60"), 6881) {
		t.Fatal("the replaced nodes file did not round-trip")
	}
}

// TestLoadNodesIgnoresSymlink verifies a .dht_nodes symlink is not followed on
// load, so a planted file elsewhere cannot seed our routing table.
func TestLoadNodesIgnoresSymlink(t *testing.T) {
	dir := t.TempDir()
	planted := filepath.Join(t.TempDir(), "planted")
	var contact [20]byte
	copy(contact[:], "planted-contact-id--")
	writeLegacyNodesFile(t, planted, contact, map[[20]byte]string{contact: "198.51.100.61:6881"})
	symlinkOrSkip(t, planted, filepath.Join(dir, nodesFileName))

	d, err := NewDHTWithConn(dir, newFakeConn())
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()
	if d.HasNodeAddress(net.ParseIP("198.51.100.61"), 6881) {
		t.Fatal("contacts were loaded through a symlink")
	}
}

// TestSaveNodesLeavesNoTempFiles verifies the atomic write cleans up after
// itself and leaves only the nodes file behind.
func TestSaveNodesLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	d, err := NewDHTWithConn(dir, newFakeConn())
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	d.addNode(idInBucket(d.nodeID, 7, 1), &net.UDPAddr{IP: net.ParseIP("198.51.100.62"), Port: 6881})
	d.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != nodesFileName {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("download dir holds %v after save, want only %s", names, nodesFileName)
	}
}

// TestLoadNodesDoesNotResolveHostnames verifies a hostname in the nodes file
// is skipped rather than resolved at startup.
func TestLoadNodesDoesNotResolveHostnames(t *testing.T) {
	dir := t.TempDir()
	var a, b [20]byte
	copy(a[:], "hostname-contact----")
	copy(b[:], "literal-contact-----")
	writeLegacyNodesFile(t, filepath.Join(dir, nodesFileName), a, map[[20]byte]string{
		a: "localhost:6881",
		b: "198.51.100.63:6881",
	})
	d, err := NewDHTWithConn(dir, newFakeConn())
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()
	if got := d.NodesCount(); got != 1 || !d.HasNodeAddress(net.ParseIP("198.51.100.63"), 6881) {
		t.Fatalf("want only the literal contact loaded, table holds %d", got)
	}
}

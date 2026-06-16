package dht

import (
	"net"
	"testing"
	"time"
)

func init() {
	DefaultBootstrapHosts = nil
}

// TestAddNodeGrowsRoutingTable verifies that AddNode — the entry point used when
// a BitTorrent peer advertises its DHT port via a BEP 5 PORT message — pings the
// advertised endpoint, learns its node ID, and inserts it into the routing table.
// Two live DHT instances talk over loopback UDP so the full ping/response path is
// exercised.
func TestAddNodeGrowsRoutingTable(t *testing.T) {
	receiver, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start receiver DHT: %v", err)
	}
	defer receiver.Close()

	adder, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start adder DHT: %v", err)
	}
	defer adder.Close()

	if adder.NodesCount() != 0 {
		t.Fatalf("expected empty routing table at start, got %d nodes", adder.NodesCount())
	}

	receiverPort := receiver.Port()
	if receiverPort == 0 {
		t.Fatal("receiver DHT has no listen port")
	}

	// Simulate ingesting a PORT message: feed the receiver's loopback endpoint.
	adder.AddNode(net.ParseIP("127.0.0.1"), receiverPort)

	deadline := time.After(3 * time.Second)
	for {
		if adder.NodesCount() > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("routing table did not grow after AddNode")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// The inserted node must be the receiver we pinged.
	nodes := adder.getCloserNodes(adder.nodeID, 8)
	found := false
	for _, n := range nodes {
		if n.Addr != nil && n.Addr.IP.Equal(net.ParseIP("127.0.0.1")) && n.Addr.Port == int(receiverPort) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected receiver %d in routing table, got %d nodes (none matching)", receiverPort, len(nodes))
	}
}

// TestAddNodeRejectsInvalidInput verifies AddNode ignores unusable endpoints
// (zero port, non-IPv4 address) without growing the table or blocking.
func TestAddNodeRejectsInvalidInput(t *testing.T) {
	d, err := NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start DHT: %v", err)
	}
	defer d.Close()

	d.AddNode(net.ParseIP("127.0.0.1"), 0)      // zero port
	d.AddNode(nil, 6881)                        // nil IP
	d.AddNode(net.ParseIP("::1"), 6881)         // IPv6 (socket is IPv4-bound)
	d.AddNode(net.ParseIP("203.0.113.5"), 6881) // valid but unreachable; no reply

	// Give the unreachable probe a moment; it must not insert anything.
	time.Sleep(100 * time.Millisecond)
	if c := d.NodesCount(); c != 0 {
		t.Errorf("expected no nodes added from invalid/unreachable input, got %d", c)
	}
}

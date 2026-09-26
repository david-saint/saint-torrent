package dht

import (
	"net"
	"testing"
)

func idString(id [20]byte) string { return string(id[:]) }

// TestLookupStoresResponderUnderReportedID is the reported forgery: a referrer
// pairs a live node's address with an ID next to the info-hash. The node must
// be stored under the ID it reports itself, and the forged ID never.
func TestLookupStoresResponderUnderReportedID(t *testing.T) {
	d, conn := newFakeDHT(t)

	var infoHash [20]byte
	copy(infoHash[:], "lookup-target-hash--")
	start := &net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 6881}
	startID := idInBucket(d.nodeID, 3, 1)
	honest := &net.UDPAddr{IP: net.ParseIP("203.0.113.2"), Port: 6881}
	honestID := idInBucket(d.nodeID, 50, 1)
	forged := infoHash
	forged[19] ^= 1

	conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		switch {
		case q == "get_peers" && sameUDPAddr(to, start):
			return map[string]interface{}{
				"id":    idString(startID),
				"nodes": compactNodes([]Node{{ID: forged, Addr: honest}}),
			}
		case q == "get_peers" && sameUDPAddr(to, honest):
			return map[string]interface{}{"id": idString(honestID)}
		}
		return nil
	})
	d.addNode(startID, start)

	d.lookup(infoHash, 0, LookupOptions{})

	if storedAddrFor(d, forged) != nil {
		t.Fatal("the responder was stored under the ID its referrer claimed")
	}
	if addr := storedAddrFor(d, honestID); addr == nil || !sameUDPAddr(addr, honest) {
		t.Fatalf("the responder was not stored under its own ID: %v", addr)
	}
	if closest := d.getCloserNodes(infoHash, 1); len(closest) == 1 && closest[0].ID == forged {
		t.Fatal("the forged ID became our closest contact to the info-hash")
	}
}

// TestLookupIgnoresResponsesWithoutValidID verifies a get_peers answer without
// a 20-byte responder ID is discarded rather than stored under a mangled ID.
func TestLookupIgnoresResponsesWithoutValidID(t *testing.T) {
	d, conn := newFakeDHT(t)

	var infoHash [20]byte
	copy(infoHash[:], "lookup-target-hash--")
	start := &net.UDPAddr{IP: net.ParseIP("203.0.113.3"), Port: 6881}
	startID := idInBucket(d.nodeID, 4, 1)
	conn.setAnswer(func(to *net.UDPAddr, q string, _ map[string]interface{}) map[string]interface{} {
		if q == "get_peers" {
			return map[string]interface{}{"id": "short"}
		}
		return nil
	})
	d.addNode(startID, start)
	d.lookup(infoHash, 0, LookupOptions{})

	if got := d.NodesCount(); got != 1 || storedAddrFor(d, startID) == nil {
		t.Fatalf("a response without a valid ID changed the table: %d contacts", got)
	}
}

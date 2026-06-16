package dht

import "testing"

// FuzzParseCompactNodes ensures the compact node-list parser used on DHT
// find_node/get_peers responses never panics and yields one node per 26 bytes
// with a non-nil address.
func FuzzParseCompactNodes(f *testing.F) {
	f.Add("")
	f.Add(string(make([]byte, 26)))
	f.Add(string(make([]byte, 52)))
	f.Add(string(make([]byte, 30))) // not a multiple of 26
	f.Fuzz(func(t *testing.T, s string) {
		nodes := parseCompactNodes(s)
		if want := len(s) / 26; len(nodes) != want {
			t.Fatalf("expected %d nodes from %d bytes, got %d", want, len(s), len(nodes))
		}
		for _, n := range nodes {
			if n.Addr == nil {
				t.Fatalf("parsed node with nil addr")
			}
		}
	})
}

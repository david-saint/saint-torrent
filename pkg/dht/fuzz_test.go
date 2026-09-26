package dht

import (
	"strings"
	"testing"
)

// FuzzParseCompactNodes ensures the compact node-list parser used on DHT
// find_node/get_peers responses never panics, rejects a list whose length is
// not a multiple of 26, and otherwise yields one node per 26-byte entry with a
// non-zero ID, each with a non-nil address.
func FuzzParseCompactNodes(f *testing.F) {
	f.Add("")
	f.Add(string(make([]byte, 26)))
	f.Add(string(make([]byte, 52)))
	f.Add(string(make([]byte, 30))) // not a multiple of 26
	f.Add(strings.Repeat("n", 26))
	f.Fuzz(func(t *testing.T, s string) {
		nodes, ok := parseCompactNodes(s)
		if len(s)%26 != 0 {
			if ok || nodes != nil {
				t.Fatalf("a %d-byte list was accepted", len(s))
			}
			return
		}
		if !ok {
			t.Fatalf("a %d-byte list was rejected", len(s))
		}
		want := 0
		for i := 0; i < len(s); i += 26 {
			if s[i:i+20] != strings.Repeat("\x00", 20) {
				want++
			}
		}
		if len(nodes) != want {
			t.Fatalf("expected %d nodes from %d bytes, got %d", want, len(s), len(nodes))
		}
		for _, n := range nodes {
			if n.Addr == nil || n.ID == ([20]byte{}) {
				t.Fatalf("parsed node with nil addr or zero ID")
			}
		}
	})
}

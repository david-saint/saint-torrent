package netpolicy

import (
	"net/netip"
	"testing"
)

func TestClassify(t *testing.T) {
	cases := map[string]Scope{
		"0.0.0.0":          ScopeInvalid,
		"0.1.2.3":          ScopeInvalid,
		"255.255.255.255":  ScopeInvalid,
		"240.0.0.1":        ScopeInvalid,
		"224.0.0.1":        ScopeInvalid,
		"::":               ScopeInvalid,
		"ff02::1":          ScopeInvalid,
		"127.0.0.1":        ScopeLoopback,
		"127.9.9.9":        ScopeLoopback,
		"::1":              ScopeLoopback,
		"::ffff:127.0.0.1": ScopeLoopback,
		"169.254.169.254":  ScopeLinkLocal,
		"fe80::1":          ScopeLinkLocal,
		"10.0.0.1":         ScopePrivate,
		"172.16.5.4":       ScopePrivate,
		"192.168.1.1":      ScopePrivate,
		"fd00::1":          ScopePrivate,
		"::ffff:10.1.1.1":  ScopePrivate,
		"8.8.8.8":          ScopeGlobal,
		"2001:4860::8888":  ScopeGlobal,
		"100.64.0.1":       ScopeGlobal,
	}
	for s, want := range cases {
		if got := Classify(netip.MustParseAddr(s)); got != want {
			t.Errorf("Classify(%s) = %d, want %d", s, got, want)
		}
	}
	if got := Classify(netip.Addr{}); got != ScopeInvalid {
		t.Errorf("Classify(zero) = %d, want invalid", got)
	}
}

func TestPeerAllowed(t *testing.T) {
	public := netip.MustParseAddr("203.0.113.7")
	lan := netip.MustParseAddr("192.168.1.20")
	loop := netip.MustParseAddr("127.0.0.1")
	unknown := netip.Addr{}

	cases := []struct {
		peer   string
		source netip.Addr
		want   bool
	}{
		{"198.51.100.1:6881", public, true},
		{"198.51.100.1:6881", unknown, true},
		{"198.51.100.1:443", public, true}, // privileged ports stay allowed
		{"198.51.100.1:0", public, false},
		{"127.0.0.1:22", public, false},
		{"127.0.0.1:22", unknown, false},
		{"127.0.0.1:6881", loop, true},
		{"[::ffff:127.0.0.1]:6881", public, false},
		{"192.168.1.1:80", public, false},
		{"192.168.1.1:6881", lan, true},
		{"192.168.1.1:6881", loop, true},
		{"169.254.169.254:80", public, false},
		{"127.0.0.1:6881", lan, false},
		{"224.0.0.1:6881", loop, false},
		{"255.255.255.255:6881", loop, false},
		{"0.0.0.0:6881", loop, false},
	}
	for _, c := range cases {
		if got := PeerAllowed(netip.MustParseAddrPort(c.peer), c.source); got != c.want {
			t.Errorf("PeerAllowed(%s, %v) = %v, want %v", c.peer, c.source, got, c.want)
		}
	}
}

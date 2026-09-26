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
		// Carrier-grade NAT (RFC 6598) is private, as in libtorrent's
		// is_local: it holds ISP-internal services and Tailscale tailnets.
		"100.64.0.1":        ScopePrivate,
		"100.100.100.100":   ScopePrivate,
		"100.127.255.254":   ScopePrivate,
		"::ffff:100.64.0.1": ScopePrivate,
		"100.63.255.255":    ScopeGlobal,
		"100.128.0.0":       ScopeGlobal,
		// fec0::/10 is deprecated site-local space.
		"fec0::1": ScopePrivate,
		"feff::1": ScopePrivate,
		"febf::1": ScopeLinkLocal,
		// IPv6 forms a translator or tunnel turns into IPv4 are judged by
		// the IPv4 address they carry: NAT64 (well-known and local-use
		// prefixes), SIIT and 6to4.
		"64:ff9b::7f00:1":         ScopeLoopback,
		"64:ff9b::a00:1":          ScopePrivate,
		"64:ff9b::6440:1":         ScopePrivate,
		"64:ff9b::a9fe:a9fe":      ScopeLinkLocal,
		"64:ff9b::":               ScopeInvalid,
		"64:ff9b::e000:1":         ScopeInvalid,
		"64:ff9b::808:808":        ScopeGlobal,
		"64:ff9b:1::a00:1":        ScopePrivate,
		"64:ff9b:1:abcd::c0a8:1":  ScopePrivate,
		"64:ff9b:1:abcd::808:808": ScopeGlobal,
		"::ffff:0:a00:1":          ScopePrivate,
		"::ffff:0:7f00:1":         ScopeLoopback,
		"::ffff:0:808:808":        ScopeGlobal,
		"2002:7f00:1::":           ScopeLoopback,
		"2002:a00:1::1":           ScopePrivate,
		"2002:a9fe:a9fe::1":       ScopeLinkLocal,
		"2002:808:808::1":         ScopeGlobal,
		// The deprecated IPv4-compatible form is no peer at all.
		"::7f00:1":  ScopeInvalid,
		"::a00:1":   ScopeInvalid,
		"::808:808": ScopeInvalid,
		"::2":       ScopeInvalid,
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
	cgnat := netip.MustParseAddr("100.101.102.103")
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
		// A public DHT node, PEX sender or tracker cannot point at CGNAT
		// space; a LAN or CGNAT source can, as with RFC 1918 addresses.
		{"100.64.1.2:6881", public, false},
		{"100.64.1.2:6881", unknown, false},
		{"100.64.1.2:6881", lan, true},
		{"100.64.1.2:6881", loop, true},
		{"100.64.1.2:6881", cgnat, true},
		{"192.168.1.1:6881", cgnat, true},
		{"127.0.0.1:6881", cgnat, false},
		{"[fec0::2]:6881", public, false},
		{"[fec0::2]:6881", lan, true},
		// A public source cannot reach LAN or loopback through the IPv6
		// forms of their addresses; public IPv4 behind NAT64 stays reachable.
		{"[64:ff9b::a00:5]:6881", public, false},
		{"[64:ff9b::a00:5]:6881", unknown, false},
		{"[64:ff9b::a00:5]:6881", lan, true},
		{"[64:ff9b:1::a00:5]:6881", public, false},
		{"[64:ff9b::7f00:1]:6881", lan, false},
		{"[64:ff9b::808:808]:6881", public, true},
		{"[2002:a00:1::1]:6881", public, false},
		{"[::ffff:0:a00:1]:6881", public, false},
		{"[::a00:1]:6881", lan, false},
	}
	for _, c := range cases {
		if got := PeerAllowed(netip.MustParseAddrPort(c.peer), c.source); got != c.want {
			t.Errorf("PeerAllowed(%s, %v) = %v, want %v", c.peer, c.source, got, c.want)
		}
	}
}

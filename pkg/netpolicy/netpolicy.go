// Package netpolicy classifies network endpoints learned from untrusted
// sources — trackers, DHT nodes, PEX messages, and URLs inside torrents — so
// a remote party cannot aim the client's connections at loopback or LAN
// services (SSRF), or at addresses that can never be a real peer. "LAN" here
// includes carrier-grade NAT space (100.64.0.0/10), which holds ISP-internal
// services and Tailscale tailnets and is unreachable from the internet.
//
// The rule mirrors libtorrent's ssrf_mitigation: an endpoint may be at most as
// "local" as whoever told us about it. A loopback tracker may hand out
// loopback peers (local test setups), a LAN peer may gossip LAN peers, but a
// public tracker, DHT node, or PEX sender can only hand out public addresses.
// Privileged ports stay allowed, matching libtorrent's default
// no_connect_privileged_ports=false, because some real peers listen on them.
package netpolicy

import "net/netip"

// Scope is the reachability class of an address.
type Scope uint8

const (
	// ScopeInvalid covers addresses that are never a unicast peer:
	// unspecified, multicast, broadcast, 0.0.0.0/8, 240.0.0.0/4 and the
	// deprecated IPv4-compatible ::a.b.c.d.
	ScopeInvalid Scope = iota
	// ScopeLoopback is 127.0.0.0/8 and ::1.
	ScopeLoopback
	// ScopeLinkLocal is 169.254.0.0/16 (including cloud metadata
	// endpoints) and fe80::/10.
	ScopeLinkLocal
	// ScopePrivate is RFC 1918, 100.64.0.0/10 carrier-grade NAT (RFC 6598,
	// also Tailscale's address space), fc00::/7 unique-local and fec0::/10
	// deprecated site-local space, as libtorrent's is_local counts them.
	ScopePrivate
	// ScopeGlobal is everything else.
	ScopeGlobal
)

var (
	cgnatPrefix     = netip.MustParsePrefix("100.64.0.0/10")
	siteLocalPrefix = netip.MustParsePrefix("fec0::/10")

	// IPv6 blocks whose addresses carry an IPv4 address that a translator or
	// tunnel on the path may deliver to (see embeddedIPv4).
	nat64Prefix      = netip.MustParsePrefix("64:ff9b::/96")    // RFC 6052 well-known prefix
	nat64LocalPrefix = netip.MustParsePrefix("64:ff9b:1::/48")  // RFC 8215 local-use
	siitPrefix       = netip.MustParsePrefix("::ffff:0:0:0/96") // RFC 2765 IPv4-translated
	sixToFourPrefix  = netip.MustParsePrefix("2002::/16")       // RFC 3056 6to4
	v4CompatPrefix   = netip.MustParsePrefix("::/96")           // RFC 4291 IPv4-compatible
)

// Classify returns the scope of a. IPv4-mapped IPv6 addresses are classified
// as the IPv4 address they carry, so ::ffff:127.0.0.1 is loopback, and so are
// the IPv6 forms a translator or tunnel turns into IPv4 (see embeddedIPv4):
// 64:ff9b::10.0.0.5 is private.
func Classify(a netip.Addr) Scope {
	a = a.Unmap()
	if v4, ok := embeddedIPv4(a); ok {
		return Classify(v4)
	}
	if !a.IsValid() || a.IsUnspecified() || a.IsMulticast() {
		return ScopeInvalid
	}
	// The rest of ::/96 besides :: and ::1 is the deprecated IPv4-compatible
	// form (RFC 4291), which no real peer uses.
	if v4CompatPrefix.Contains(a.WithZone("")) && !a.IsLoopback() {
		return ScopeInvalid
	}
	if a.Is4() {
		if b := a.As4(); b[0] == 0 || b[0] >= 240 {
			return ScopeInvalid
		}
	}
	switch {
	case a.IsLoopback():
		return ScopeLoopback
	case a.IsLinkLocalUnicast():
		return ScopeLinkLocal
	case a.IsPrivate(), cgnatPrefix.Contains(a), siteLocalPrefix.Contains(a):
		return ScopePrivate
	}
	return ScopeGlobal
}

// embeddedIPv4 returns the IPv4 address that a, an IPv6 address, carries when
// it lies in a block a translator or tunnel on the path may turn into IPv4
// traffic to that address: NAT64's well-known prefix 64:ff9b::/96 (RFC 6052)
// and local-use 64:ff9b:1::/48 (RFC 8215, read with the IPv4 address in the
// low 32 bits, as under the /96 prefixes it is deployed with), SIIT's
// IPv4-translated ::ffff:0:a.b.c.d (RFC 2765), and 6to4's 2002:aabb:ccdd::/48
// (RFC 3056). Judged as global, such an address would let a torrent, tracker
// or PEX sender reach loopback or LAN services through, say, a NAT64 gateway
// that forwards to private IPv4, as cloud NAT gateways do. A network-specific
// NAT64 prefix cannot be told from the address and stays out of reach here.
func embeddedIPv4(a netip.Addr) (netip.Addr, bool) {
	if !a.Is6() {
		return netip.Addr{}, false
	}
	a = a.WithZone("")
	b := a.As16()
	switch {
	case nat64Prefix.Contains(a), nat64LocalPrefix.Contains(a), siitPrefix.Contains(a):
		return netip.AddrFrom4([4]byte(b[12:16])), true
	case sixToFourPrefix.Contains(a):
		return netip.AddrFrom4([4]byte(b[2:6])), true
	}
	return netip.Addr{}, false
}

// IsLocal reports whether a is loopback, link-local, or private.
func IsLocal(a netip.Addr) bool {
	switch Classify(a) {
	case ScopeLoopback, ScopeLinkLocal, ScopePrivate:
		return true
	}
	return false
}

// PeerAllowed reports whether a peer endpoint learned from source may be
// dialed. source is the address of whoever supplied the endpoint (the
// tracker, DHT node, or PEX sender); pass the zero netip.Addr when the source
// is unknown or remote-controlled (for example a magnet link), which is
// treated as public.
func PeerAllowed(peer netip.AddrPort, source netip.Addr) bool {
	if peer.Port() == 0 {
		return false
	}
	switch Classify(peer.Addr()) {
	case ScopeGlobal:
		return true
	case ScopeLoopback:
		return Classify(source) == ScopeLoopback
	case ScopeLinkLocal, ScopePrivate:
		return IsLocal(source)
	}
	return false
}

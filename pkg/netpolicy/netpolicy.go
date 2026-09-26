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
	// unspecified, multicast, broadcast, 0.0.0.0/8 and 240.0.0.0/4.
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
)

// Classify returns the scope of a. IPv4-mapped IPv6 addresses are classified
// as the IPv4 address they carry, so ::ffff:127.0.0.1 is loopback.
func Classify(a netip.Addr) Scope {
	a = a.Unmap()
	if !a.IsValid() || a.IsUnspecified() || a.IsMulticast() {
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

package downloader

import (
	"net"
	"net/netip"
	"net/url"
	"strings"

	"sainttorrent/pkg/netpolicy"
	"sainttorrent/pkg/tracker"
)

// peerAddrPort converts a wire-format peer endpoint for netpolicy. ok is false
// when ip is not a 4- or 16-byte address.
func peerAddrPort(ip net.IP, port uint16) (netip.AddrPort, bool) {
	a, ok := netip.AddrFromSlice(ip)
	if !ok {
		return netip.AddrPort{}, false
	}
	return netip.AddrPortFrom(a.Unmap(), port), true
}

// trackerPeerSource returns the address a tracker's peers are judged against
// (netpolicy.PeerAllowed): the tracker's host when it is an IP literal or
// "localhost", else the zero Addr, which counts as public. So only a tracker on
// loopback or the LAN can point us at loopback or LAN peers. trackerURL may be
// the announce URL or its trackerLogID form.
func trackerPeerSource(trackerURL string) netip.Addr {
	u, err := url.Parse(trackerURL)
	if err != nil {
		return netip.Addr{}
	}
	host := strings.TrimSuffix(u.Hostname(), ".")
	if strings.EqualFold(host, "localhost") {
		return netip.AddrFrom4([4]byte{127, 0, 0, 1})
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}
	}
	return a
}

// trackerPeerAllowed reports whether a peer a tracker at source listed may be
// dialed: never an address that cannot be a unicast peer (multicast,
// broadcast, 0.0.0.0/8, 240.0.0.0/4), and loopback or LAN ones only from a
// tracker that is itself that local.
func trackerPeerAllowed(p tracker.Peer, source netip.Addr) bool {
	ap, ok := peerAddrPort(p.IP, p.Port)
	return ok && netpolicy.PeerAllowed(ap, source)
}

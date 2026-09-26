package downloader

import (
	"net"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"sainttorrent/pkg/netpolicy"
	"sainttorrent/pkg/tracker"
)

// maxPeerFailCount is how many connection attempts in a row (dial, encryption
// or handshake) may fail before a known peer is only retried every
// failedPeerRedialBackoff instead of every peerRedialBackoff, as libtorrent's
// max_failcount. Otherwise dead or junk addresses fed by DHT, PEX or a tracker
// hold an outbound slot for a dial timeout every minute for as long as the
// torrent runs, and keep real peers from being dialed.
const maxPeerFailCount = 3

// failedPeerRedialBackoff is how often a peer past maxPeerFailCount is still
// tried, in case it was our own network that failed.
const failedPeerRedialBackoff = 30 * time.Minute

// redialBackoff is how long after its last attempt ps may be dialed again.
func (ps *PeerState) redialBackoff() time.Duration {
	if ps.FailCount >= maxPeerFailCount {
		return failedPeerRedialBackoff
	}
	return peerRedialBackoff
}

// noteDialFailed counts a failed connection attempt to ps.
func (ps *PeerState) noteDialFailed() {
	if ps.FailCount < ^uint8(0) {
		ps.FailCount++
	}
}

// markTrackerListed records that a tracker listed ps's address. As in
// libtorrent, a tracker listing takes one failure off the count: someone else
// is apparently reaching the peer, so it earns another try. DHT and PEX
// listings do not, because anyone can make them.
func (ps *PeerState) markTrackerListed() {
	if ps.FailCount > 0 {
		ps.FailCount--
	}
}

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

// isOwnPeerEndpointLocked reports whether ip:port is our own advertised peer
// endpoint: the NAT-mapped external address with the port we announce. DHT
// nodes return our own announce in get_peers values; dialing it only reaches
// ourselves (or fails) while holding an outbound slot. Caller holds m.mu.
func (m *TorrentManager) isOwnPeerEndpointLocked(ip net.IP, port uint16) bool {
	if port == 0 || port != m.advertisedPeerPort || m.natStatus.ExternalIP == "" {
		return false
	}
	ext := net.ParseIP(m.natStatus.ExternalIP)
	return ext != nil && ext.Equal(ip)
}

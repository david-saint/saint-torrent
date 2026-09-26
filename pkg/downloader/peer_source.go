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

// PeerSource records who supplied a known peer's address. The bits accumulate,
// so an address that DHT found and a tracker also listed keeps both.
type PeerSource uint8

const (
	// PeerSourceTracker marks an address a tracker listed.
	PeerSourceTracker PeerSource = 1 << iota
	// PeerSourceDiscovery marks an address DHT or PEX supplied.
	PeerSourceDiscovery
	// PeerSourceIncoming marks an address a peer connected to us from. It
	// vouches for nothing: anyone who knows the infohash can connect.
	PeerSourceIncoming
)

// untrackedDiscovery reports whether DHT or PEX supplied ps's address and no
// tracker listed it, which BEP 27 rules out as a peer of a private torrent.
func (ps *PeerState) untrackedDiscovery() bool {
	return ps.Source&(PeerSourceDiscovery|PeerSourceTracker) == PeerSourceDiscovery
}

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
	ps.Source |= PeerSourceTracker
	if ps.FailCount > 0 {
		ps.FailCount--
	}
}

// privateRefusesDialLocked reports whether we must not connect out to the
// entry ps (nil when the address is unknown) because the torrent is private
// and DHT or PEX, not a tracker, supplied the address: BEP 27 limits a private
// torrent's peers to its trackers. A magnet learns the flag only with its
// metadata, so discovery entries from before that are refused from then on.
// A nil entry is refused too: an outbound dial always has one, so it was
// forgotten (by purgeDiscoveryPeersLocked, or pruned) while the dial was in
// flight. Caller holds s.mu (read or write).
func (s *Session) privateRefusesDialLocked(ps *PeerState) bool {
	if s.Torrent == nil || !s.Torrent.Private {
		return false
	}
	return ps == nil || ps.untrackedDiscovery()
}

// purgeDiscoveryPeersLocked runs when metadata reveals that a magnet is
// private. Peers that DHT or PEX supplied and no tracker listed (allowed while
// the flag was unknown) are disconnected and forgotten, so they neither keep
// trading with us nor get redialed by maintenance or Resume. Entries still
// being dialed are left for connectToPeer, which refuses them. Caller holds
// s.mu.
func (s *Session) purgeDiscoveryPeersLocked() {
	if s.Torrent == nil || !s.Torrent.Private {
		return
	}
	for addr, ps := range s.Peers {
		if !ps.untrackedDiscovery() {
			continue
		}
		if client, active := s.activePeers[addr]; active {
			// The connection's disconnect handler forgets the entry: other
			// code expects every active connection to have one until then.
			if client.Conn != nil {
				_ = client.Conn.Close()
			}
			continue
		}
		if ps.Dialing {
			continue
		}
		delete(s.Peers, addr)
	}
}

// forgetRefusedPeerLocked drops the entry for addr when privateRefusesDialLocked
// refuses it and no connection or dial is using it. Caller holds s.mu.
func (s *Session) forgetRefusedPeerLocked(addr string) {
	if ps := s.Peers[addr]; ps != nil && !ps.Active && !ps.Dialing && s.privateRefusesDialLocked(ps) {
		delete(s.Peers, addr)
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

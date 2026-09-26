package downloader

import (
	"net"
	"net/netip"
	"strconv"
	"time"

	"sainttorrent/pkg/netpolicy"
	"sainttorrent/pkg/peer"
)

var pexInterval = 60 * time.Second

const pexDeltaLimit = 50

// pexIngestLimit is how many added peers of one ut_pex message we act on. BEP 11
// allows 50 per message (we send at most pexDeltaLimit), so the rest of a longer
// list is ignored rather than dialed.
const pexIngestLimit = 50

// pexMaxPortsPerIP is how many ports of one IP a ut_pex message may have us
// dial. BEP 11 advises ignoring an IP listed with many ports: a sender could
// otherwise aim all pexIngestLimit dials of every message at one host's ports.
// Two leaves room for two clients behind one NAT.
const pexMaxPortsPerIP = 2

// pexDiscoverPeer hands a PEX-learned address to the session; tests swap it to
// see the dial candidates without dialing. It is set in init because a direct
// initializer would form a cycle (the peer loop calls handlePEXMessage).
var pexDiscoverPeer func(s *Session, addr string)

func init() {
	pexDiscoverPeer = (*Session).AddPeerFromDiscovery
}

// maxPEXPerInterval is how many ut_pex messages one connection may send within
// one pexInterval; the next one inside it drops the connection. libtorrent
// allows the same three a minute, so no client that works with it is dropped.
const maxPEXPerInterval = 3

// pexRateLimiter decides, per connection, which ut_pex messages we act on. It
// is owned by the connection's message loop.
type pexRateLimiter struct {
	recent   [maxPEXPerInterval]time.Time // when the last messages arrived, oldest first
	lastUsed time.Time                    // when we last acted on one
}

// admit reports, for a ut_pex message arriving at now, whether to decode and act
// on it (use) and whether to drop the sender instead (flood). A message sooner
// than half of pexInterval after the last one used is ignored undecoded, so a
// peer cannot make us dial at the rate it sends. Only a sender over the
// libtorrent rate is dropped: a lifetime count of early messages would also
// catch an honest peer whose messages our loop happened to read back to back
// (after a stall on disk backpressure, say), or that sends every 20-30 s.
func (l *pexRateLimiter) admit(now time.Time) (use, flood bool) {
	if !l.recent[0].IsZero() && now.Sub(l.recent[0]) < pexInterval {
		return false, true
	}
	copy(l.recent[:], l.recent[1:])
	l.recent[len(l.recent)-1] = now
	if !l.lastUsed.IsZero() && now.Sub(l.lastUsed) < pexInterval/2 {
		return false, false
	}
	l.lastUsed = now
	return true, false
}

func (s *Session) pexEnabledLocked() bool {
	return s.Torrent != nil && !s.Torrent.Private
}

// pexAdvertiseAllowedLocked reports whether we may send our peers to others. A
// magnet still fetching metadata does not know its BEP 27 private flag, so until
// it does it only takes PEX in: advertising could leak a private swarm.
func (s *Session) pexAdvertiseAllowedLocked() bool {
	return s.pexEnabledLocked() && !s.metadataMode
}

func (s *Session) pexEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pexEnabledLocked()
}

// extensionHandshakeMapLocked returns the BEP 10 extensions we advertise. A magnet
// still fetching needs ut_metadata to download the info dict (its private flag is
// not known yet), but once metadata is known a private torrent stops offering it,
// so its info dict is never handed to peers outside its tracker's swarm (BEP 27).
func (s *Session) extensionHandshakeMapLocked() map[string]int {
	extensions := make(map[string]int, 2)
	if s.metadataMode || s.Torrent == nil || !s.Torrent.Private {
		extensions[peer.ExtNameMetadata] = peer.LocalMetadataExtID
	}
	if s.pexEnabledLocked() {
		extensions[peer.ExtNamePEX] = peer.LocalPEXExtID
	}
	return extensions
}

// handlePEXMessage acts on the peers a ut_pex message from fromAddr (at fromIP)
// added. A sender may only point us at addresses as local as its own
// (netpolicy): a public peer cannot aim our dials at loopback or LAN services,
// nor anyone at multicast or broadcast addresses. Nor may it list one IP with
// more than pexMaxPortsPerIP ports.
func (s *Session) handlePEXMessage(fromAddr, fromIP string, msg *peer.PEXMessage) {
	if msg == nil || !s.pexEnabled() {
		return
	}
	source, _ := netip.ParseAddr(fromIP)
	added := msg.Added
	if len(added) > pexIngestLimit {
		added = added[:pexIngestLimit]
	}
	var portsPerIP map[netip.Addr]uint8 // only a list of 2+ can repeat an IP
	if len(added) > 1 {
		portsPerIP = make(map[netip.Addr]uint8, len(added))
	}
	for _, p := range added {
		ap, ok := peerAddrPort(p.IP, p.Port)
		if !ok || !netpolicy.PeerAllowed(ap, source) {
			continue
		}
		addr := net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))
		if addr == fromAddr {
			continue
		}
		if portsPerIP != nil {
			if portsPerIP[ap.Addr()] >= pexMaxPortsPerIP {
				continue
			}
			portsPerIP[ap.Addr()]++
		}
		pexDiscoverPeer(s, addr)
	}
}

func (s *Session) buildPEXDelta(excludeAddr string, advertised map[string]struct{}) (*peer.PEXMessage, map[string]struct{}, bool) {
	current, ok := s.pexSnapshot(excludeAddr)
	if !ok {
		return nil, advertised, false
	}
	next := make(map[string]struct{}, len(advertised)+len(current))
	for addr := range advertised {
		next[addr] = struct{}{}
	}

	msg := &peer.PEXMessage{}
	added := 0
	for addr, p := range current {
		if _, ok := advertised[addr]; ok {
			continue
		}
		if added >= pexDeltaLimit {
			continue
		}
		msg.Added = append(msg.Added, p)
		next[addr] = struct{}{}
		added++
	}

	dropped := 0
	for addr := range advertised {
		if _, ok := current[addr]; ok {
			continue
		}
		if dropped >= pexDeltaLimit {
			continue
		}
		if p, ok := pexPeerFromAddr(addr); ok {
			msg.Dropped = append(msg.Dropped, p)
		}
		delete(next, addr)
		dropped++
	}

	if len(msg.Added) == 0 && len(msg.Dropped) == 0 {
		return nil, next, false
	}
	return msg, next, true
}

// pexSnapshot returns the peers we may advertise to excludeAddr; ok is false
// when we may not advertise any.
func (s *Session) pexSnapshot(excludeAddr string) (map[string]peer.PEXPeer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.pexAdvertiseAllowedLocked() {
		return nil, false
	}

	peers := make(map[string]peer.PEXPeer)
	for addr, ps := range s.Peers {
		if addr == excludeAddr || !ps.Active || !ps.Dialable {
			continue
		}
		p, ok := pexPeerFromState(ps)
		if !ok {
			continue
		}
		peers[net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))] = p
	}
	return peers, true
}

func pexPeerFromState(ps *PeerState) (peer.PEXPeer, bool) {
	if ps == nil || ps.Port == 0 {
		return peer.PEXPeer{}, false
	}
	ip := net.ParseIP(ps.IP)
	if ip == nil || ip.IsUnspecified() {
		return peer.PEXPeer{}, false
	}
	return peer.PEXPeer{IP: ip, Port: ps.Port}, true
}

func pexPeerFromAddr(addr string) (peer.PEXPeer, bool) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return peer.PEXPeer{}, false
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return peer.PEXPeer{}, false
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() {
		return peer.PEXPeer{}, false
	}
	return peer.PEXPeer{IP: ip, Port: uint16(port)}, true
}

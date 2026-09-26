package downloader

import (
	"net"
	"strings"
)

// maxConnectionsPerIP bounds how many connections one remote host may have open
// to a session at once, so a single host cannot take every inbound slot by
// connecting from different source ports. Four leaves room for several clients
// behind one NAT. IPv6 addresses count per /64, the block a single host usually
// controls. Loopback is exempt, for local tests and same-machine setups.
const maxConnectionsPerIP = 4

// maxSelfAddrs bounds how many dial addresses that answered with our own peer ID
// (our own listener, reached through a tracker or DHT) a session remembers.
const maxSelfAddrs = 16

// peerAdmission is a session's connection admission state: open connections per
// remote host and per remote peer ID, and the dial addresses that turned out to be
// us. It is consulted once per connection, never per message. Guarded by
// Session.mu; the zero value is ready to use.
type peerAdmission struct {
	perHost   map[string]int
	peerIDs   map[[20]byte]struct{}
	selfAddrs map[string]struct{}
}

// peerHostKey returns the key connections from ip are counted under (the address,
// or its /64 for IPv6) and whether ip is a loopback address.
func peerHostKey(ip string) (key string, loopback bool) {
	if i := strings.IndexByte(ip, '%'); i >= 0 {
		ip = ip[:i] // drop an IPv6 zone
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return ip, false
	}
	if v4 := parsed.To4(); v4 != nil {
		return v4.String(), v4.IsLoopback()
	}
	return parsed.Mask(net.CIDRMask(64, 128)).String() + "/64", parsed.IsLoopback()
}

// admitPeerLocked decides whether a connection that completed its handshake may
// run, and if so counts it; it returns a non-empty reason when it may not. It
// refuses a connection to ourselves, a second connection to a peer ID we are
// already connected to, a host over maxConnectionsPerIP, and a second connection
// under an address key that is already active (its PeerState would be shared).
// A zero peer ID identifies nobody, so it is exempt from the ID checks. Caller
// holds s.mu.
func (s *Session) admitPeerLocked(peerAddr, hostKey string, loopback bool, remoteID [20]byte, outbound bool) string {
	a := &s.admission
	if _, active := s.activePeers[peerAddr]; active {
		return "duplicate_address"
	}
	if remoteID != ([20]byte{}) {
		if remoteID == s.PeerID {
			if outbound {
				a.rememberSelfAddrLocked(peerAddr)
			}
			return "self_connection"
		}
		if _, dup := a.peerIDs[remoteID]; dup {
			return "duplicate_peer_id"
		}
	}
	if !loopback && a.perHost[hostKey] >= maxConnectionsPerIP {
		return "per_ip_limit"
	}
	if a.perHost == nil {
		a.perHost = make(map[string]int)
	}
	a.perHost[hostKey]++
	if remoteID != ([20]byte{}) {
		if a.peerIDs == nil {
			a.peerIDs = make(map[[20]byte]struct{})
		}
		a.peerIDs[remoteID] = struct{}{}
	}
	return ""
}

// releasePeerLocked undoes admitPeerLocked when the connection ends. Caller holds
// s.mu.
func (s *Session) releasePeerLocked(hostKey string, remoteID [20]byte) {
	a := &s.admission
	if a.perHost[hostKey] <= 1 {
		delete(a.perHost, hostKey)
	} else {
		a.perHost[hostKey]--
	}
	if remoteID != ([20]byte{}) {
		delete(a.peerIDs, remoteID)
	}
}

func (a *peerAdmission) rememberSelfAddrLocked(peerAddr string) {
	if a.selfAddrs == nil {
		a.selfAddrs = make(map[string]struct{})
	}
	if len(a.selfAddrs) < maxSelfAddrs {
		a.selfAddrs[peerAddr] = struct{}{}
	}
}

// refusesDialLocked reports whether we must not dial peerAddr: it is one of our
// own addresses. Caller holds s.mu (read or write).
func (s *Session) refusesDialLocked(peerAddr string) bool {
	_, self := s.admission.selfAddrs[peerAddr]
	return self
}

package downloader

import (
	"net"
	"strings"
	"time"
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

// A host whose pieces fail the hash check hashFailStrikesToBan times within
// peerBanDuration is refused, for dials and incoming connections alike, for
// peerBanDuration. Every piece is assembled from a single connection, so the
// blame is exact; one strike is forgiven, for a peer that was fed bad data
// itself or hit a rare fault. maxStrikeEntries bounds the hosts tracked.
const (
	hashFailStrikesToBan = 2
	peerBanDuration      = time.Hour
	maxStrikeEntries     = 1024
)

// peerAdmission is a session's connection admission state: open connections per
// remote host and per remote peer ID, the dial addresses that turned out to be
// us, and hash-failure strikes and bans per host. It is consulted once per
// connection, never per message or block. Guarded by Session.mu; the zero value
// is ready to use.
type peerAdmission struct {
	perHost   map[string]int
	peerIDs   map[[20]byte]struct{}
	selfAddrs map[string]struct{}
	strikes   map[string]*hostStrikes
}

// hostStrikes records a host's recent hash failures and any ban they earned.
type hostStrikes struct {
	count       int
	last        time.Time
	bannedUntil time.Time
}

// pieceSource identifies the connection a completed piece came from, so a hash
// failure can be charged to its host. A connection costs its host at most one
// strike, however many of its pieces were in flight when the first one failed.
type pieceSource struct {
	host   string // admission host key; "" when exempt (loopback)
	struck bool   // guarded by Session.mu
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
	if a.bannedLocked(hostKey, time.Now()) {
		return "banned"
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

// refusesDialLocked reports whether we must not dial peerAddr (at ip): it is one
// of our own addresses or its host is banned. Caller holds s.mu (read or write).
func (s *Session) refusesDialLocked(peerAddr, ip string) bool {
	a := &s.admission
	if _, self := a.selfAddrs[peerAddr]; self {
		return true
	}
	if len(a.strikes) == 0 {
		return false
	}
	hostKey, _ := peerHostKey(ip)
	return a.bannedLocked(hostKey, time.Now())
}

// refusesIncomingLocked reports whether a connection from remote comes from a
// banned host. Caller holds s.mu (read or write).
func (s *Session) refusesIncomingLocked(remote net.Addr) bool {
	if len(s.admission.strikes) == 0 || remote == nil {
		return false
	}
	host, _, err := net.SplitHostPort(remote.String())
	if err != nil {
		return false
	}
	hostKey, _ := peerHostKey(host)
	return s.admission.bannedLocked(hostKey, time.Now())
}

func (a *peerAdmission) bannedLocked(hostKey string, now time.Time) bool {
	st := a.strikes[hostKey]
	return st != nil && now.Before(st.bannedUntil)
}

// strikePieceSourceLocked records a hash failure against the host of src and
// reports whether that banned the host. A nil source (webseeds), an exempt host
// or a connection that was already charged adds no strike. Caller holds s.mu.
func (s *Session) strikePieceSourceLocked(src *pieceSource, now time.Time) bool {
	if src == nil || src.host == "" || src.struck {
		return false
	}
	src.struck = true
	return s.strikePeerHostLocked(src.host, now)
}

// strikePeerHostLocked records a hash failure for hostKey and reports whether it
// banned the host. Caller holds s.mu.
func (s *Session) strikePeerHostLocked(hostKey string, now time.Time) bool {
	a := &s.admission
	st := a.strikes[hostKey]
	if st == nil {
		if a.strikes == nil {
			a.strikes = make(map[string]*hostStrikes)
		}
		if len(a.strikes) >= maxStrikeEntries {
			a.evictStrikeLocked(now)
		}
		st = &hostStrikes{}
		a.strikes[hostKey] = st
	}
	if now.Sub(st.last) > peerBanDuration {
		st.count = 0 // an old strike is forgotten
	}
	st.count++
	st.last = now
	if st.count < hashFailStrikesToBan {
		return false
	}
	st.count = 0
	st.bannedUntil = now.Add(peerBanDuration)
	return true
}

// evictStrikeLocked makes room in a full strike table: it drops every entry that
// no longer matters (no live ban, strikes forgotten), or else the host struck
// longest ago, preferring one that is not banned. Only runs on a hash failure,
// never per connection.
func (a *peerAdmission) evictStrikeLocked(now time.Time) {
	var oldestKey, oldestBannedKey string
	var oldest, oldestBanned time.Time
	for key, st := range a.strikes {
		banned := now.Before(st.bannedUntil)
		switch {
		case !banned && now.Sub(st.last) > peerBanDuration:
			delete(a.strikes, key)
		case !banned && (oldestKey == "" || st.last.Before(oldest)):
			oldestKey, oldest = key, st.last
		case banned && (oldestBannedKey == "" || st.last.Before(oldestBanned)):
			oldestBannedKey, oldestBanned = key, st.last
		}
	}
	if len(a.strikes) < maxStrikeEntries {
		return
	}
	if oldestKey != "" {
		delete(a.strikes, oldestKey)
	} else {
		delete(a.strikes, oldestBannedKey)
	}
}

// closeHostConnsLocked closes every active connection from hostKey, so a ban
// takes effect on connections that were open when it was earned. Caller holds
// s.mu.
func (s *Session) closeHostConnsLocked(hostKey string) {
	for addr, client := range s.activePeers {
		ps := s.Peers[addr]
		if ps == nil {
			continue
		}
		if key, _ := peerHostKey(ps.IP); key == hostKey && client.Conn != nil {
			_ = client.Conn.Close()
		}
	}
}

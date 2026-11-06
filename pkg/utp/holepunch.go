package utp

import (
	"net"
	"net/netip"
	"sync"
	"time"
)

// maxExpectedIncoming bounds the endpoints ExpectIncoming exempts from
// refusal at once. Each entry is an endpoint a holepunch relay told us to
// connect to, so a handful are live at a time.
const maxExpectedIncoming = 256

// ExpectIncoming lets new inbound connections from addr through for ttl even
// while s refuses incoming connections (SetRefuseIncoming). A BEP 55
// holepunch has both peers dial each other at once, and whichever SYN crosses
// the NATs first must be accepted, not answered with a RESET that would also
// end the other side's attempt. The exemption only matters while refusal is
// on; at most maxExpectedIncoming endpoints are held, the oldest replaced
// first.
func (s *Socket) ExpectIncoming(addr netip.AddrPort, ttl time.Duration) {
	if !addr.IsValid() || ttl <= 0 {
		return
	}
	addr = normalizeAddrPort(addr)
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expected == nil {
		s.expected = make(map[netip.AddrPort]time.Time)
	}
	if _, ok := s.expected[addr]; !ok && len(s.expected) >= maxExpectedIncoming {
		var oldest netip.AddrPort
		var oldestAt time.Time
		for a, until := range s.expected {
			if !now.Before(until) {
				delete(s.expected, a)
				continue
			}
			if !oldest.IsValid() || until.Before(oldestAt) {
				oldest, oldestAt = a, until
			}
		}
		if len(s.expected) >= maxExpectedIncoming {
			delete(s.expected, oldest)
		}
	}
	s.expected[addr] = now.Add(ttl)
}

// expectedLocked reports whether addr was named by ExpectIncoming and has not
// expired. Only consulted for SYNs (and half-open completions) while refusing.
// Caller holds s.mu.
func (s *Socket) expectedLocked(addr *net.UDPAddr, now time.Time) bool {
	if len(s.expected) == 0 {
		return false
	}
	ap := normalizeAddrPort(addr.AddrPort())
	until, ok := s.expected[ap]
	if !ok {
		return false
	}
	if !now.Before(until) {
		delete(s.expected, ap)
		return false
	}
	return true
}

// refusesLocked reports whether a new inbound connection from addr is turned
// away (see SetRefuseIncoming and ExpectIncoming). Caller holds s.mu.
func (s *Socket) refusesLocked(addr *net.UDPAddr, now time.Time) bool {
	return s.refuseIncoming.Load() && !s.expectedLocked(addr, now)
}

func normalizeAddrPort(ap netip.AddrPort) netip.AddrPort {
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
}

// natFilter emulates the inbound filtering of an endpoint-dependent
// ("port-restricted cone") NAT in front of a Socket: a datagram is delivered
// only from an endpoint the socket has sent to. See EmulateNAT.
type natFilter struct {
	mu   sync.Mutex
	sent map[netip.AddrPort]struct{}
}

func (f *natFilter) noteSent(addr *net.UDPAddr) {
	ap := normalizeAddrPort(addr.AddrPort())
	f.mu.Lock()
	f.sent[ap] = struct{}{}
	f.mu.Unlock()
}

func (f *natFilter) allows(addr *net.UDPAddr) bool {
	ap := normalizeAddrPort(addr.AddrPort())
	f.mu.Lock()
	_, ok := f.sent[ap]
	f.mu.Unlock()
	return ok
}

// EmulateNAT makes s drop every datagram from an endpoint it has not sent
// one to, as a port-restricted cone NAT in front of it would: nobody can
// reach it first, and a peer it dials can answer. It is for tests and
// simulations of peers behind NAT, such as BEP 55 holepunching, and cannot
// be turned off again.
func (s *Socket) EmulateNAT() {
	s.nat.CompareAndSwap(nil, &natFilter{sent: make(map[netip.AddrPort]struct{})})
}

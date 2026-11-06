package downloader

import (
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"

	"sainttorrent/pkg/netpolicy"
)

// externalAddrMinReporters is how many distinct hosts must report the same
// address in their extension handshake's yourip before it is taken as ours. A
// single peer can say anything; requiring agreement keeps one hostile peer from
// making us skip dialing another peer's endpoint.
const externalAddrMinReporters = 2

// maxExternalAddrCandidates bounds the addresses being voted on. Votes are
// cheap to rebuild, so a full table is simply cleared.
const maxExternalAddrCandidates = 16

// maxExternalAddrs bounds the confirmed addresses kept. A network change adds a
// new one, so the oldest is dropped first.
const maxExternalAddrs = 4

// externalAddrs tracks the public addresses we are reached at, so a tracker,
// DHT or PEX listing our own endpoint is not dialed. Behind a NAT with no
// UPnP/NAT-PMP mapping (a phone hotspot, campus or hotel wi-fi) nothing else
// tells us that address, and the dial would hold an outbound slot until it
// timed out. Addresses come from the NAT gateway when it maps a port, and from
// peers' BEP 10 yourip once enough distinct hosts agree. One instance is shared
// by every session of a manager; nil is valid and knows no address.
type externalAddrs struct {
	mu    sync.Mutex
	votes map[netip.Addr]map[netip.Addr]struct{} // candidate -> reporter host keys
	// known is read lock-free on every dial decision; writers replace it.
	known atomic.Pointer[[]netip.Addr]
}

func newExternalAddrs() *externalAddrs {
	return &externalAddrs{votes: make(map[netip.Addr]map[netip.Addr]struct{})}
}

// contains reports whether a is one of our known external addresses.
func (e *externalAddrs) contains(a netip.Addr) bool {
	if e == nil {
		return false
	}
	known := e.known.Load()
	if known == nil {
		return false
	}
	return slices.Contains(*known, a.Unmap())
}

// any reports whether any external address is known, so callers can skip
// parsing an endpoint when none is.
func (e *externalAddrs) any() bool {
	if e == nil {
		return false
	}
	known := e.known.Load()
	return known != nil && len(*known) > 0
}

// setMapped records the address a NAT gateway reported for its port mapping.
// The gateway is trusted, so it needs no votes.
func (e *externalAddrs) setMapped(a netip.Addr) {
	if e == nil {
		return
	}
	a = a.Unmap()
	if netpolicy.Classify(a) != netpolicy.ScopeGlobal {
		return
	}
	e.mu.Lock()
	e.addLocked(a)
	e.mu.Unlock()
}

// report records that the peer at reporter sees us at observed. Only public
// addresses count, both ways: a LAN peer reports our LAN address, which no
// tracker or DHT node hands back to us.
func (e *externalAddrs) report(reporter, observed netip.Addr) {
	if e == nil {
		return
	}
	reporter, observed = reporter.Unmap(), observed.Unmap()
	if netpolicy.Classify(observed) != netpolicy.ScopeGlobal || netpolicy.Classify(reporter) != netpolicy.ScopeGlobal {
		return
	}
	if e.contains(observed) {
		return
	}
	key := reporterKey(reporter)
	e.mu.Lock()
	defer e.mu.Unlock()
	voters := e.votes[observed]
	if voters == nil {
		if len(e.votes) >= maxExternalAddrCandidates {
			clear(e.votes)
		}
		voters = make(map[netip.Addr]struct{}, externalAddrMinReporters)
		e.votes[observed] = voters
	}
	voters[key] = struct{}{}
	if len(voters) >= externalAddrMinReporters {
		delete(e.votes, observed)
		e.addLocked(observed)
	}
}

// addLocked publishes a as known, newest first, dropping the oldest past
// maxExternalAddrs. Caller holds e.mu.
func (e *externalAddrs) addLocked(a netip.Addr) {
	var next []netip.Addr
	if cur := e.known.Load(); cur != nil {
		if slices.Contains(*cur, a) {
			return
		}
		next = make([]netip.Addr, 0, min(len(*cur)+1, maxExternalAddrs))
		next = append(next, a)
		next = append(next, (*cur)[:min(len(*cur), maxExternalAddrs-1)]...)
	} else {
		next = []netip.Addr{a}
	}
	e.known.Store(&next)
}

// reporterKey groups reporters the way peer admission groups hosts: an IPv4
// address on its own, an IPv6 address by its /64, which one host can fill.
func reporterKey(a netip.Addr) netip.Addr {
	if a.Is6() {
		p, err := a.Prefix(64)
		if err == nil {
			return p.Addr()
		}
	}
	return a
}

// isOwnEndpoint reports whether addr ("host:port", as peers are keyed) is one
// of our external addresses at port, the port we advertise.
func (e *externalAddrs) isOwnEndpoint(addr string, port uint16) bool {
	if port == 0 || !e.any() {
		return false
	}
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return false
	}
	return ap.Port() == port && e.contains(ap.Addr())
}

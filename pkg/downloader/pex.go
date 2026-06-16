package downloader

import (
	"net"
	"strconv"
	"time"

	"sainttorrent/pkg/peer"
)

var pexInterval = 60 * time.Second

const pexDeltaLimit = 50

func (s *Session) pexEnabledLocked() bool {
	return s.Torrent != nil && !s.Torrent.Private
}

func (s *Session) pexEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pexEnabledLocked()
}

func (s *Session) extensionHandshakeMapLocked() map[string]int {
	extensions := map[string]int{
		peer.ExtNameMetadata: peer.LocalMetadataExtID,
	}
	if s.pexEnabledLocked() {
		extensions[peer.ExtNamePEX] = peer.LocalPEXExtID
	}
	return extensions
}

func (s *Session) handlePEXMessage(fromAddr string, msg *peer.PEXMessage) {
	if msg == nil || !s.pexEnabled() {
		return
	}
	for _, p := range msg.Added {
		if p.Port == 0 || p.IP == nil || p.IP.IsUnspecified() {
			continue
		}
		addr := net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))
		if addr == fromAddr {
			continue
		}
		s.AddPeerFromDiscovery(addr)
	}
}

func (s *Session) buildPEXDelta(excludeAddr string, advertised map[string]struct{}) (*peer.PEXMessage, map[string]struct{}, bool) {
	if !s.pexEnabled() {
		return nil, advertised, false
	}
	current := s.pexSnapshot(excludeAddr)
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

func (s *Session) pexSnapshot(excludeAddr string) map[string]peer.PEXPeer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.pexEnabledLocked() {
		return nil
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
	return peers
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

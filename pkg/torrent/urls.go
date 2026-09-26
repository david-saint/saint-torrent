package torrent

import (
	"net/url"
	"strings"
)

const (
	// maxTrackers bounds the unique tracker URLs kept from a torrent's
	// announce-list or a magnet's tr= parameters. Every announce contacts each
	// tracker concurrently, so an unbounded list meant one socket per entry.
	// Real announce-lists hold a few dozen at most.
	maxTrackers = 200
	// maxWebSeeds bounds the unique url-list entries kept; large mirror lists
	// run to a few hundred.
	maxWebSeeds = 1024
)

// urlSet collects normalized URLs in first-seen order, dropping invalid ones
// and repeats, up to a fixed number of entries.
type urlSet struct {
	list     []string
	seen     map[string]struct{}
	limit    int
	allowUDP bool
}

func newURLSet(limit int, allowUDP bool) *urlSet {
	return &urlSet{seen: make(map[string]struct{}), limit: limit, allowUDP: allowUDP}
}

// add records raw if it is a new valid URL. It returns false once the set is
// full, so callers can stop walking a long list early.
func (s *urlSet) add(raw string) bool {
	if len(s.list) >= s.limit {
		return false
	}
	u, ok := normalizeURL(raw, s.allowUDP)
	if !ok {
		return true
	}
	if _, dup := s.seen[u]; dup {
		return true
	}
	s.seen[u] = struct{}{}
	s.list = append(s.list, u)
	return len(s.list) < s.limit
}

// normalizeURL trims raw and keeps it only if it is an http or https URL (or
// udp, when allowUDP is set) with a host. Scheme and host are lowercased so
// equivalent spellings dedupe to one entry.
func normalizeURL(raw string, allowUDP bool) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return "", false
	}
	switch u.Scheme { // url.Parse already lowercases the scheme
	case "http", "https":
	case "udp":
		if !allowUDP {
			return "", false
		}
	default:
		return "", false
	}
	u.Host = strings.ToLower(u.Host)
	return u.String(), true
}

package torrent

import (
	"net/url"
	"strings"
)

// URL length limits. A torrent is otherwise bounded only by MaxFileSize, so
// one announce or url-list entry could run to megabytes, stored with the
// torrent and sent as the request line of every announce or range request.
const (
	// MaxTrackerURLLength bounds one tracker URL (announce, announce-list
	// or a magnet's tr=). Real announce URLs, passkeys included, stay under a
	// few hundred bytes; tracker.ParseAnnounceURL applies the same limit.
	MaxTrackerURLLength = 2048
	// MaxWebSeedURLLength bounds one url-list entry, the base each file's
	// path is appended to.
	MaxWebSeedURLLength = 4096
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

// urlSet collects normalized URLs in first-seen order, dropping invalid,
// over-long and repeated ones, up to a fixed number of entries.
type urlSet struct {
	list        []string
	seen        map[string]struct{}
	limit       int
	allowUDP    bool
	maxURLBytes int
}

func newURLSet(limit int, allowUDP bool, maxURLBytes int) *urlSet {
	return &urlSet{seen: make(map[string]struct{}), limit: limit, allowUDP: allowUDP, maxURLBytes: maxURLBytes}
}

// add records raw if it is a new valid URL. It returns false once the set is
// full, so callers can stop walking a long list early.
func (s *urlSet) add(raw string) bool {
	if len(s.list) >= s.limit {
		return false
	}
	u, ok := normalizeURL(raw, s.allowUDP, s.maxURLBytes)
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
// udp, when allowUDP is set) with a host, at most maxBytes long both as given
// and once normalized (escaping can lengthen it). Scheme and host are
// lowercased so equivalent spellings dedupe to one entry.
func normalizeURL(raw string, allowUDP bool, maxBytes int) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > maxBytes {
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
	if s := u.String(); len(s) <= maxBytes {
		return s, true
	}
	return "", false
}

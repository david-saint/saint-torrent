// Package tracker implements BitTorrent tracker announcement protocols over HTTP and UDP.
package tracker

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"sainttorrent/pkg/bencode"
)

// Peer represents a torrent peer discovered from the tracker.
type Peer struct {
	IP   net.IP
	Port uint16
}

// TrackerResponse represents the parsed response from a tracker.
type TrackerResponse struct {
	Interval    int
	MinInterval int
	Peers       []Peer
	Warning     string
	Complete    int
	Incomplete  int
}

const defaultNumWant = 200

// BuildTrackerURL constructs the tracker announce URL with the proper parameters.
// Specifically, it escapes infoHash and peerID exactly as required by the BitTorrent spec.
func BuildTrackerURL(baseURL string, infoHash [20]byte, peerID [20]byte, port uint16, uploaded, downloaded, left int64, compact bool, event string, numWant ...int) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	params := base.Query()
	params.Set("port", strconv.Itoa(int(port)))
	params.Set("uploaded", strconv.FormatInt(uploaded, 10))
	params.Set("downloaded", strconv.FormatInt(downloaded, 10))
	params.Set("left", strconv.FormatInt(left, 10))
	params.Del("info_hash")
	params.Del("peer_id")
	if compact {
		params.Set("compact", "1")
	} else {
		params.Set("compact", "0")
	}
	want := defaultNumWant
	if len(numWant) > 0 {
		want = numWant[0]
	}
	if want != 0 {
		params.Set("numwant", strconv.Itoa(want))
	} else {
		params.Del("numwant")
	}
	if event != "" {
		params.Set("event", event)
	} else {
		params.Del("event")
	}

	// Escape infoHash and peerID manually as per BitTorrent spec
	escapedInfoHash := escapeBinary(infoHash[:])
	escapedPeerID := escapeBinary(peerID[:])

	rawQuery := params.Encode()
	if rawQuery != "" {
		rawQuery += "&"
	}
	rawQuery += "info_hash=" + escapedInfoHash + "&peer_id=" + escapedPeerID
	base.RawQuery = rawQuery

	return base.String(), nil
}

func escapeBinary(b []byte) string {
	var sb strings.Builder
	for _, c := range b {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			sb.WriteByte(c)
		} else {
			fmt.Fprintf(&sb, "%%%02X", c)
		}
	}
	return sb.String()
}

// MaxResponsePeers caps how many peers one announce reply may contribute. We
// ask for numwant=200 and honest trackers never send ten times that; the cap
// bounds the parse, dedup and dial work a hostile tracker can cause.
const MaxResponsePeers = 2000

// maxTrackerMessage caps tracker-supplied text (failure reason, warning
// message, UDP error) that ends up in errors, logs, the TUI and the HTTP API.
const maxTrackerMessage = 256

// trackerMessage bounds tracker-supplied text to maxTrackerMessage bytes of
// valid UTF-8, so a hostile tracker cannot push megabytes of text (or broken
// encodings) into every place an error is shown.
func trackerMessage(s string) string {
	if len(s) > maxTrackerMessage {
		s = s[:maxTrackerMessage]
	}
	s = strings.ToValidUTF8(s, "�")
	if len(s) > maxTrackerMessage {
		// Replacement characters grew it; cut again on a rune boundary.
		cut := maxTrackerMessage
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut]
	}
	return s
}

// clampCount converts a tracker-supplied integer to a non-negative int that
// fits in 32 bits, so a huge value cannot wrap on 32-bit builds (2^32+1 would
// otherwise become 1) or overflow later Duration arithmetic.
func clampCount(v int64) int {
	switch {
	case v < 0:
		return 0
	case v > math.MaxInt32:
		return math.MaxInt32
	}
	return int(v)
}

// compactPeers decodes up to limit compact peer entries of stride bytes (an
// IPv4 or IPv6 address followed by a big-endian port). The addresses are
// copied into one small backing array, so the peers never pin the response
// body in memory.
func compactPeers(data string, ipLen, limit int) []Peer {
	stride := ipLen + 2
	n := min(len(data)/stride, limit)
	if n <= 0 {
		return nil
	}
	ips := make([]byte, n*ipLen)
	peers := make([]Peer, 0, n)
	for i := 0; i < n; i++ {
		entry := data[i*stride : (i+1)*stride]
		ip := ips[i*ipLen : (i+1)*ipLen : (i+1)*ipLen]
		copy(ip, entry[:ipLen])
		port := uint16(entry[ipLen])<<8 | uint16(entry[ipLen+1])
		peers = append(peers, Peer{IP: net.IP(ip), Port: port})
	}
	return peers
}

// ParseTrackerResponse decodes a bencoded tracker response. At most
// MaxResponsePeers peers are returned.
func ParseTrackerResponse(data []byte) (*TrackerResponse, error) {
	val, rest, err := bencode.DecodePrefix(data)
	if err != nil {
		return nil, fmt.Errorf("bencode parsing error: %w", err)
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("bencode parsing error: trailing data after tracker response")
	}
	dict, ok := val.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("tracker response is not a dictionary")
	}

	if failReason, ok := dict["failure reason"].(string); ok {
		return nil, fmt.Errorf("tracker error: %s", trackerMessage(failReason))
	}

	var interval int
	if intVal, ok := dict["interval"].(int64); ok {
		interval = clampCount(intVal)
	} else {
		return nil, fmt.Errorf("missing or invalid interval")
	}

	var minInterval int
	if minIntVal, ok := dict["min interval"].(int64); ok {
		minInterval = clampCount(minIntVal)
	}

	var warning string
	if warnVal, ok := dict["warning message"].(string); ok {
		warning = trackerMessage(warnVal)
	}

	var complete int
	if compVal, ok := dict["complete"].(int64); ok {
		complete = clampCount(compVal)
	}

	var incomplete int
	if incompVal, ok := dict["incomplete"].(int64); ok {
		incomplete = clampCount(incompVal)
	}

	var peers []Peer
	peersVal, exists := dict["peers"]
	if exists {
		if peersStr, ok := peersVal.(string); ok {
			// Compact IPv4 peers (6 bytes per peer)
			if len(peersStr)%6 != 0 {
				return nil, fmt.Errorf("compact peers length must be a multiple of 6, got %d", len(peersStr))
			}
			peers = compactPeers(peersStr, 4, MaxResponsePeers)
		} else if peersList, ok := peersVal.([]interface{}); ok {
			// Non-compact peer list (list of dictionaries)
			for _, pVal := range peersList {
				if len(peers) >= MaxResponsePeers {
					break
				}
				pDict, ok := pVal.(map[string]interface{})
				if !ok {
					continue
				}
				ipStr, ok := pDict["ip"].(string)
				if !ok {
					continue
				}
				var port uint16
				if portVal, ok := pDict["port"].(int64); ok {
					if portVal < 0 || portVal > 65535 {
						continue // invalid port range
					}
					port = uint16(portVal)
				} else {
					continue
				}
				ip := net.ParseIP(ipStr)
				if ip != nil {
					peers = append(peers, Peer{
						IP:   ip,
						Port: port,
					})
				}
			}
		} else {
			return nil, fmt.Errorf("invalid peers field format")
		}
	}

	// Compact IPv6 peers (18 bytes per peer)
	if peers6Val, ok := dict["peers6"].(string); ok {
		if len(peers6Val)%18 != 0 {
			return nil, fmt.Errorf("compact peers6 length must be a multiple of 18, got %d", len(peers6Val))
		}
		peers = append(peers, compactPeers(peers6Val, 16, MaxResponsePeers-len(peers))...)
	}

	return &TrackerResponse{
		Interval:    interval,
		MinInterval: minInterval,
		Peers:       peers,
		Warning:     warning,
		Complete:    complete,
		Incomplete:  incomplete,
	}, nil
}

// ScrapeStats holds the swarm-health counts reported by a tracker scrape for a
// single info hash (BEP 48 "files" entry / BEP 15 scrape response triple).
type ScrapeStats struct {
	// Complete is the number of seeders (peers with the complete file).
	Complete int
	// Downloaded is the number of times the torrent has been downloaded to
	// completion, as reported by the tracker.
	Downloaded int
	// Incomplete is the number of leechers (peers still downloading).
	Incomplete int
}

// maxScrapeResponse caps how many bytes of an HTTP scrape response we buffer. A
// single-hash scrape reply is about 100 bytes; this ceiling stops a malicious
// or MITM'd tracker from streaming data (and bencode decode amplification)
// into memory.
const maxScrapeResponse = 64 * 1024

// defaultScrapeTimeout bounds an HTTP scrape when the caller passes a context
// without its own deadline, so an exported call can never hang indefinitely.
const defaultScrapeTimeout = 30 * time.Second

// ParseAnnounceURL parses a tracker announce URL and checks that it names a
// host and a supported scheme: http, https or udp, in any letter case (the
// returned URL's Scheme is lowercase).
func ParseAnnounceURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "http", "https", "udp":
	default:
		return nil, fmt.Errorf("unsupported tracker scheme %q", u.Scheme)
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("tracker URL has no host")
	}
	return u, nil
}

// ReadCappedBody reads up to max bytes from r, returning an error if the source
// exceeds the cap rather than silently truncating. Reading one byte past the
// cap is what lets us detect (rather than hide) an over-limit response. It
// bounds tracker announce and scrape bodies against a malicious or MITM'd
// tracker streaming unbounded data into memory.
func ReadCappedBody(r io.Reader, max int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, fmt.Errorf("response exceeds %d bytes", max)
	}
	return data, nil
}

// ScrapeURL derives the scrape URL from an announce URL using the BEP 48
// convention: the final path segment must begin with "announce", and that
// "announce" prefix is replaced with "scrape" (e.g. "/announce" -> "/scrape",
// "/x/announce.php" -> "/x/scrape.php"). An error is returned when the URL has
// no path or the final segment does not begin with "announce", signalling that
// the tracker does not advertise scrape support.
//
// The result is canonicalized via url.String(). A percent-encoded path
// separator in the announce URL (e.g. "/x%2Fannounce") cannot be preserved
// because url.Parse decodes it before this function sees it; such URLs are
// effectively non-existent for real trackers.
func ScrapeURL(announceURL string) (string, error) {
	u, err := url.Parse(announceURL)
	if err != nil {
		return "", err
	}
	idx := strings.LastIndex(u.Path, "/")
	if idx == -1 {
		return "", fmt.Errorf("announce URL %q has no path segment to derive scrape URL", announceURL)
	}
	last := u.Path[idx+1:]
	if !strings.HasPrefix(last, "announce") {
		return "", fmt.Errorf("announce URL segment %q does not support scrape", last)
	}
	u.Path = u.Path[:idx+1] + "scrape" + last[len("announce"):]
	// Clear any escaped-path cached from the announce URL so String() re-encodes
	// canonically from the new Path rather than emitting a stale RawPath. (A
	// percent-encoded path separator in the announce URL is already decoded by
	// url.Parse, so it cannot be faithfully preserved here regardless.)
	u.RawPath = ""
	return u.String(), nil
}

// BuildScrapeURL constructs an HTTP scrape request URL, appending one
// info_hash query parameter per requested hash. The hashes are escaped exactly
// as required by the BitTorrent spec, mirroring BuildTrackerURL.
func BuildScrapeURL(scrapeURL string, infoHashes ...[20]byte) (string, error) {
	base, err := url.Parse(scrapeURL)
	if err != nil {
		return "", err
	}
	params := base.Query()
	params.Del("info_hash")

	var sb strings.Builder
	sb.WriteString(params.Encode())
	for _, h := range infoHashes {
		if sb.Len() > 0 {
			sb.WriteByte('&')
		}
		sb.WriteString("info_hash=")
		sb.WriteString(escapeBinary(h[:]))
	}
	base.RawQuery = sb.String()
	return base.String(), nil
}

// ParseScrapeResponse decodes a bencoded HTTP scrape response (BEP 48). The
// returned map is keyed by the raw 20-byte info hash found in the "files"
// dictionary. Entries whose key is not exactly 20 bytes are skipped.
func ParseScrapeResponse(data []byte) (map[[20]byte]ScrapeStats, error) {
	val, rest, err := bencode.DecodePrefix(data)
	if err != nil {
		return nil, fmt.Errorf("bencode parsing error: %w", err)
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("bencode parsing error: trailing data after scrape response")
	}
	dict, ok := val.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("scrape response is not a dictionary")
	}
	if failReason, ok := dict["failure reason"].(string); ok {
		return nil, fmt.Errorf("tracker error: %s", trackerMessage(failReason))
	}
	filesVal, ok := dict["files"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("scrape response missing files dictionary")
	}

	result := make(map[[20]byte]ScrapeStats, len(filesVal))
	for key, v := range filesVal {
		if len(key) != 20 {
			continue
		}
		fileDict, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		var stats ScrapeStats
		if c, ok := fileDict["complete"].(int64); ok {
			stats.Complete = clampCount(c)
		}
		if d, ok := fileDict["downloaded"].(int64); ok {
			stats.Downloaded = clampCount(d)
		}
		if i, ok := fileDict["incomplete"].(int64); ok {
			stats.Incomplete = clampCount(i)
		}
		var hash [20]byte
		copy(hash[:], key)
		result[hash] = stats
	}
	return result, nil
}

// HTTPScrape performs a full HTTP(S) scrape against the tracker identified by
// announceURL (BEP 48). It derives the scrape endpoint from the announce URL,
// issues the request, and parses the swarm-health counts per info hash. The
// context bounds the request lifetime.
func HTTPScrape(ctx context.Context, announceURL string, infoHashes ...[20]byte) (map[[20]byte]ScrapeStats, error) {
	scrapeURL, err := ScrapeURL(announceURL)
	if err != nil {
		return nil, err
	}
	reqURL, err := BuildScrapeURL(scrapeURL, infoHashes...)
	if err != nil {
		return nil, err
	}

	// Guarantee a deadline even if the caller supplied an unbounded context, so
	// a stalled tracker can never hang the request indefinitely.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultScrapeTimeout)
		defer cancel()
	}

	req, err := NewRequest(ctx, PurposeScrape, reqURL)
	if err != nil {
		return nil, err
	}

	resp, err := HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape returned HTTP %d", resp.StatusCode)
	}

	data, err := ReadCappedBody(resp.Body, maxScrapeResponse)
	if err != nil {
		return nil, err
	}

	return ParseScrapeResponse(data)
}

// Scrape queries a tracker's scrape endpoint, dispatching to the HTTP or UDP
// implementation based on the announce URL scheme. It returns swarm-health
// counts keyed by raw info hash.
func Scrape(ctx context.Context, announceURL string, infoHashes ...[20]byte) (map[[20]byte]ScrapeStats, error) {
	// Dispatch on the parsed, lowercased scheme: a byte-prefix match would send
	// "httpx://" down the HTTP path and reject a valid "HTTP://".
	u, err := ParseAnnounceURL(announceURL)
	if err != nil {
		return nil, fmt.Errorf("scrape: %w", err)
	}
	if u.Scheme == "udp" {
		return UDPScrape(ctx, announceURL, infoHashes...)
	}
	return HTTPScrape(ctx, announceURL, infoHashes...)
}

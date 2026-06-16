// Package tracker implements BitTorrent tracker announcement protocols over HTTP and UDP.
package tracker

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

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
			sb.WriteString(fmt.Sprintf("%%%02X", c))
		}
	}
	return sb.String()
}

// ParseTrackerResponse decodes a bencoded tracker response.
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
		return nil, fmt.Errorf("tracker error: %s", failReason)
	}

	var interval int
	if intVal, ok := dict["interval"].(int64); ok {
		interval = int(intVal)
	} else {
		return nil, fmt.Errorf("missing or invalid interval")
	}

	var minInterval int
	if minIntVal, ok := dict["min interval"].(int64); ok {
		minInterval = int(minIntVal)
	}

	var warning string
	if warnVal, ok := dict["warning message"].(string); ok {
		warning = warnVal
	}

	var complete int
	if compVal, ok := dict["complete"].(int64); ok {
		complete = int(compVal)
	}

	var incomplete int
	if incompVal, ok := dict["incomplete"].(int64); ok {
		incomplete = int(incompVal)
	}

	var peers []Peer
	peersVal, exists := dict["peers"]
	if exists {
		if peersStr, ok := peersVal.(string); ok {
			// Compact IPv4 peers (6 bytes per peer)
			peersBytes := []byte(peersStr)
			if len(peersBytes)%6 != 0 {
				return nil, fmt.Errorf("compact peers length must be a multiple of 6, got %d", len(peersBytes))
			}
			numPeers := len(peersBytes) / 6
			for i := 0; i < numPeers; i++ {
				offset := i * 6
				ip := net.IP(peersBytes[offset : offset+4])
				port := binary.BigEndian.Uint16(peersBytes[offset+4 : offset+6])
				peers = append(peers, Peer{
					IP:   ip,
					Port: port,
				})
			}
		} else if peersList, ok := peersVal.([]interface{}); ok {
			// Non-compact peer list (list of dictionaries)
			for _, pVal := range peersList {
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
		peers6Bytes := []byte(peers6Val)
		if len(peers6Bytes)%18 != 0 {
			return nil, fmt.Errorf("compact peers6 length must be a multiple of 18, got %d", len(peers6Bytes))
		}
		numPeers6 := len(peers6Bytes) / 18
		for i := 0; i < numPeers6; i++ {
			offset := i * 18
			ip := net.IP(peers6Bytes[offset : offset+16])
			port := binary.BigEndian.Uint16(peers6Bytes[offset+16 : offset+18])
			peers = append(peers, Peer{
				IP:   ip,
				Port: port,
			})
		}
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
// scrape reply is a small bencoded dictionary; this ceiling stops a malicious or
// MITM'd tracker from streaming unbounded data into memory.
const maxScrapeResponse = 1 * 1024 * 1024

// ScrapeURL derives the scrape URL from an announce URL using the BEP 48
// convention: the final path segment must begin with "announce", and that
// "announce" prefix is replaced with "scrape" (e.g. "/announce" -> "/scrape",
// "/x/announce.php" -> "/x/scrape.php"). An error is returned when the URL has
// no path or the final segment does not begin with "announce", signalling that
// the tracker does not advertise scrape support.
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
		return nil, fmt.Errorf("tracker error: %s", failReason)
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
			stats.Complete = int(c)
		}
		if d, ok := fileDict["downloaded"].(int64); ok {
			stats.Downloaded = int(d)
		}
		if i, ok := fileDict["incomplete"].(int64); ok {
			stats.Incomplete = int(i)
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape returned HTTP %d", resp.StatusCode)
	}

	// Reading one byte past the cap lets us detect and reject an over-limit
	// response rather than silently truncating it.
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxScrapeResponse+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxScrapeResponse {
		return nil, fmt.Errorf("scrape response exceeds %d bytes", maxScrapeResponse)
	}

	return ParseScrapeResponse(data)
}

// Scrape queries a tracker's scrape endpoint, dispatching to the HTTP or UDP
// implementation based on the announce URL scheme. It returns swarm-health
// counts keyed by raw info hash.
func Scrape(ctx context.Context, announceURL string, infoHashes ...[20]byte) (map[[20]byte]ScrapeStats, error) {
	u, err := url.Parse(announceURL)
	if err != nil {
		return nil, fmt.Errorf("parsing announce URL: %w", err)
	}
	switch u.Scheme {
	case "udp":
		return UDPScrape(ctx, announceURL, infoHashes...)
	case "http", "https":
		return HTTPScrape(ctx, announceURL, infoHashes...)
	default:
		return nil, fmt.Errorf("unsupported tracker scheme %q for scrape", u.Scheme)
	}
}

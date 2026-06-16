// Package tracker implements BitTorrent tracker announcement protocols over HTTP and UDP.
package tracker

import (
	"encoding/binary"
	"fmt"
	"net"
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

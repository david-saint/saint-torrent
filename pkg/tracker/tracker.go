// Package tracker implements BitTorrent tracker announcement protocols over HTTP and UDP.
package tracker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
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

type bencodeValue interface{}

func parseBencode(data []byte) (bencodeValue, []byte, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("empty data")
	}
	switch data[0] {
	case 'i':
		// Integer
		end := bytes.IndexByte(data, 'e')
		if end == -1 {
			return nil, nil, fmt.Errorf("malformed integer")
		}
		val, err := strconv.ParseInt(string(data[1:end]), 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid integer: %v", err)
		}
		return val, data[end+1:], nil
	case 'l':
		// List
		var list []bencodeValue
		rest := data[1:]
		for len(rest) > 0 && rest[0] != 'e' {
			var val bencodeValue
			var err error
			val, rest, err = parseBencode(rest)
			if err != nil {
				return nil, nil, err
			}
			list = append(list, val)
		}
		if len(rest) == 0 {
			return nil, nil, fmt.Errorf("unterminated list")
		}
		return list, rest[1:], nil
	case 'd':
		// Dictionary
		dict := make(map[string]bencodeValue)
		rest := data[1:]
		for len(rest) > 0 && rest[0] != 'e' {
			// Key must be a string
			var keyVal bencodeValue
			var err error
			keyVal, rest, err = parseBencode(rest)
			if err != nil {
				return nil, nil, err
			}
			key, ok := keyVal.(string)
			if !ok {
				return nil, nil, fmt.Errorf("dict key must be string")
			}
			var val bencodeValue
			val, rest, err = parseBencode(rest)
			if err != nil {
				return nil, nil, err
			}
			dict[key] = val
		}
		if len(rest) == 0 {
			return nil, nil, fmt.Errorf("unterminated dict")
		}
		return dict, rest[1:], nil
	default:
		// String: <length>:<data>
		colon := bytes.IndexByte(data, ':')
		if colon == -1 {
			return nil, nil, fmt.Errorf("malformed string")
		}
		length, err := strconv.Atoi(string(data[:colon]))
		if err != nil {
			return nil, nil, fmt.Errorf("invalid string length: %v", err)
		}
		if length < 0 {
			return nil, nil, fmt.Errorf("negative string length: %d", length)
		}
		if length > len(data)-colon-1 {
			return nil, nil, fmt.Errorf("string length exceeds data size")
		}
		strVal := data[colon+1 : colon+1+length]
		return string(strVal), data[colon+1+length:], nil
	}
}

// ParseTrackerResponse decodes a bencoded tracker response.
func ParseTrackerResponse(data []byte) (*TrackerResponse, error) {
	val, rest, err := parseBencode(data)
	if err != nil {
		return nil, fmt.Errorf("bencode parsing error: %w", err)
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("bencode parsing error: trailing data after tracker response")
	}
	dict, ok := val.(map[string]bencodeValue)
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
		} else if peersList, ok := peersVal.([]bencodeValue); ok {
			// Non-compact peer list (list of dictionaries)
			for _, pVal := range peersList {
				pDict, ok := pVal.(map[string]bencodeValue)
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

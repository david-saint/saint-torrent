package peer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"

	"sainttorrent/pkg/bencode"
)

const (
	PEXFlagEncrypted byte = 1 << 0
	PEXFlagSeed      byte = 1 << 1
	PEXFlagUTP       byte = 1 << 2

	MaxPEXPeers = 1000
)

// PEXPeer is one peer endpoint in a BEP 11 Peer Exchange message.
type PEXPeer struct {
	IP    net.IP
	Port  uint16
	Flags byte
}

// PEXMessage represents a BEP 11 Peer Exchange message.
type PEXMessage struct {
	Added   []PEXPeer
	Dropped []PEXPeer
}

// ParsePEXMessage parses a ut_pex payload. It supports compact IPv4
// added/dropped lists plus their IPv6 counterparts (added6/dropped6).
func ParsePEXMessage(data []byte) (*PEXMessage, error) {
	decoded, err := bencode.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode PEX message: %w", err)
	}
	dict, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, errors.New("PEX message is not a dictionary")
	}

	msg := &PEXMessage{}
	remaining := MaxPEXPeers
	if added, err := parsePEXPeers(dict, "added", "added.f", 6, remaining); err != nil {
		return nil, err
	} else {
		msg.Added = append(msg.Added, added...)
		remaining -= len(added)
	}
	if added6, err := parsePEXPeers(dict, "added6", "added6.f", 18, remaining); err != nil {
		return nil, err
	} else {
		msg.Added = append(msg.Added, added6...)
		remaining -= len(added6)
	}
	if dropped, err := parsePEXPeers(dict, "dropped", "", 6, remaining); err != nil {
		return nil, err
	} else {
		msg.Dropped = append(msg.Dropped, dropped...)
		remaining -= len(dropped)
	}
	if dropped6, err := parsePEXPeers(dict, "dropped6", "", 18, remaining); err != nil {
		return nil, err
	} else {
		msg.Dropped = append(msg.Dropped, dropped6...)
	}

	return msg, nil
}

func parsePEXPeers(dict map[string]interface{}, peerKey string, flagsKey string, peerSize int, maxCount int) ([]PEXPeer, error) {
	raw, ok := dict[peerKey]
	if !ok {
		return nil, nil
	}
	compact, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("%q is not a compact peer string", peerKey)
	}
	compactBytes := []byte(compact)
	if len(compactBytes)%peerSize != 0 {
		return nil, fmt.Errorf("%q length must be a multiple of %d, got %d", peerKey, peerSize, len(compactBytes))
	}
	count := len(compactBytes) / peerSize
	if count > maxCount {
		return nil, fmt.Errorf("PEX message has too many peers: %q adds %d with only %d remaining", peerKey, count, maxCount)
	}

	flags := make([]byte, count)
	if flagsKey != "" {
		if rawFlags, ok := dict[flagsKey]; ok {
			flagString, ok := rawFlags.(string)
			if !ok {
				return nil, fmt.Errorf("%q is not a flag string", flagsKey)
			}
			flagBytes := []byte(flagString)
			if len(flagBytes) != count {
				return nil, fmt.Errorf("%q length = %d, want %d", flagsKey, len(flagBytes), count)
			}
			copy(flags, flagBytes)
		}
	}

	peers := make([]PEXPeer, 0, count)
	for i := 0; i < count; i++ {
		offset := i * peerSize
		var ip net.IP
		var port uint16
		if peerSize == 6 {
			ip = net.IPv4(compactBytes[offset], compactBytes[offset+1], compactBytes[offset+2], compactBytes[offset+3])
			port = binary.BigEndian.Uint16(compactBytes[offset+4 : offset+6])
		} else {
			ip = make(net.IP, net.IPv6len)
			copy(ip, compactBytes[offset:offset+net.IPv6len])
			port = binary.BigEndian.Uint16(compactBytes[offset+net.IPv6len : offset+peerSize])
		}
		peers = append(peers, PEXPeer{
			IP:    ip,
			Port:  port,
			Flags: flags[i],
		})
	}

	return peers, nil
}

// SerializePEXMessage serializes a BEP 11 Peer Exchange message.
func SerializePEXMessage(msg *PEXMessage) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("nil PEX message")
	}
	if len(msg.Added)+len(msg.Dropped) > MaxPEXPeers {
		return nil, fmt.Errorf("PEX message has too many peers: %d", len(msg.Added)+len(msg.Dropped))
	}

	payload := make(map[string]interface{})
	added4, added4Flags, added6, added6Flags := compactPEXPeers(msg.Added, true)
	if len(added4) > 0 {
		payload["added"] = added4
		payload["added.f"] = added4Flags
	}
	if len(added6) > 0 {
		payload["added6"] = added6
		payload["added6.f"] = added6Flags
	}

	dropped4, _, dropped6, _ := compactPEXPeers(msg.Dropped, false)
	if len(dropped4) > 0 {
		payload["dropped"] = dropped4
	}
	if len(dropped6) > 0 {
		payload["dropped6"] = dropped6
	}

	data, err := bencode.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to encode PEX message: %w", err)
	}
	return data, nil
}

func compactPEXPeers(peers []PEXPeer, includeFlags bool) (ipv4 []byte, ipv4Flags []byte, ipv6 []byte, ipv6Flags []byte) {
	for _, p := range peers {
		if p.Port == 0 || p.IP == nil || p.IP.IsUnspecified() {
			continue
		}
		if ip4 := p.IP.To4(); ip4 != nil {
			ipv4 = append(ipv4, ip4...)
			var portBuf [2]byte
			binary.BigEndian.PutUint16(portBuf[:], p.Port)
			ipv4 = append(ipv4, portBuf[:]...)
			if includeFlags {
				ipv4Flags = append(ipv4Flags, p.Flags)
			}
			continue
		}
		if ip16 := p.IP.To16(); ip16 != nil {
			ipv6 = append(ipv6, ip16...)
			var portBuf [2]byte
			binary.BigEndian.PutUint16(portBuf[:], p.Port)
			ipv6 = append(ipv6, portBuf[:]...)
			if includeFlags {
				ipv6Flags = append(ipv6Flags, p.Flags)
			}
		}
	}
	return ipv4, ipv4Flags, ipv6, ipv6Flags
}

// SendPEX sends a BEP 11 Peer Exchange message using the peer's advertised
// extension message ID.
func (c *Client) SendPEX(extMsgID byte, msg *PEXMessage) error {
	payload, err := SerializePEXMessage(msg)
	if err != nil {
		return err
	}
	return c.sendExtendedPayload(extMsgID, payload)
}

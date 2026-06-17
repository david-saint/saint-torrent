// Package utp implements the BEP 29 uTP transport.
package utp

import (
	"encoding/binary"
	"errors"
)

const (
	protocolVersion = 1
	headerSize      = 20
	maxPayloadSize  = 1180
)

type packetType byte

const (
	packetTypeData  packetType = 0
	packetTypeFin   packetType = 1
	packetTypeState packetType = 2
	packetTypeReset packetType = 3
	packetTypeSyn   packetType = 4
)

var errShortPacket = errors.New("utp: packet too short")

type packet struct {
	typ           packetType
	version       byte
	extension     byte
	connID        uint16
	timestamp     uint32
	timestampDiff uint32
	wndSize       uint32
	seqNr         uint16
	ackNr         uint16
	payload       []byte
}

// IsPacket reports whether data has a BEP 29 uTP header shape. DHT messages are
// bencoded and begin with 'd', so this check is sufficient for UDP demuxing.
func IsPacket(data []byte) bool {
	if len(data) < headerSize {
		return false
	}
	version := data[0] & 0x0f
	typ := data[0] >> 4
	return version == protocolVersion && typ <= byte(packetTypeSyn)
}

func parsePacket(data []byte) (packet, error) {
	if len(data) < headerSize {
		return packet{}, errShortPacket
	}
	p := packet{
		typ:           packetType(data[0] >> 4),
		version:       data[0] & 0x0f,
		extension:     data[1],
		connID:        binary.BigEndian.Uint16(data[2:4]),
		timestamp:     binary.BigEndian.Uint32(data[4:8]),
		timestampDiff: binary.BigEndian.Uint32(data[8:12]),
		wndSize:       binary.BigEndian.Uint32(data[12:16]),
		seqNr:         binary.BigEndian.Uint16(data[16:18]),
		ackNr:         binary.BigEndian.Uint16(data[18:20]),
	}
	if p.version != protocolVersion || p.typ > packetTypeSyn {
		return packet{}, errors.New("utp: invalid packet header")
	}

	offset := headerSize
	for ext := p.extension; ext != 0; {
		if len(data) < offset+2 {
			return packet{}, errShortPacket
		}
		next := data[offset]
		extLen := int(data[offset+1])
		offset += 2
		if len(data) < offset+extLen {
			return packet{}, errShortPacket
		}
		offset += extLen
		ext = next
	}
	p.payload = data[offset:]
	return p, nil
}

func (p packet) marshal() []byte {
	out := make([]byte, headerSize+len(p.payload))
	out[0] = byte(p.typ)<<4 | protocolVersion
	out[1] = p.extension
	binary.BigEndian.PutUint16(out[2:4], p.connID)
	binary.BigEndian.PutUint32(out[4:8], p.timestamp)
	binary.BigEndian.PutUint32(out[8:12], p.timestampDiff)
	binary.BigEndian.PutUint32(out[12:16], p.wndSize)
	binary.BigEndian.PutUint16(out[16:18], p.seqNr)
	binary.BigEndian.PutUint16(out[18:20], p.ackNr)
	copy(out[headerSize:], p.payload)
	return out
}

func seqLTE(a, b uint16) bool {
	return int16(b-a) >= 0
}

func seqLT(a, b uint16) bool {
	return a != b && seqLTE(a, b)
}

package peer

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MessageID is the type for BitTorrent peer message identifiers.
type MessageID byte

const (
	MsgChoke         MessageID = 0
	MsgUnchoke       MessageID = 1
	MsgInterested    MessageID = 2
	MsgNotInterested MessageID = 3
	MsgHave          MessageID = 4
	MsgBitfield      MessageID = 5
	MsgRequest       MessageID = 6
	MsgPiece         MessageID = 7
	MsgCancel        MessageID = 8
	// MsgPort carries a peer's DHT UDP port (BEP 5). Sent after the handshake to
	// DHT-capable peers; on receipt the advertised port plus the peer's source IP
	// is fed into the DHT routing table.
	MsgPort MessageID = 9
)

// Message represents a BitTorrent peer wire message.
type Message struct {
	ID      MessageID
	Payload []byte
}

// Handshake represents the initial BitTorrent connection handshake.
type Handshake struct {
	Pstr     string
	InfoHash [20]byte
	PeerID   [20]byte
	Reserved [8]byte
}

// Serialize serializes a peer message into bytes.
// A nil message is serialized as a Keep-Alive message (4 bytes of 0).
func (m *Message) Serialize() []byte {
	if m == nil {
		return make([]byte, 4)
	}
	length := uint32(len(m.Payload) + 1)
	buf := make([]byte, 4+length)
	binary.BigEndian.PutUint32(buf[0:4], length)
	buf[4] = byte(m.ID)
	copy(buf[5:], m.Payload)
	return buf
}

// MaxMessageLength is the limit on message sizes we are willing to parse (2MB).
const MaxMessageLength = 2 * 1024 * 1024

// ParseMessage parses a peer message from an io.Reader.
func ParseMessage(r io.Reader) (*Message, error) {
	lengthBuf := make([]byte, 4)
	_, err := io.ReadFull(r, lengthBuf)
	if err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBuf)
	if length == 0 {
		return nil, nil // Keep-Alive message
	}
	if length > MaxMessageLength {
		return nil, fmt.Errorf("message length %d exceeds maximum limit %d", length, MaxMessageLength)
	}

	messageBuf := make([]byte, length)
	_, err = io.ReadFull(r, messageBuf)
	if err != nil {
		return nil, err
	}

	return &Message{
		ID:      MessageID(messageBuf[0]),
		Payload: messageBuf[1:],
	}, nil
}

// Serialize serializes a Handshake into bytes.
func (h *Handshake) Serialize() []byte {
	buf := make([]byte, 68)
	buf[0] = byte(len(h.Pstr))
	copy(buf[1:20], h.Pstr)
	copy(buf[20:28], h.Reserved[:])
	copy(buf[28:48], h.InfoHash[:])
	copy(buf[48:68], h.PeerID[:])
	return buf
}

// ParseHandshake parses a Handshake from an io.Reader.
func ParseHandshake(r io.Reader) (*Handshake, error) {
	pstrlenBuf := make([]byte, 1)
	_, err := io.ReadFull(r, pstrlenBuf)
	if err != nil {
		return nil, err
	}
	pstrlen := int(pstrlenBuf[0])
	if pstrlen != 19 {
		return nil, fmt.Errorf("invalid pstrlen: expected 19, got %d", pstrlen)
	}

	pstrBuf := make([]byte, 19)
	_, err = io.ReadFull(r, pstrBuf)
	if err != nil {
		return nil, err
	}
	pstr := string(pstrBuf)
	if pstr != "BitTorrent protocol" {
		return nil, fmt.Errorf("invalid protocol: expected 'BitTorrent protocol', got %q", pstr)
	}

	reserved := make([]byte, 8)
	_, err = io.ReadFull(r, reserved)
	if err != nil {
		return nil, err
	}
	var resBytes [8]byte
	copy(resBytes[:], reserved)

	var infoHash [20]byte
	_, err = io.ReadFull(r, infoHash[:])
	if err != nil {
		return nil, err
	}

	var peerID [20]byte
	_, err = io.ReadFull(r, peerID[:])
	if err != nil {
		return nil, err
	}

	return &Handshake{
		Pstr:     pstr,
		InfoHash: infoHash,
		PeerID:   peerID,
		Reserved: resBytes,
	}, nil
}

package peer

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
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

	MsgSuggestPiece  MessageID = 13
	MsgHaveAll       MessageID = 14
	MsgHaveNone      MessageID = 15
	MsgRejectRequest MessageID = 16
	MsgAllowedFast   MessageID = 17
)

const (
	FastExtensionReservedByte = 7
	FastExtensionReservedBit  = 0x04
)

// Message represents a BitTorrent peer wire message.
type Message struct {
	ID      MessageID
	Payload []byte
	// pooled is the backing buffer this message's Payload was read into, when it
	// came from the inbound buffer pool (see readMessage). It is nil for messages
	// built in memory (Serialize round-trips, ParseMessage) or read into a fresh
	// heap allocation because they were too large to pool. Release returns it.
	pooled *[]byte
}

// maxPooledMessageLen is the capacity of buffers recycled through inboundBufPool:
// a full 16 KiB block payload plus its 9-byte piece header (1 id + 4 index +
// 4 begin). Piece messages dominate the inbound hot path, so pooling their
// buffers turns the old per-message heap allocation into buffer reuse. Rarer,
// larger messages (a big bitfield, an extension payload) fall back to a one-off
// heap allocation and are never returned to the pool.
const maxPooledMessageLen = 9 + 16*1024

// inboundBufPool recycles the buffers ParseMessage-style reads decode into. It
// holds *[]byte rather than []byte so returning a buffer boxes only a pointer,
// keeping Put itself allocation-free on the wire hot path.
var inboundBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, maxPooledMessageLen)
		return &b
	},
}

// Release returns a pooled inbound-message buffer to the shared pool. Call it
// exactly once, after the message's Payload is no longer referenced (for a piece
// message, after its block has been copied into the piece buffer). It is a no-op
// for messages whose buffer was heap-allocated, and safe on a nil receiver.
func (m *Message) Release() {
	if m == nil || m.pooled == nil {
		return
	}
	buf := m.pooled
	m.pooled = nil
	m.Payload = nil
	inboundBufPool.Put(buf)
}

// Handshake represents the initial BitTorrent connection handshake.
type Handshake struct {
	Pstr     string
	InfoHash [20]byte
	PeerID   [20]byte
	Reserved [8]byte
}

// EnableFastExtension marks a handshake as supporting BEP 6 Fast Extension.
func EnableFastExtension(reserved *[8]byte) {
	if reserved == nil {
		return
	}
	reserved[FastExtensionReservedByte] |= FastExtensionReservedBit
}

// SupportsFastExtension reports whether the BEP 6 Fast Extension bit is set.
func SupportsFastExtension(reserved [8]byte) bool {
	return reserved[FastExtensionReservedByte]&FastExtensionReservedBit != 0
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

// readMessage parses a peer message like ParseMessage, but keeps the wire hot
// path allocation-free: the 4-byte length prefix is read into the caller-owned
// lengthBuf scratch, and the payload is read into a buffer borrowed from
// inboundBufPool when it fits. The returned Message owns that pooled buffer until
// Release is called; ownership of a piece block passes to the downloader, which
// releases it after copying the block into the piece buffer. Oversized messages
// fall back to a fresh heap allocation with Release as a no-op. lengthBuf must be
// at least 4 bytes and is only valid for the duration of the call.
func readMessage(r io.Reader, lengthBuf []byte) (*Message, error) {
	if _, err := io.ReadFull(r, lengthBuf[:4]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBuf[:4])
	if length == 0 {
		return nil, nil // Keep-Alive message
	}
	if length > MaxMessageLength {
		return nil, fmt.Errorf("message length %d exceeds maximum limit %d", length, MaxMessageLength)
	}

	var (
		messageBuf []byte
		pooled     *[]byte
	)
	if length <= maxPooledMessageLen {
		pooled = inboundBufPool.Get().(*[]byte)
		messageBuf = (*pooled)[:length]
	} else {
		messageBuf = make([]byte, length)
	}

	if _, err := io.ReadFull(r, messageBuf); err != nil {
		if pooled != nil {
			inboundBufPool.Put(pooled)
		}
		return nil, err
	}

	return &Message{
		ID:      MessageID(messageBuf[0]),
		Payload: messageBuf[1:],
		pooled:  pooled,
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

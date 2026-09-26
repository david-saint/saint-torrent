package peer

import (
	"bufio"
	"encoding/binary"
	"errors"
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

// maxBitfieldBytes is the largest bitfield any torrent can need: one bit for
// each of torrent.MaxPieceCount (1<<22) pieces.
const maxBitfieldBytes = (1 << 22) / 8

// MaxMessageLength is the largest message (id byte included) we parse: the
// bitfield of a torrent with the most pieces we accept. Every other message type
// has a much smaller limit of its own; see messageLengthRules.
const MaxMessageLength = 1 + maxBitfieldBytes

// defaultBitfieldLimit is the bitfield size a Client accepts until
// SetBitfieldLimit narrows it: the largest bitfield a magnet can need, one bit
// for each of the MaxMetadataSize/20 piece hashes its info dict can hold.
const defaultBitfieldLimit = (MaxMetadataSize/20 + 7) / 8

// ErrInvalidMessageLength reports a peer message whose length does not fit its
// type (or exceeds MaxMessageLength). Readers return it wrapped with the message
// id and length; the connection should be dropped.
var ErrInvalidMessageLength = errors.New("invalid peer message length")

// lengthRule is the inclusive range of lengths (id byte included) allowed for
// one message id.
type lengthRule struct {
	min, max uint32
}

// messageLengthRules holds the allowed length range of every message id, so the
// reader validates a frame with one table lookup. Fixed-size messages must have
// their exact size (libtorrent drops a peer for anything else too); a piece
// carries at most one 16 KiB block; an extended message at most the largest
// BEP 10 payload we decode (MaxExtHandshakeSize). The bitfield's upper bound
// depends on the torrent and is supplied by the caller. Ids we do not implement
// may be up to maxPooledMessageLen, so an unknown extension costs a pooled
// buffer at most.
var messageLengthRules = func() (rules [256]lengthRule) {
	for i := range rules {
		rules[i] = lengthRule{1, maxPooledMessageLen}
	}
	for _, id := range []MessageID{MsgChoke, MsgUnchoke, MsgInterested, MsgNotInterested, MsgHaveAll, MsgHaveNone} {
		rules[id] = lengthRule{1, 1}
	}
	for _, id := range []MessageID{MsgHave, MsgSuggestPiece, MsgAllowedFast} {
		rules[id] = lengthRule{5, 5}
	}
	for _, id := range []MessageID{MsgRequest, MsgCancel, MsgRejectRequest} {
		rules[id] = lengthRule{13, 13}
	}
	rules[MsgPort] = lengthRule{3, 3}
	rules[MsgPiece] = lengthRule{9, 9 + maxBlockLength}
	rules[MsgBitfield] = lengthRule{2, 0} // max: 1 + the caller's bitfield limit
	rules[MsgExtended] = lengthRule{2, 2 + MaxExtHandshakeSize}
	return rules
}()

// maxBlockLength is the largest block a piece message may carry (16 KiB, the
// block size every client requests).
const maxBlockLength = 16 * 1024

// checkMessageLength reports whether length (id byte included) is allowed for a
// message with the given id, where bitfieldLimit is the largest bitfield payload
// accepted. It allocates only when it rejects.
func checkMessageLength(id MessageID, length uint32, bitfieldLimit int) error {
	rule := messageLengthRules[id]
	maxLen := rule.max
	if id == MsgBitfield {
		maxLen = 1 + uint32(min(max(bitfieldLimit, 1), maxBitfieldBytes))
	}
	if length < rule.min || length > maxLen {
		return fmt.Errorf("%w: id %d, length %d", ErrInvalidMessageLength, id, length)
	}
	return nil
}

// ParseMessage parses a peer message from an io.Reader. It checks only the
// overall MaxMessageLength cap; the connection reader (Client.ReadMessage) also
// checks each message against the limit of its type before allocating.
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
		return nil, fmt.Errorf("%w: length %d exceeds maximum limit %d", ErrInvalidMessageLength, length, MaxMessageLength)
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
// releases it after copying the block into the piece buffer. lengthBuf must be at
// least 4 bytes and is only valid for the duration of the call.
//
// Every message is checked against the length limit of its type (see
// messageLengthRules; bitfieldLimit bounds a bitfield's payload) and rejected with
// ErrInvalidMessageLength. A message that fits a pooled buffer is checked after
// the read, which costs the block path one table lookup; a larger one is checked
// before anything is allocated for it, from its id byte peeked out of r's buffer,
// so a peer cannot make us allocate or read a payload its type does not allow.
func readMessage(r *bufio.Reader, lengthBuf []byte, bitfieldLimit int) (*Message, error) {
	if _, err := io.ReadFull(r, lengthBuf[:4]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBuf[:4])
	if length == 0 {
		return nil, nil // Keep-Alive message
	}
	if length > MaxMessageLength {
		return nil, fmt.Errorf("%w: length %d exceeds maximum limit %d", ErrInvalidMessageLength, length, MaxMessageLength)
	}

	if length <= maxPooledMessageLen {
		pooled := inboundBufPool.Get().(*[]byte)
		messageBuf := (*pooled)[:length]
		if _, err := io.ReadFull(r, messageBuf); err != nil {
			inboundBufPool.Put(pooled)
			return nil, err
		}
		id := MessageID(messageBuf[0])
		if err := checkMessageLength(id, length, bitfieldLimit); err != nil {
			inboundBufPool.Put(pooled)
			return nil, err
		}
		return &Message{
			ID:      id,
			Payload: messageBuf[1:],
			pooled:  pooled,
		}, nil
	}

	// Too large to pool: validate the id before allocating. Peek leaves the id
	// byte in the reader's buffer, where the payload read below picks it up.
	idByte, err := r.Peek(1)
	if err != nil {
		return nil, err
	}
	if err := checkMessageLength(MessageID(idByte[0]), length, bitfieldLimit); err != nil {
		return nil, err
	}
	messageBuf := make([]byte, length)
	if _, err := io.ReadFull(r, messageBuf); err != nil {
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

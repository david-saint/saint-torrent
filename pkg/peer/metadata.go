package peer

import (
	"encoding/binary"
	"errors"
	"fmt"

	"sainttorrent/pkg/bencode"
)

// Extension message IDs for BEP 10 (Extension Protocol).
const (
	MsgExtended MessageID = 20

	ExtHandshake byte = 0

	ExtNameMetadata = "ut_metadata"
	ExtNamePEX      = "ut_pex"

	LocalMetadataExtID = 1
	LocalPEXExtID      = 2
)

// MetadataMsg types for BEP 9 (Metadata Exchange).
const (
	MetadataRequest = 0
	MetadataData    = 1
	MetadataReject  = 2
)

// MetadataBlockSize is the standard block size (16 KiB) per BEP 9.
const MetadataBlockSize = 16384

// MaxMetadataSize caps peer-advertised torrent metadata before allocation.
const MaxMetadataSize = 16 * 1024 * 1024

const maxMetadataPieces = MaxMetadataSize / MetadataBlockSize

// Size caps for BEP 10 sub-messages, checked before any bencode decode. The wire
// cap (MaxMessageLength) is sized for piece blocks, and decoding a 2 MiB payload of
// tiny containers builds a tree ~50x its size, so control messages get their own
// limits. Real extension handshakes are well under 1 KiB, and a ut_metadata data
// message is a small dict plus at most one 16 KiB block (libtorrent drops anything
// over 17 KiB too).
const (
	MaxExtHandshakeSize    = 64 * 1024
	MaxMetadataMessageSize = MetadataBlockSize + 1024
)

// ExtensionHandshake represents the BEP 10 extension handshake payload.
// It carries the "m" dictionary mapping extension names to message IDs,
// the total metadata size, an optional client identifier, and the optional
// number of outstanding requests the sender is willing to queue.
type ExtensionHandshake struct {
	Extensions   map[string]int // m dict: extension name -> message ID
	MetadataSize int            // metadata_size field
	ClientName   string         // v field (optional)
	RequestQueue int            // reqq field (optional; 0 when absent or invalid)
}

// maxRequestQueue caps a parsed reqq value; real clients advertise a few hundred
// to a few thousand.
const maxRequestQueue = 1 << 20

// ParseExtensionHandshake parses a BEP 10 extension handshake from bencoded data.
// The input must be a bencoded dictionary containing at least an "m" key.
func ParseExtensionHandshake(data []byte) (*ExtensionHandshake, error) {
	if len(data) > MaxExtHandshakeSize {
		return nil, fmt.Errorf("extension handshake too large: %d bytes", len(data))
	}
	decoded, err := bencode.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode extension handshake: %w", err)
	}

	dict, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, errors.New("extension handshake is not a dictionary")
	}

	hs := &ExtensionHandshake{
		Extensions: make(map[string]int),
	}

	// Parse the "m" dictionary (extension name -> message ID mapping).
	if mVal, exists := dict["m"]; exists {
		mDict, ok := mVal.(map[string]interface{})
		if !ok {
			return nil, errors.New("'m' value is not a dictionary")
		}
		for name, idVal := range mDict {
			id, ok := idVal.(int64)
			if !ok {
				return nil, fmt.Errorf("extension ID for %q is not an integer", name)
			}
			if id < 0 || id > 255 {
				return nil, fmt.Errorf("extension ID for %q is out of range: %d", name, id)
			}
			if id == 0 {
				continue
			}
			hs.Extensions[name] = int(id)
		}
	}

	// Parse optional metadata_size.
	if msVal, exists := dict["metadata_size"]; exists {
		ms, ok := msVal.(int64)
		if !ok {
			return nil, errors.New("'metadata_size' is not an integer")
		}
		if ms < 0 || ms > int64(MaxMetadataSize) {
			return nil, fmt.Errorf("'metadata_size' out of range: %d", ms)
		}
		hs.MetadataSize = int(ms)
	}

	// Parse optional client name.
	if vVal, exists := dict["v"]; exists {
		v, ok := vVal.(string)
		if !ok {
			return nil, errors.New("'v' is not a string")
		}
		hs.ClientName = v
	}

	// reqq is advisory, so a malformed value is ignored rather than failing the
	// whole handshake.
	if rVal, exists := dict["reqq"]; exists {
		if reqq, ok := rVal.(int64); ok && reqq > 0 {
			hs.RequestQueue = int(min(reqq, maxRequestQueue))
		}
	}

	return hs, nil
}

// SerializeExtensionHandshake creates the bencoded extension handshake payload.
// We advertise ut_metadata support with the given message ID and, if known,
// the total metadata size.
func SerializeExtensionHandshake(utMetadataID int, metadataSize int) ([]byte, error) {
	return SerializeExtensionHandshakeWithExtensions(map[string]int{
		ExtNameMetadata: utMetadataID,
	}, metadataSize)
}

// SerializeExtensionHandshakeWithExtensions creates the bencoded extension
// handshake payload for an arbitrary BEP 10 extension map.
func SerializeExtensionHandshakeWithExtensions(extensions map[string]int, metadataSize int) ([]byte, error) {
	return (&ExtensionHandshake{Extensions: extensions, MetadataSize: metadataSize}).Serialize()
}

// Serialize creates the bencoded extension handshake payload. Zero-valued
// optional fields are left out.
func (hs *ExtensionHandshake) Serialize() ([]byte, error) {
	mDict := make(map[string]interface{}, len(hs.Extensions))
	for name, id := range hs.Extensions {
		if name == "" {
			return nil, errors.New("extension name cannot be empty")
		}
		if id < 0 || id > 255 {
			return nil, fmt.Errorf("extension ID for %q is out of range: %d", name, id)
		}
		if id == 0 {
			continue
		}
		mDict[name] = id
	}
	payload := map[string]interface{}{
		"m": mDict,
	}
	if hs.MetadataSize > 0 {
		payload["metadata_size"] = hs.MetadataSize
	}
	if hs.ClientName != "" {
		payload["v"] = hs.ClientName
	}
	if hs.RequestQueue > 0 {
		payload["reqq"] = hs.RequestQueue
	}

	data, err := bencode.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to encode extension handshake: %w", err)
	}
	return data, nil
}

// MetadataMessage represents a BEP 9 metadata exchange message.
type MetadataMessage struct {
	MsgType   int    // 0=request, 1=data, 2=reject
	Piece     int    // piece index (0-based)
	TotalSize int    // optional total_size for data messages
	Data      []byte // only present for msg_type=1 (data)
}

// ParseMetadataMessage parses a ut_metadata message payload.
//
// For request and reject messages the payload is a bencoded dictionary.
// For data messages (msg_type=1) the bencoded dictionary is followed by
// the raw metadata piece bytes. Because bencode.Unmarshal rejects trailing
// data, we locate the dictionary boundary ourselves and split the payload.
func ParseMetadataMessage(data []byte) (*MetadataMessage, error) {
	if len(data) == 0 {
		return nil, errors.New("empty metadata message")
	}
	if len(data) > MaxMetadataMessageSize {
		return nil, fmt.Errorf("metadata message too large: %d bytes", len(data))
	}

	// Find the end of the bencoded dictionary so we can separate the
	// optional trailing piece data (present in msg_type=1 messages).
	dictEnd, err := bencodedDictSpan(data)
	if err != nil {
		return nil, fmt.Errorf("failed to find dictionary boundary: %w", err)
	}

	decoded, err := bencode.Unmarshal(data[:dictEnd])
	if err != nil {
		return nil, fmt.Errorf("failed to decode metadata message: %w", err)
	}

	dict, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, errors.New("metadata message is not a dictionary")
	}

	msgTypeVal, exists := dict["msg_type"]
	if !exists {
		return nil, errors.New("missing 'msg_type' in metadata message")
	}
	msgType, ok := msgTypeVal.(int64)
	if !ok {
		return nil, errors.New("'msg_type' is not an integer")
	}
	if msgType < MetadataRequest || msgType > MetadataReject {
		return nil, fmt.Errorf("unknown metadata msg_type: %d", msgType)
	}

	pieceVal, exists := dict["piece"]
	if !exists {
		return nil, errors.New("missing 'piece' in metadata message")
	}
	piece, ok := pieceVal.(int64)
	if !ok {
		return nil, errors.New("'piece' is not an integer")
	}
	if piece < 0 || piece >= int64(maxMetadataPieces) {
		return nil, fmt.Errorf("metadata piece index out of bounds: %d", piece)
	}

	msg := &MetadataMessage{
		MsgType: int(msgType),
		Piece:   int(piece),
	}

	if totalSizeVal, exists := dict["total_size"]; exists {
		totalSize, ok := totalSizeVal.(int64)
		if !ok {
			return nil, errors.New("'total_size' is not an integer")
		}
		if totalSize < 0 || totalSize > int64(MaxMetadataSize) {
			return nil, fmt.Errorf("'total_size' out of range: %d", totalSize)
		}
		msg.TotalSize = int(totalSize)
	}

	// Data messages carry raw piece bytes after the bencoded dictionary.
	if msg.MsgType == MetadataData {
		trailing := data[dictEnd:]
		if len(trailing) == 0 {
			return nil, errors.New("data message has no piece data")
		}
		if len(trailing) > MetadataBlockSize {
			return nil, fmt.Errorf("metadata data block too large: %d bytes", len(trailing))
		}
		msg.Data = trailing
	} else if len(data[dictEnd:]) != 0 {
		return nil, errors.New("request/reject metadata message has trailing data")
	}

	return msg, nil
}

// SerializeMetadataRequest creates a bencoded metadata request message
// for the given piece index.
func SerializeMetadataRequest(piece int) ([]byte, error) {
	if piece < 0 || piece >= maxMetadataPieces {
		return nil, fmt.Errorf("metadata piece index out of bounds: %d", piece)
	}
	payload := map[string]interface{}{
		"msg_type": MetadataRequest,
		"piece":    piece,
	}
	data, err := bencode.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to encode metadata request: %w", err)
	}
	return data, nil
}

// SendExtHandshake sends the BEP 10 extension handshake on the client connection.
// It constructs a Message with ID = MsgExtended and the ExtHandshake sub-ID (0),
// followed by the bencoded handshake payload.
func (c *Client) SendExtHandshake(utMetadataID int, metadataSize int) error {
	payload, err := SerializeExtensionHandshake(utMetadataID, metadataSize)
	if err != nil {
		return err
	}

	return c.sendExtendedPayload(ExtHandshake, payload)
}

// SendExtHandshakeWithExtensions sends a BEP 10 extension handshake advertising
// the provided local extension IDs.
func (c *Client) SendExtHandshakeWithExtensions(extensions map[string]int, metadataSize int) error {
	payload, err := SerializeExtensionHandshakeWithExtensions(extensions, metadataSize)
	if err != nil {
		return err
	}

	return c.sendExtendedPayload(ExtHandshake, payload)
}

// SendExtensionHandshake sends a BEP 10 extension handshake built from hs.
func (c *Client) SendExtensionHandshake(hs *ExtensionHandshake) error {
	payload, err := hs.Serialize()
	if err != nil {
		return err
	}
	return c.sendExtendedPayload(ExtHandshake, payload)
}

func (c *Client) sendExtendedPayload(extMsgID byte, payload []byte) error {
	// Extension message payload: [ext_msg_id (1 byte)][bencoded payload]
	msgPayload := make([]byte, 1+len(payload))
	msgPayload[0] = extMsgID
	copy(msgPayload[1:], payload)

	return c.SendMessage(&Message{
		ID:      MsgExtended,
		Payload: msgPayload,
	})
}

// SendMetadataRequest sends a BEP 9 metadata request for the given piece
// using the peer's advertised extension message ID.
func (c *Client) SendMetadataRequest(extMsgID byte, piece int) error {
	payload, err := SerializeMetadataRequest(piece)
	if err != nil {
		return err
	}

	// Extension message payload: [ext_msg_id (1 byte)][bencoded payload]
	msgPayload := make([]byte, 1+len(payload))
	msgPayload[0] = extMsgID
	copy(msgPayload[1:], payload)

	return c.SendMessage(&Message{
		ID:      MsgExtended,
		Payload: msgPayload,
	})
}

// bencodedDictSpan returns the number of bytes occupied by the bencoded
// dictionary at the start of data. This is needed to split BEP 9 data
// messages where piece bytes follow the dictionary. It delegates to the
// shared bencode.ValueSpan so there is a single span implementation.
func bencodedDictSpan(data []byte) (int, error) {
	if len(data) == 0 || data[0] != 'd' {
		return 0, errors.New("not a bencoded dictionary")
	}
	return bencode.ValueSpan(data)
}

// SendMetadataData sends a BEP 9 metadata piece message. Like SendPiece it frames
// the message straight into the write buffer, so the up-to-16 KiB block is not
// copied into a payload and then again by Message.Serialize.
func (c *Client) SendMetadataData(extMsgID byte, piece int, totalSize int, data []byte) error {
	payloadDict := map[string]interface{}{
		"msg_type":   MetadataData,
		"piece":      piece,
		"total_size": int64(totalSize),
	}
	dictBytes, err := bencode.Marshal(payloadDict)
	if err != nil {
		return err
	}

	// 4-byte length prefix + message ID + extended message ID.
	var hdr [6]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(2+len(dictBytes)+len(data)))
	hdr[4] = byte(MsgExtended)
	hdr[5] = extMsgID

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.armWriteDeadlineLocked()
	for _, part := range [][]byte{hdr[:], dictBytes, data} {
		if _, err := c.w.Write(part); err != nil {
			return c.writeFailedLocked(err)
		}
	}
	return c.writeFailedLocked(c.w.Flush())
}

// SendMetadataReject sends a BEP 9 metadata reject message.
func (c *Client) SendMetadataReject(extMsgID byte, piece int) error {
	payloadDict := map[string]interface{}{
		"msg_type": MetadataReject,
		"piece":    piece,
	}
	dictBytes, err := bencode.Marshal(payloadDict)
	if err != nil {
		return err
	}

	msgPayload := make([]byte, 1+len(dictBytes))
	msgPayload[0] = extMsgID
	copy(msgPayload[1:], dictBytes)

	return c.SendMessage(&Message{
		ID:      MsgExtended,
		Payload: msgPayload,
	})
}

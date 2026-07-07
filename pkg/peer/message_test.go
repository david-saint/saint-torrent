package peer

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestSerializeMessage(t *testing.T) {
	tests := []struct {
		name     string
		msg      *Message
		expected []byte
	}{
		{
			name:     "Keep-Alive",
			msg:      nil,
			expected: []byte{0, 0, 0, 0},
		},
		{
			name:     "Choke",
			msg:      &Message{ID: MsgChoke},
			expected: []byte{0, 0, 0, 1, 0},
		},
		{
			name:     "Unchoke",
			msg:      &Message{ID: MsgUnchoke},
			expected: []byte{0, 0, 0, 1, 1},
		},
		{
			name:     "Interested",
			msg:      &Message{ID: MsgInterested},
			expected: []byte{0, 0, 0, 1, 2},
		},
		{
			name:     "Have",
			msg:      &Message{ID: MsgHave, Payload: []byte{0, 0, 0, 5}},
			expected: []byte{0, 0, 0, 5, 4, 0, 0, 0, 5},
		},
		{
			name:     "Request",
			msg:      &Message{ID: MsgRequest, Payload: []byte{0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3}},
			expected: []byte{0, 0, 0, 13, 6, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3},
		},
		{
			name:     "SuggestPiece",
			msg:      &Message{ID: MsgSuggestPiece, Payload: []byte{0, 0, 0, 7}},
			expected: []byte{0, 0, 0, 5, 13, 0, 0, 0, 7},
		},
		{
			name:     "HaveAll",
			msg:      &Message{ID: MsgHaveAll},
			expected: []byte{0, 0, 0, 1, 14},
		},
		{
			name:     "HaveNone",
			msg:      &Message{ID: MsgHaveNone},
			expected: []byte{0, 0, 0, 1, 15},
		},
		{
			name:     "RejectRequest",
			msg:      &Message{ID: MsgRejectRequest, Payload: []byte{0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3}},
			expected: []byte{0, 0, 0, 13, 16, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3},
		},
		{
			name:     "AllowedFast",
			msg:      &Message{ID: MsgAllowedFast, Payload: []byte{0, 0, 0, 9}},
			expected: []byte{0, 0, 0, 5, 17, 0, 0, 0, 9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.msg.Serialize()
			if !bytes.Equal(got, tt.expected) {
				t.Errorf("Serialize() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestFastExtensionReservedBit(t *testing.T) {
	var reserved [8]byte
	if SupportsFastExtension(reserved) {
		t.Fatal("empty reserved bytes unexpectedly advertise fast extension")
	}
	EnableFastExtension(&reserved)
	if !SupportsFastExtension(reserved) {
		t.Fatal("reserved bytes do not advertise fast extension after EnableFastExtension")
	}
	if reserved[FastExtensionReservedByte] != FastExtensionReservedBit {
		t.Fatalf("reserved fast byte = %#x, want %#x", reserved[FastExtensionReservedByte], FastExtensionReservedBit)
	}
}

func TestParseMessage(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		expectedMsg *Message
		expectErr   bool
	}{
		{
			name:        "Keep-Alive",
			input:       []byte{0, 0, 0, 0},
			expectedMsg: nil,
			expectErr:   false,
		},
		{
			name:        "Choke",
			input:       []byte{0, 0, 0, 1, 0},
			expectedMsg: &Message{ID: MsgChoke, Payload: []byte{}},
			expectErr:   false,
		},
		{
			name:        "Have",
			input:       []byte{0, 0, 0, 5, 4, 0, 0, 0, 5},
			expectedMsg: &Message{ID: MsgHave, Payload: []byte{0, 0, 0, 5}},
			expectErr:   false,
		},
		{
			name:      "Incomplete length prefix",
			input:     []byte{0, 0, 0},
			expectErr: true,
		},
		{
			name:      "Incomplete payload",
			input:     []byte{0, 0, 0, 5, 4, 0},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.input)
			got, err := ParseMessage(r)
			if (err != nil) != tt.expectErr {
				t.Fatalf("ParseMessage() error = %v, expectErr %v", err, tt.expectErr)
			}
			if tt.expectErr {
				return
			}
			if tt.expectedMsg == nil {
				if got != nil {
					t.Errorf("Expected nil message, got %v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("Expected message, got nil")
			}
			if got.ID != tt.expectedMsg.ID {
				t.Errorf("Expected ID %v, got %v", tt.expectedMsg.ID, got.ID)
			}
			if !bytes.Equal(got.Payload, tt.expectedMsg.Payload) {
				t.Errorf("Expected payload %v, got %v", tt.expectedMsg.Payload, got.Payload)
			}
		})
	}
}

// pieceWire builds the on-wire bytes for a piece message carrying the given block.
func pieceWire(index, begin uint32, block []byte) []byte {
	length := uint32(9 + len(block)) // id + index + begin + block
	buf := make([]byte, 4+length)
	binary.BigEndian.PutUint32(buf[0:4], length)
	buf[4] = byte(MsgPiece)
	binary.BigEndian.PutUint32(buf[5:9], index)
	binary.BigEndian.PutUint32(buf[9:13], begin)
	copy(buf[13:], block)
	return buf
}

// TestReadMessageMatchesParseMessage checks the pooled reader decodes the same
// fields as the allocating ParseMessage across representative message shapes.
func TestReadMessageMatchesParseMessage(t *testing.T) {
	inputs := [][]byte{
		{0, 0, 0, 0},                // keep-alive
		{0, 0, 0, 1, 0},             // choke
		{0, 0, 0, 5, 4, 0, 0, 0, 5}, // have
		pieceWire(3, 16384, []byte("a block of data")),
	}
	for i, in := range inputs {
		want, werr := ParseMessage(bytes.NewReader(in))
		got, gerr := readMessage(bytes.NewReader(in), make([]byte, 4))
		if (werr == nil) != (gerr == nil) {
			t.Fatalf("input %d: error mismatch: ParseMessage=%v readMessage=%v", i, werr, gerr)
		}
		if want == nil {
			if got != nil {
				t.Fatalf("input %d: expected nil message, got %v", i, got)
			}
			continue
		}
		if got.ID != want.ID || !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("input %d: got {ID:%d Payload:%v}, want {ID:%d Payload:%v}", i, got.ID, got.Payload, want.ID, want.Payload)
		}
	}
}

// TestReadMessagePoolsBlockBuffers verifies that a block-sized message is read
// into a pooled buffer and that Release returns that exact buffer to the pool.
func TestReadMessagePoolsBlockBuffers(t *testing.T) {
	block := make([]byte, 16384)
	for i := range block {
		block[i] = byte(i)
	}
	in := pieceWire(1, 0, block) // length == maxPooledMessageLen, poolable

	msg, err := readMessage(bytes.NewReader(in), make([]byte, 4))
	if err != nil {
		t.Fatalf("readMessage failed: %v", err)
	}
	if msg.pooled == nil {
		t.Fatal("expected a pooled backing buffer for a block-sized message")
	}
	if !bytes.Equal(msg.Payload[8:], block) {
		t.Fatal("payload block does not round-trip")
	}

	ptr := msg.pooled
	msg.Release()
	if msg.pooled != nil || msg.Payload != nil {
		t.Fatal("Release must clear the pooled buffer and payload")
	}
	// Put then Get on the same goroutine returns the private-slotted buffer, so the
	// just-released buffer must be the next one handed out.
	if reused := inboundBufPool.Get().(*[]byte); reused != ptr {
		t.Fatal("released buffer was not returned to the pool")
	} else {
		inboundBufPool.Put(reused)
	}

	// Release is safe to call again (and on a heap-backed message / nil receiver).
	msg.Release()
	var nilMsg *Message
	nilMsg.Release()
}

// TestReadMessageOversizedNotPooled checks messages larger than the pooled buffer
// size fall back to a heap allocation with Release as a no-op.
func TestReadMessageOversizedNotPooled(t *testing.T) {
	block := make([]byte, maxPooledMessageLen) // 9-byte header pushes length over the cap
	in := pieceWire(0, 0, block)
	msg, err := readMessage(bytes.NewReader(in), make([]byte, 4))
	if err != nil {
		t.Fatalf("readMessage failed: %v", err)
	}
	if msg.pooled != nil {
		t.Fatal("oversized message must not borrow a pooled buffer")
	}
	if len(msg.Payload) != 8+len(block) {
		t.Fatalf("unexpected payload length %d", len(msg.Payload))
	}
	msg.Release() // no-op, must not panic
}

// TestReadMessageReleaseOnShortReadReclaimsBuffer ensures a truncated payload
// returns its borrowed buffer to the pool rather than leaking it.
func TestReadMessageReleaseOnShortReadReclaimsBuffer(t *testing.T) {
	// Advertise a poolable length but supply fewer payload bytes than promised.
	in := []byte{0, 0, 0, 10, 7, 1, 2, 3} // length 10, only 3 payload bytes follow
	if _, err := readMessage(bytes.NewReader(in), make([]byte, 4)); err == nil {
		t.Fatal("expected a short-read error")
	}
	// The borrowed buffer must have been Put back on the error path; draining it
	// here must not observe a wrongly-sized buffer.
	if bp := inboundBufPool.Get().(*[]byte); cap(*bp) != maxPooledMessageLen {
		t.Fatalf("pool returned a buffer of cap %d, want %d", cap(*bp), maxPooledMessageLen)
	} else {
		inboundBufPool.Put(bp)
	}
}

func TestHandshakeSerializeAndParse(t *testing.T) {
	infoHash := [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	peerID := [20]byte{21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40}

	h := &Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: infoHash,
		PeerID:   peerID,
	}

	serialized := h.Serialize()
	if len(serialized) != 68 {
		t.Fatalf("Expected serialized handshake length 68, got %d", len(serialized))
	}

	// Verify protocol string length byte
	if serialized[0] != 19 {
		t.Errorf("Expected first byte 19, got %d", serialized[0])
	}

	// Verify protocol string itself
	pstr := string(serialized[1:20])
	if pstr != "BitTorrent protocol" {
		t.Errorf("Expected protocol string %q, got %q", "BitTorrent protocol", pstr)
	}

	parsed, err := ParseHandshake(bytes.NewReader(serialized))
	if err != nil {
		t.Fatalf("Failed to parse serialized handshake: %v", err)
	}

	if parsed.Pstr != h.Pstr {
		t.Errorf("Parsed Pstr = %q, expected %q", parsed.Pstr, h.Pstr)
	}
	if parsed.InfoHash != h.InfoHash {
		t.Errorf("Parsed InfoHash = %v, expected %v", parsed.InfoHash, h.InfoHash)
	}
	if parsed.PeerID != h.PeerID {
		t.Errorf("Parsed PeerID = %v, expected %v", parsed.PeerID, h.PeerID)
	}
}

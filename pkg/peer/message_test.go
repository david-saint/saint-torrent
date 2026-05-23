package peer

import (
	"bytes"
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

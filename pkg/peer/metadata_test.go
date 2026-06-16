package peer

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
)

func TestParseExtensionHandshake(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantErr      bool
		wantExtID    int    // expected ut_metadata ID
		wantMetaSize int    // expected metadata_size
		wantClient   string // expected v
	}{
		{
			name:         "basic ut_metadata",
			input:        "d1:md11:ut_metadatai2ee13:metadata_sizei31235ee",
			wantExtID:    2,
			wantMetaSize: 31235,
		},
		{
			name:         "with client name",
			input:        "d1:md11:ut_metadatai1ee13:metadata_sizei1024e1:v14:saintTorrent/1e",
			wantExtID:    1,
			wantMetaSize: 1024,
			wantClient:   "saintTorrent/1",
		},
		{
			name:      "empty m dict",
			input:     "d1:mdee",
			wantExtID: 0,
		},
		{
			name:    "not a dictionary",
			input:   "i42e",
			wantErr: true,
		},
		{
			name:    "m value not a dict",
			input:   "d1:mi5ee",
			wantErr: true,
		},
		{
			name:    "invalid bencode",
			input:   "garbage",
			wantErr: true,
		},
		{
			name:    "metadata size too large",
			input:   "d1:md11:ut_metadatai2ee13:metadata_sizei16777217ee",
			wantErr: true,
		},
		{
			name:    "metadata size negative",
			input:   "d1:md11:ut_metadatai2ee13:metadata_sizei-1ee",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hs, err := ParseExtensionHandshake([]byte(tt.input))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotID, ok := hs.Extensions["ut_metadata"]; ok {
				if gotID != tt.wantExtID {
					t.Errorf("ut_metadata ID = %d, want %d", gotID, tt.wantExtID)
				}
			} else if tt.wantExtID != 0 {
				t.Error("ut_metadata not found in extensions")
			}
			if hs.MetadataSize != tt.wantMetaSize {
				t.Errorf("MetadataSize = %d, want %d", hs.MetadataSize, tt.wantMetaSize)
			}
			if hs.ClientName != tt.wantClient {
				t.Errorf("ClientName = %q, want %q", hs.ClientName, tt.wantClient)
			}
		})
	}
}

func TestSerializeExtensionHandshake(t *testing.T) {
	data, err := SerializeExtensionHandshake(2, 31235)
	if err != nil {
		t.Fatalf("SerializeExtensionHandshake failed: %v", err)
	}

	// Parse it back and verify round-trip.
	hs, err := ParseExtensionHandshake(data)
	if err != nil {
		t.Fatalf("ParseExtensionHandshake failed on serialized data: %v", err)
	}
	if hs.Extensions["ut_metadata"] != 2 {
		t.Errorf("ut_metadata = %d, want 2", hs.Extensions["ut_metadata"])
	}
	if hs.MetadataSize != 31235 {
		t.Errorf("MetadataSize = %d, want 31235", hs.MetadataSize)
	}
}

func TestSerializeExtensionHandshake_NoMetadataSize(t *testing.T) {
	data, err := SerializeExtensionHandshake(3, 0)
	if err != nil {
		t.Fatalf("SerializeExtensionHandshake failed: %v", err)
	}

	hs, err := ParseExtensionHandshake(data)
	if err != nil {
		t.Fatalf("ParseExtensionHandshake failed: %v", err)
	}
	if hs.Extensions["ut_metadata"] != 3 {
		t.Errorf("ut_metadata = %d, want 3", hs.Extensions["ut_metadata"])
	}
	if hs.MetadataSize != 0 {
		t.Errorf("MetadataSize = %d, want 0", hs.MetadataSize)
	}
}

func TestSerializeExtensionHandshakeWithPEX(t *testing.T) {
	data, err := SerializeExtensionHandshakeWithExtensions(map[string]int{
		ExtNameMetadata: LocalMetadataExtID,
		ExtNamePEX:      LocalPEXExtID,
	}, 1234)
	if err != nil {
		t.Fatalf("SerializeExtensionHandshakeWithExtensions failed: %v", err)
	}

	hs, err := ParseExtensionHandshake(data)
	if err != nil {
		t.Fatalf("ParseExtensionHandshake failed: %v", err)
	}
	if hs.Extensions[ExtNameMetadata] != LocalMetadataExtID {
		t.Errorf("ut_metadata = %d, want %d", hs.Extensions[ExtNameMetadata], LocalMetadataExtID)
	}
	if hs.Extensions[ExtNamePEX] != LocalPEXExtID {
		t.Errorf("ut_pex = %d, want %d", hs.Extensions[ExtNamePEX], LocalPEXExtID)
	}
	if hs.MetadataSize != 1234 {
		t.Errorf("MetadataSize = %d, want 1234", hs.MetadataSize)
	}
}

func TestSerializeExtensionHandshakeRejectsInvalidExtensionID(t *testing.T) {
	if _, err := SerializeExtensionHandshakeWithExtensions(map[string]int{ExtNamePEX: 256}, 0); err == nil {
		t.Fatal("expected invalid extension ID to fail")
	}
}

func TestParseMetadataMessage_Request(t *testing.T) {
	// msg_type=0, piece=0
	input := "d8:msg_typei0e5:piecei0ee"
	msg, err := ParseMetadataMessage([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.MsgType != MetadataRequest {
		t.Errorf("MsgType = %d, want %d", msg.MsgType, MetadataRequest)
	}
	if msg.Piece != 0 {
		t.Errorf("Piece = %d, want 0", msg.Piece)
	}
	if msg.Data != nil {
		t.Errorf("Data should be nil for request, got %d bytes", len(msg.Data))
	}
}

func TestParseMetadataMessage_Data(t *testing.T) {
	// msg_type=1, piece=2, total_size=42, followed by raw piece data
	dict := "d8:msg_typei1e5:piecei2e10:total_sizei42ee"
	rawPiece := "hello metadata piece"
	input := dict + rawPiece

	msg, err := ParseMetadataMessage([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.MsgType != MetadataData {
		t.Errorf("MsgType = %d, want %d", msg.MsgType, MetadataData)
	}
	if msg.Piece != 2 {
		t.Errorf("Piece = %d, want 2", msg.Piece)
	}
	if !bytes.Equal(msg.Data, []byte(rawPiece)) {
		t.Errorf("Data = %q, want %q", msg.Data, rawPiece)
	}
}

func TestParseMetadataMessage_Reject(t *testing.T) {
	input := "d8:msg_typei2e5:piecei5ee"
	msg, err := ParseMetadataMessage([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.MsgType != MetadataReject {
		t.Errorf("MsgType = %d, want %d", msg.MsgType, MetadataReject)
	}
	if msg.Piece != 5 {
		t.Errorf("Piece = %d, want 5", msg.Piece)
	}
}

func TestParseMetadataMessage_DataWithNoTrailing(t *testing.T) {
	// A data message with no trailing bytes should fail.
	input := "d8:msg_typei1e5:piecei0ee"
	_, err := ParseMetadataMessage([]byte(input))
	if err == nil {
		t.Fatal("expected error for data message with no piece data")
	}
}

func TestParseMetadataMessageRejectsOversizedDataBlock(t *testing.T) {
	dict := "d8:msg_typei1e5:piecei0e10:total_sizei16777216ee"
	input := append([]byte(dict), bytes.Repeat([]byte{'x'}, MetadataBlockSize+1)...)
	_, err := ParseMetadataMessage(input)
	if err == nil {
		t.Fatal("expected oversized metadata block to fail")
	}
}

func TestParseMetadataMessageRejectsNegativePiece(t *testing.T) {
	_, err := ParseMetadataMessage([]byte("d8:msg_typei0e5:piecei-1ee"))
	if err == nil {
		t.Fatal("expected negative piece index to fail")
	}
}

func TestParseExtensionHandshakeIgnoresDisabledExtension(t *testing.T) {
	hs, err := ParseExtensionHandshake([]byte("d1:md11:ut_metadatai0eee"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := hs.Extensions["ut_metadata"]; ok {
		t.Fatal("expected ut_metadata ID 0 to be treated as disabled")
	}
}

func TestParseMetadataMessageRejectsOutOfRangePiece(t *testing.T) {
	_, err := ParseMetadataMessage([]byte("d8:msg_typei0e5:piecei1024ee"))
	if err == nil {
		t.Fatal("expected out-of-range metadata piece to fail")
	}
}

func TestParseMetadataMessageRejectsTrailingDataForRequest(t *testing.T) {
	_, err := ParseMetadataMessage([]byte("d8:msg_typei0e5:piecei0eetrailing"))
	if err == nil {
		t.Fatal("expected request with trailing data to fail")
	}
}

func TestParseMetadataMessageRejectsNegativeStringLengthSpan(t *testing.T) {
	_, err := bencodedDictSpan([]byte("d1:a-1:xe"))
	if err == nil {
		t.Fatal("expected negative string length to fail")
	}
}

func TestParseMetadataMessage_MissingFields(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"missing msg_type", "d5:piecei0ee"},
		{"missing piece", "d8:msg_typei0ee"},
		{"empty", ""},
		{"not a dict", "i42e"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseMetadataMessage([]byte(tt.input))
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestSerializeMetadataRequest(t *testing.T) {
	data, err := SerializeMetadataRequest(3)
	if err != nil {
		t.Fatalf("SerializeMetadataRequest failed: %v", err)
	}

	msg, err := ParseMetadataMessage(data)
	if err != nil {
		t.Fatalf("ParseMetadataMessage failed on serialized request: %v", err)
	}
	if msg.MsgType != MetadataRequest {
		t.Errorf("MsgType = %d, want %d", msg.MsgType, MetadataRequest)
	}
	if msg.Piece != 3 {
		t.Errorf("Piece = %d, want 3", msg.Piece)
	}
}

func TestSerializeMetadataRequest_RoundTrip(t *testing.T) {
	for _, piece := range []int{0, 1, 5, 100} {
		data, err := SerializeMetadataRequest(piece)
		if err != nil {
			t.Fatalf("piece %d: serialize error: %v", piece, err)
		}
		msg, err := ParseMetadataMessage(data)
		if err != nil {
			t.Fatalf("piece %d: parse error: %v", piece, err)
		}
		if msg.MsgType != MetadataRequest {
			t.Errorf("piece %d: MsgType = %d, want %d", piece, msg.MsgType, MetadataRequest)
		}
		if msg.Piece != piece {
			t.Errorf("piece %d: Piece = %d", piece, msg.Piece)
		}
	}
}

func TestPEXMessageRoundTrip(t *testing.T) {
	input := &PEXMessage{
		Added: []PEXPeer{
			{IP: net.ParseIP("127.0.0.1"), Port: 6881, Flags: PEXFlagSeed},
			{IP: net.ParseIP("2001:db8::1"), Port: 51413, Flags: PEXFlagUTP},
		},
		Dropped: []PEXPeer{
			{IP: net.ParseIP("192.0.2.10"), Port: 6000},
			{IP: net.ParseIP("2001:db8::2"), Port: 6001},
		},
	}

	data, err := SerializePEXMessage(input)
	if err != nil {
		t.Fatalf("SerializePEXMessage failed: %v", err)
	}
	msg, err := ParsePEXMessage(data)
	if err != nil {
		t.Fatalf("ParsePEXMessage failed: %v", err)
	}
	if len(msg.Added) != len(input.Added) {
		t.Fatalf("added count = %d, want %d", len(msg.Added), len(input.Added))
	}
	for i, got := range msg.Added {
		want := input.Added[i]
		if !got.IP.Equal(want.IP) || got.Port != want.Port || got.Flags != want.Flags {
			t.Errorf("added[%d] = %s:%d flags=%d, want %s:%d flags=%d", i, got.IP, got.Port, got.Flags, want.IP, want.Port, want.Flags)
		}
	}
	if len(msg.Dropped) != len(input.Dropped) {
		t.Fatalf("dropped count = %d, want %d", len(msg.Dropped), len(input.Dropped))
	}
	for i, got := range msg.Dropped {
		want := input.Dropped[i]
		if !got.IP.Equal(want.IP) || got.Port != want.Port {
			t.Errorf("dropped[%d] = %s:%d, want %s:%d", i, got.IP, got.Port, want.IP, want.Port)
		}
	}
}

func TestParsePEXMessageRejectsMalformedCompactPeers(t *testing.T) {
	data, err := bencode.Marshal(map[string]interface{}{
		"added": []byte{127, 0, 0, 1, 0x1a},
	})
	if err != nil {
		t.Fatalf("failed to marshal PEX: %v", err)
	}
	if _, err := ParsePEXMessage(data); err == nil {
		t.Fatal("expected malformed compact peer list to fail")
	}
}

func TestParsePEXMessageRejectsMismatchedFlags(t *testing.T) {
	data, err := bencode.Marshal(map[string]interface{}{
		"added":   []byte{127, 0, 0, 1, 0x1a, 0xe1},
		"added.f": []byte{0x00, 0x01},
	})
	if err != nil {
		t.Fatalf("failed to marshal PEX: %v", err)
	}
	if _, err := ParsePEXMessage(data); err == nil {
		t.Fatal("expected mismatched flags to fail")
	}
}

func TestParsePEXMessageRejectsTooManyPeersAcrossFields(t *testing.T) {
	data, err := bencode.Marshal(map[string]interface{}{
		"added":   compactIPv4PEXTestPeers(MaxPEXPeers),
		"dropped": compactIPv4PEXTestPeers(1),
	})
	if err != nil {
		t.Fatalf("failed to marshal PEX: %v", err)
	}
	if _, err := ParsePEXMessage(data); err == nil {
		t.Fatal("expected split-field peer overflow to fail")
	}
}

func compactIPv4PEXTestPeers(count int) []byte {
	compact := make([]byte, 0, count*6)
	for i := 0; i < count; i++ {
		compact = append(compact, 10, 0, byte(i>>8), byte(i))
		var port [2]byte
		binary.BigEndian.PutUint16(port[:], uint16(1+i%65535))
		compact = append(compact, port[:]...)
	}
	return compact
}

func TestExtensionHandshakeRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		id   int
		size int
	}{
		{1, 0},
		{2, 16384},
		{3, 31235},
		{255, 1048576},
	} {
		data, err := SerializeExtensionHandshake(tc.id, tc.size)
		if err != nil {
			t.Fatalf("id=%d size=%d: serialize error: %v", tc.id, tc.size, err)
		}
		hs, err := ParseExtensionHandshake(data)
		if err != nil {
			t.Fatalf("id=%d size=%d: parse error: %v", tc.id, tc.size, err)
		}
		if hs.Extensions["ut_metadata"] != tc.id {
			t.Errorf("ut_metadata = %d, want %d", hs.Extensions["ut_metadata"], tc.id)
		}
		if hs.MetadataSize != tc.size {
			t.Errorf("MetadataSize = %d, want %d", hs.MetadataSize, tc.size)
		}
	}
}

func TestConstants(t *testing.T) {
	if MsgExtended != 20 {
		t.Errorf("MsgExtended = %d, want 20", MsgExtended)
	}
	if ExtHandshake != 0 {
		t.Errorf("ExtHandshake = %d, want 0", ExtHandshake)
	}
	if MetadataBlockSize != 16384 {
		t.Errorf("MetadataBlockSize = %d, want 16384", MetadataBlockSize)
	}
}

func TestSendExtHandshake(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	infoHash := [20]byte{1}
	peerID := [20]byte{2}
	client := NewClient(clientConn, infoHash, peerID)

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.SendExtHandshake(2, 31235)
	}()

	// Read from server side with a deadline.
	serverConn.SetDeadline(time.Now().Add(2 * time.Second))

	msg, err := ParseMessage(serverConn)
	if err != nil {
		t.Fatalf("ParseMessage failed: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("SendExtHandshake failed: %v", err)
	}

	if msg.ID != MsgExtended {
		t.Fatalf("message ID = %d, want %d", msg.ID, MsgExtended)
	}
	if len(msg.Payload) == 0 {
		t.Fatal("empty payload")
	}
	if msg.Payload[0] != ExtHandshake {
		t.Errorf("ext sub-ID = %d, want %d", msg.Payload[0], ExtHandshake)
	}

	// Parse the bencoded handshake from the rest of the payload.
	hs, err := ParseExtensionHandshake(msg.Payload[1:])
	if err != nil {
		t.Fatalf("ParseExtensionHandshake failed: %v", err)
	}
	if hs.Extensions["ut_metadata"] != 2 {
		t.Errorf("ut_metadata = %d, want 2", hs.Extensions["ut_metadata"])
	}
	if hs.MetadataSize != 31235 {
		t.Errorf("MetadataSize = %d, want 31235", hs.MetadataSize)
	}
}

func TestSendMetadataRequest(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	infoHash := [20]byte{1}
	peerID := [20]byte{2}
	client := NewClient(clientConn, infoHash, peerID)

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.SendMetadataRequest(3, 5)
	}()

	serverConn.SetDeadline(time.Now().Add(2 * time.Second))

	msg, err := ParseMessage(serverConn)
	if err != nil {
		t.Fatalf("ParseMessage failed: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("SendMetadataRequest failed: %v", err)
	}

	if msg.ID != MsgExtended {
		t.Fatalf("message ID = %d, want %d", msg.ID, MsgExtended)
	}
	if msg.Payload[0] != 3 {
		t.Errorf("ext sub-ID = %d, want 3", msg.Payload[0])
	}

	// Parse the metadata request from the bencoded payload.
	metaMsg, err := ParseMetadataMessage(msg.Payload[1:])
	if err != nil {
		t.Fatalf("ParseMetadataMessage failed: %v", err)
	}
	if metaMsg.MsgType != MetadataRequest {
		t.Errorf("MsgType = %d, want %d", metaMsg.MsgType, MetadataRequest)
	}
	if metaMsg.Piece != 5 {
		t.Errorf("Piece = %d, want 5", metaMsg.Piece)
	}
}

func TestBencodedDictSpan(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantSpan int
		wantErr  bool
	}{
		{
			name:     "simple dict",
			input:    "d3:fooi42ee",
			wantSpan: 11,
		},
		{
			name:     "dict with trailing data",
			input:    "d3:fooi42eeextra",
			wantSpan: 11,
		},
		{
			name:     "nested dict",
			input:    "d1:ad1:bi1eee",
			wantSpan: 13,
		},
		{
			name:    "not a dict",
			input:   "i42e",
			wantErr: true,
		},
		{
			name:    "empty",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			span, err := bencodedDictSpan([]byte(tt.input))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if span != tt.wantSpan {
				t.Errorf("span = %d, want %d", span, tt.wantSpan)
			}
		})
	}
}

func TestMessageSerializationExtended(t *testing.T) {
	// Verify that MsgExtended messages round-trip through Message.Serialize / ParseMessage.
	payload := []byte{ExtHandshake, 'd', '1', ':', 'm', 'd', 'e', 'e'}
	msg := &Message{ID: MsgExtended, Payload: payload}
	serialized := msg.Serialize()

	// Verify the wire format: 4-byte length prefix, then message ID, then payload.
	length := binary.BigEndian.Uint32(serialized[0:4])
	if int(length) != 1+len(payload) {
		t.Errorf("wire length = %d, want %d", length, 1+len(payload))
	}
	if serialized[4] != byte(MsgExtended) {
		t.Errorf("wire message ID = %d, want %d", serialized[4], MsgExtended)
	}
	if !bytes.Equal(serialized[5:], payload) {
		t.Errorf("wire payload mismatch")
	}

	// Parse it back.
	parsed, err := ParseMessage(bytes.NewReader(serialized))
	if err != nil {
		t.Fatalf("ParseMessage failed: %v", err)
	}
	if parsed.ID != MsgExtended {
		t.Errorf("parsed ID = %d, want %d", parsed.ID, MsgExtended)
	}
	if !bytes.Equal(parsed.Payload, payload) {
		t.Errorf("parsed payload mismatch")
	}
}

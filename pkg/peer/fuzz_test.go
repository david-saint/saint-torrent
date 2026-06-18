package peer

import (
	"bytes"
	"testing"
)

// FuzzParseMessage feeds arbitrary framing to the wire-message parser. It must
// never panic, and a parsed message must re-serialize to a frame no larger than
// the bytes it was read from.
func FuzzParseMessage(f *testing.F) {
	seeds := [][]byte{
		{0, 0, 0, 0},                // keep-alive
		{0, 0, 0, 1, 0},             // choke
		{0, 0, 0, 5, 4, 0, 0, 0, 1}, // have
		{0, 0, 0, 13, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // request
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		msg, err := ParseMessage(bytes.NewReader(data))
		if err != nil || msg == nil {
			return
		}
		if out := msg.Serialize(); len(out) > len(data) {
			t.Fatalf("serialized message (%d bytes) larger than source frame (%d bytes)", len(out), len(data))
		}
	})
}

// FuzzParseExtensionHandshake ensures the BEP 10 handshake parser never panics
// and only accepts an in-range metadata size.
func FuzzParseExtensionHandshake(f *testing.F) {
	seeds := []string{
		"d1:md11:ut_metadatai1eee",
		"d1:md11:ut_metadatai2ee13:metadata_sizei1234ee",
		"d1:md11:ut_metadatai1ee1:v6:client e",
		"de",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		hs, err := ParseExtensionHandshake(data)
		if err != nil {
			return
		}
		if hs.MetadataSize < 0 || hs.MetadataSize > MaxMetadataSize {
			t.Fatalf("metadata size out of range: %d", hs.MetadataSize)
		}
	})
}

// FuzzParseMetadataMessage ensures the BEP 9 message parser never panics and
// enforces its msg_type and data-block invariants.
func FuzzParseMetadataMessage(f *testing.F) {
	seeds := []string{
		"d8:msg_typei0e5:piecei0ee",
		"d8:msg_typei1e5:piecei0e10:total_sizei5eeHELLO",
		"d8:msg_typei2e5:piecei3ee",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		msg, err := ParseMetadataMessage(data)
		if err != nil {
			return
		}
		if msg.MsgType < MetadataRequest || msg.MsgType > MetadataReject {
			t.Fatalf("invalid msg_type: %d", msg.MsgType)
		}
		if msg.MsgType == MetadataData && len(msg.Data) == 0 {
			t.Fatalf("data message with empty piece data")
		}
		if len(msg.Data) > MetadataBlockSize {
			t.Fatalf("data block too large: %d", len(msg.Data))
		}
	})
}

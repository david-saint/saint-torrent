package peer

import (
	"bytes"
	"net"
	"testing"
)

// TestExtensionParsersRejectOversizedPayloads checks the pre-decode size caps: a
// payload past the cap is refused before bencode builds a tree for it, while the
// largest legitimate messages (a full ut_metadata data block, a MaxPEXPeers IPv6
// PEX message) still fit.
func TestExtensionParsersRejectOversizedPayloads(t *testing.T) {
	flat := func(n int) []byte { return append(append([]byte{'d'}, bytes.Repeat([]byte("0:le"), n)...), 'e') }
	if _, err := ParseExtensionHandshake(flat(MaxExtHandshakeSize / 4)); err == nil {
		t.Fatal("oversized extension handshake was decoded")
	}
	if _, err := ParsePEXMessage(flat(MaxPEXMessageSize / 4)); err == nil {
		t.Fatal("oversized PEX message was decoded")
	}
	if _, err := ParseMetadataMessage(flat(MaxMetadataMessageSize / 4)); err == nil {
		t.Fatal("oversized metadata message was decoded")
	}

	dict := "d8:msg_typei1e5:piecei0e10:total_sizei16777216ee"
	block := append([]byte(dict), bytes.Repeat([]byte{'x'}, MetadataBlockSize)...)
	if _, err := ParseMetadataMessage(block); err != nil {
		t.Fatalf("full metadata data block rejected: %v", err)
	}

	peers := make([]PEXPeer, MaxPEXPeers)
	for i := range peers {
		ip := make(net.IP, net.IPv6len)
		ip[0], ip[14], ip[15] = 0x20, byte(i>>8), byte(i)
		peers[i] = PEXPeer{IP: ip, Port: uint16(1 + i), Flags: PEXFlagSeed}
	}
	full, err := SerializePEXMessage(&PEXMessage{Added: peers})
	if err != nil {
		t.Fatalf("serialize PEX: %v", err)
	}
	if len(full) > MaxPEXMessageSize {
		t.Fatalf("largest legitimate PEX message is %d bytes, over the %d cap", len(full), MaxPEXMessageSize)
	}
	if _, err := ParsePEXMessage(full); err != nil {
		t.Fatalf("largest legitimate PEX message rejected: %v", err)
	}
}

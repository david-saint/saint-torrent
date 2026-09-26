package peer

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// TestExtensionHandshakeRequestQueue checks BEP 10 reqq round-trips and that a
// malformed value is ignored rather than failing the handshake.
func TestExtensionHandshakeRequestQueue(t *testing.T) {
	data, err := (&ExtensionHandshake{
		Extensions:   map[string]int{ExtNameMetadata: 3},
		MetadataSize: 100,
		ClientName:   "saintTorrent",
		RequestQueue: 512,
	}).Serialize()
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	hs, err := ParseExtensionHandshake(data)
	if err != nil {
		t.Fatalf("ParseExtensionHandshake: %v", err)
	}
	if hs.RequestQueue != 512 || hs.Extensions[ExtNameMetadata] != 3 || hs.MetadataSize != 100 || hs.ClientName != "saintTorrent" {
		t.Fatalf("round trip = %+v", hs)
	}

	for _, input := range []string{"d1:mde4:reqq3:abce", "d1:mde4:reqqi-5ee", "d1:mde4:reqqi0ee"} {
		hs, err := ParseExtensionHandshake([]byte(input))
		if err != nil {
			t.Fatalf("%q: %v", input, err)
		}
		if hs.RequestQueue != 0 {
			t.Fatalf("%q: RequestQueue = %d, want 0", input, hs.RequestQueue)
		}
	}
}

// TestSendMetadataDataWireFormat checks the directly framed ut_metadata data
// message parses back to the same piece, total size and block.
func TestSendMetadataDataWireFormat(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	client := NewClient(clientConn, [20]byte{1}, [20]byte{2})

	block := bytes.Repeat([]byte{0xab}, MetadataBlockSize)
	errCh := make(chan error, 1)
	go func() { errCh <- client.SendMetadataData(7, 3, 5*MetadataBlockSize, block) }()

	_ = serverConn.SetDeadline(time.Now().Add(2 * time.Second))
	msg, err := ParseMessage(serverConn)
	if err != nil {
		t.Fatalf("ParseMessage: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("SendMetadataData: %v", err)
	}
	if msg.ID != MsgExtended || msg.Payload[0] != 7 {
		t.Fatalf("got message %d on extended id %d, want %d on 7", msg.ID, msg.Payload[0], MsgExtended)
	}
	m, err := ParseMetadataMessage(msg.Payload[1:])
	if err != nil {
		t.Fatalf("ParseMetadataMessage: %v", err)
	}
	if m.MsgType != MetadataData || m.Piece != 3 || m.TotalSize != 5*MetadataBlockSize || !bytes.Equal(m.Data, block) {
		t.Fatalf("round trip mismatch: type=%d piece=%d total=%d len=%d", m.MsgType, m.Piece, m.TotalSize, len(m.Data))
	}
}

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

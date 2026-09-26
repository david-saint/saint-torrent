package peer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"sainttorrent/pkg/torrent"
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
		got, gerr := readMessage(bufio.NewReader(bytes.NewReader(in)), make([]byte, 4), defaultBitfieldLimit)
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
// into a pooled buffer and that Release returns the buffer to the pool.
func TestReadMessagePoolsBlockBuffers(t *testing.T) {
	block := make([]byte, 16384)
	for i := range block {
		block[i] = byte(i)
	}
	in := pieceWire(1, 0, block) // length == maxPooledMessageLen, poolable

	msg, err := readMessage(bufio.NewReader(bytes.NewReader(in)), make([]byte, 4), defaultBitfieldLimit)
	if err != nil {
		t.Fatalf("readMessage failed: %v", err)
	}
	if msg.pooled == nil {
		t.Fatal("expected a pooled backing buffer for a block-sized message")
	}
	if !bytes.Equal(msg.Payload[8:], block) {
		t.Fatal("payload block does not round-trip")
	}

	msg.Release()
	if msg.pooled != nil || msg.Payload != nil {
		t.Fatal("Release must clear the pooled buffer and payload")
	}
	// sync.Pool gives no identity guarantee (GC or the race runtime may reshuffle
	// slots), so assert on shape like the short-read test: whatever Get hands out
	// next must be a full-size pooled buffer.
	if reused := inboundBufPool.Get().(*[]byte); cap(*reused) != maxPooledMessageLen {
		t.Fatalf("pool returned a buffer of cap %d, want %d", cap(*reused), maxPooledMessageLen)
	} else {
		inboundBufPool.Put(reused)
	}

	// Release is safe to call again (and on a heap-backed message / nil receiver).
	msg.Release()
	var nilMsg *Message
	nilMsg.Release()
}

// TestReadMessageOversizedNotPooled checks messages larger than the pooled buffer
// size fall back to a heap allocation with Release as a no-op. Only a bitfield
// or an extended message may be that large.
func TestReadMessageOversizedNotPooled(t *testing.T) {
	bitfield := bytes.Repeat([]byte{0xff}, 2*maxPooledMessageLen)
	in := (&Message{ID: MsgBitfield, Payload: bitfield}).Serialize()
	msg, err := readMessage(bufio.NewReader(bytes.NewReader(in)), make([]byte, 4), defaultBitfieldLimit)
	if err != nil {
		t.Fatalf("readMessage failed: %v", err)
	}
	if msg.pooled != nil {
		t.Fatal("oversized message must not borrow a pooled buffer")
	}
	if msg.ID != MsgBitfield || !bytes.Equal(msg.Payload, bitfield) {
		t.Fatalf("unexpected message: id %d, payload length %d", msg.ID, len(msg.Payload))
	}
	msg.Release() // no-op, must not panic
}

// TestReadMessageReleaseOnShortReadReclaimsBuffer ensures a truncated payload
// returns its borrowed buffer to the pool rather than leaking it.
func TestReadMessageReleaseOnShortReadReclaimsBuffer(t *testing.T) {
	// Advertise a poolable length but supply fewer payload bytes than promised.
	in := []byte{0, 0, 0, 10, 7, 1, 2, 3} // length 10, only 3 payload bytes follow
	if _, err := readMessage(bufio.NewReader(bytes.NewReader(in)), make([]byte, 4), defaultBitfieldLimit); err == nil {
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

// frameA1 returns a frame whose length prefix says length and whose body is the id
// followed by length-1 payload bytes (a keep-alive for length 0).
func frameA1(id MessageID, length uint32) []byte {
	buf := make([]byte, 4+int(length))
	binary.BigEndian.PutUint32(buf, length)
	if length > 0 {
		buf[4] = byte(id)
	}
	return buf
}

func readFrameA1(frame []byte, bitfieldLimit int) (*Message, error) {
	return readMessage(bufio.NewReader(bytes.NewReader(frame)), make([]byte, 4), bitfieldLimit)
}

// TestReadMessageFixedSizeLengths checks every fixed-size message: its exact
// length is read, one byte more or less fails with ErrInvalidMessageLength (for a
// one-byte message, one less is a keep-alive).
func TestReadMessageFixedSizeLengths(t *testing.T) {
	cases := []struct {
		id     MessageID
		length uint32
	}{
		{MsgChoke, 1}, {MsgUnchoke, 1}, {MsgInterested, 1}, {MsgNotInterested, 1},
		{MsgHaveAll, 1}, {MsgHaveNone, 1},
		{MsgHave, 5}, {MsgSuggestPiece, 5}, {MsgAllowedFast, 5},
		{MsgRequest, 13}, {MsgCancel, 13}, {MsgRejectRequest, 13},
		{MsgPort, 3},
	}
	for _, tc := range cases {
		msg, err := readFrameA1(frameA1(tc.id, tc.length), defaultBitfieldLimit)
		if err != nil || msg == nil || msg.ID != tc.id || len(msg.Payload) != int(tc.length)-1 {
			t.Fatalf("id %d length %d: got %v, %v; want the message", tc.id, tc.length, msg, err)
		}
		msg.Release()
		for _, bad := range []uint32{tc.length - 1, tc.length + 1} {
			if bad == 0 {
				continue // a keep-alive
			}
			if _, err := readFrameA1(frameA1(tc.id, bad), defaultBitfieldLimit); !errors.Is(err, ErrInvalidMessageLength) {
				t.Fatalf("id %d length %d: err = %v, want ErrInvalidMessageLength", tc.id, bad, err)
			}
		}
	}
}

// A piece carries an index, a begin offset and at most one 16 KiB block.
func TestReadMessagePieceLengths(t *testing.T) {
	for _, length := range []uint32{8, 9 + maxBlockLength + 1} {
		if _, err := readFrameA1(frameA1(MsgPiece, length), defaultBitfieldLimit); !errors.Is(err, ErrInvalidMessageLength) {
			t.Fatalf("piece length %d: err = %v, want ErrInvalidMessageLength", length, err)
		}
	}
	for _, length := range []uint32{9, 9 + maxBlockLength} {
		msg, err := readFrameA1(frameA1(MsgPiece, length), defaultBitfieldLimit)
		if err != nil || msg == nil {
			t.Fatalf("piece length %d: %v", length, err)
		}
		msg.Release()
	}
	if 9+maxBlockLength != maxPooledMessageLen {
		t.Fatalf("largest piece (%d) does not fill the pooled buffer (%d)", 9+maxBlockLength, maxPooledMessageLen)
	}
}

// touchFailReaderA1 fails the test if the reader ever asks it for bytes.
type touchFailReaderA1 struct{ t *testing.T }

func (r touchFailReaderA1) Read([]byte) (int, error) {
	r.t.Error("the payload of a rejected message was read")
	return 0, io.EOF
}

// An unknown id may not be larger than a pooled buffer, and the rejection comes
// from the header alone: no payload byte is read or allocated for.
func TestReadMessageRejectsLargeUnknownIDBeforeReading(t *testing.T) {
	header := frameA1(30, maxPooledMessageLen+1)[:5] // prefix + id only
	r := bufio.NewReader(io.MultiReader(bytes.NewReader(header), touchFailReaderA1{t}))
	if _, err := readMessage(r, make([]byte, 4), defaultBitfieldLimit); !errors.Is(err, ErrInvalidMessageLength) {
		t.Fatalf("err = %v, want ErrInvalidMessageLength", err)
	}

	msg, err := readFrameA1(frameA1(30, maxPooledMessageLen), defaultBitfieldLimit)
	if err != nil || msg == nil {
		t.Fatalf("unknown id at the pooled size: %v", err)
	}
	msg.Release()
}

// An extended message holds at most the largest BEP 10 payload we decode.
func TestReadMessageExtendedLengths(t *testing.T) {
	msg, err := readFrameA1(frameA1(MsgExtended, 2+MaxExtHandshakeSize), defaultBitfieldLimit)
	if err != nil || msg == nil || len(msg.Payload) != 1+MaxExtHandshakeSize {
		t.Fatalf("largest extended message: %v, %v", msg, err)
	}
	for _, length := range []uint32{1, 2 + MaxExtHandshakeSize + 1} {
		if _, err := readFrameA1(frameA1(MsgExtended, length), defaultBitfieldLimit); !errors.Is(err, ErrInvalidMessageLength) {
			t.Fatalf("extended length %d: err = %v, want ErrInvalidMessageLength", length, err)
		}
	}
}

// pipeClientA1 returns a Client reading from one end of a pipe and the other end.
func pipeClientA1(t *testing.T) (*Client, net.Conn) {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() {
		_ = local.Close()
		_ = remote.Close()
	})
	return NewClient(local, [20]byte{}, [20]byte{}), remote
}

// readBitfieldA1 sends a bitfield of n bytes and returns what the client reads.
func readBitfieldA1(t *testing.T, c *Client, remote net.Conn, n int) (*Message, error) {
	t.Helper()
	frame := (&Message{ID: MsgBitfield, Payload: make([]byte, n)}).Serialize()
	go func() {
		_ = remote.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, _ = remote.Write(frame)
	}()
	return c.ReadMessage()
}

// The bitfield limit defaults to the largest bitfield a magnet can need, and
// SetBitfieldLimit sizes it to the torrent, clamped to what any torrent can need.
func TestClientBitfieldLimit(t *testing.T) {
	if defaultBitfieldLimit != 104858 {
		t.Fatalf("defaultBitfieldLimit = %d, want 104858", defaultBitfieldLimit)
	}
	c, remote := pipeClientA1(t)
	if msg, err := readBitfieldA1(t, c, remote, defaultBitfieldLimit); err != nil || len(msg.Payload) != defaultBitfieldLimit {
		t.Fatalf("default-limit bitfield: %v", err)
	}
	c, remote = pipeClientA1(t)
	if _, err := readBitfieldA1(t, c, remote, defaultBitfieldLimit+1); !errors.Is(err, ErrInvalidMessageLength) {
		t.Fatalf("bitfield over the default limit: err = %v, want ErrInvalidMessageLength", err)
	}

	c, remote = pipeClientA1(t)
	c.SetBitfieldLimit(torrent.MaxPieceCount) // raise to the largest torrent
	if msg, err := readBitfieldA1(t, c, remote, maxBitfieldBytes); err != nil || len(msg.Payload) != maxBitfieldBytes {
		t.Fatalf("largest bitfield after raising the limit: %v", err)
	}

	c, remote = pipeClientA1(t)
	c.SetBitfieldLimit(9) // two bytes
	if msg, err := readBitfieldA1(t, c, remote, 2); err != nil {
		t.Fatalf("exact bitfield: %v", err)
	} else {
		msg.Release()
	}
	c, remote = pipeClientA1(t)
	c.SetBitfieldLimit(9)
	if _, err := readBitfieldA1(t, c, remote, 3); !errors.Is(err, ErrInvalidMessageLength) {
		t.Fatalf("bitfield over a lowered limit: err = %v, want ErrInvalidMessageLength", err)
	}

	for _, tc := range []struct{ numPieces, want int }{
		{-5, 1}, {0, 1}, {1, 1}, {8, 1}, {9, 2},
		{torrent.MaxPieceCount, maxBitfieldBytes},
		{torrent.MaxPieceCount + 1, maxBitfieldBytes},
		{int(^uint(0) >> 1), maxBitfieldBytes},
	} {
		c.SetBitfieldLimit(tc.numPieces)
		if got := int(c.bitfieldLimit.Load()); got != tc.want {
			t.Fatalf("SetBitfieldLimit(%d) = %d, want %d", tc.numPieces, got, tc.want)
		}
	}
}

// MaxMessageLength is exactly the bitfield of the largest torrent we accept.
func TestMaxMessageLengthCoversLargestBitfield(t *testing.T) {
	if want := 1 + (torrent.MaxPieceCount+7)/8; MaxMessageLength != want {
		t.Fatalf("MaxMessageLength = %d, want %d", MaxMessageLength, want)
	}
	if _, err := readFrameA1(frameA1(MsgBitfield, MaxMessageLength+1), maxBitfieldBytes); !errors.Is(err, ErrInvalidMessageLength) {
		t.Fatalf("err = %v, want ErrInvalidMessageLength", err)
	}
}

// A rejected large header costs no payload allocation: a length over
// MaxMessageLength, and an id whose type does not allow a large length.
func TestRejectedLargeHeaderAllocatesNoPayload(t *testing.T) {
	for _, header := range [][]byte{
		frameA1(MsgPiece, 1<<20)[:5],
		frameA1(30, MaxMessageLength)[:5],
		frameA1(MsgHave, MaxMessageLength)[:5],
	} {
		src := bytes.NewReader(header)
		r := bufio.NewReader(src)
		lengthBuf := make([]byte, 4)
		read := func() {
			src.Reset(header)
			r.Reset(src)
			if _, err := readMessage(r, lengthBuf, defaultBitfieldLimit); !errors.Is(err, ErrInvalidMessageLength) {
				t.Fatalf("err = %v, want ErrInvalidMessageLength", err)
			}
		}
		// Only the wrapped error is allocated.
		if allocs := testing.AllocsPerRun(100, read); allocs > 4 {
			t.Fatalf("rejecting a large header made %.0f allocations", allocs)
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		const runs = 50
		for i := 0; i < runs; i++ {
			read()
		}
		runtime.ReadMemStats(&after)
		if perRun := (after.TotalAlloc - before.TotalAlloc) / runs; perRun > 4096 {
			t.Fatalf("rejecting a large header allocated %d bytes per read", perRun)
		}
	}
}

// BenchmarkReadMessagePiece reads full-block piece messages through the pooled
// path, the inbound hot path.
func BenchmarkReadMessagePiece(b *testing.B) {
	frame := pieceWire(1, 0, bytes.Repeat([]byte{7}, maxBlockLength))
	src := bytes.NewReader(frame)
	r := bufio.NewReaderSize(src, 64*1024)
	lengthBuf := make([]byte, 4)
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		src.Reset(frame)
		r.Reset(src)
		msg, err := readMessage(r, lengthBuf, defaultBitfieldLimit)
		if err != nil {
			b.Fatal(err)
		}
		msg.Release()
	}
}

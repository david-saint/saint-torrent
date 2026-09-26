// Package peer implements the BitTorrent peer wire protocol.
package peer

import (
	"bufio"
	"encoding/binary"
	"net"
	"sync"
	"time"
)

// peerReadBufferSize is the size of the per-connection read buffer.
// 64 KiB allows several inbound messages (e.g. piece blocks) to be drained in one syscall.
const peerReadBufferSize = 64 * 1024

// peerWriteBufferSize is the size of the per-connection write buffer.
// 32 KiB is sized to hold a raised dynamic-pipeline request burst
// (1024 requests * 17 bytes = 17 KiB) in a single write syscall with headroom.
const peerWriteBufferSize = 32 * 1024

// peerWriteTimeout bounds how long a write to a peer may go without the socket
// taking the bytes. A peer that stops reading (a zero TCP window, a uTP endpoint
// that vanished) would otherwise block the connection's goroutine in a write
// forever, holding its connection slot. Any peer that reads at all drains one
// buffered write (at most 32 KiB) long before this.
const peerWriteTimeout = 90 * time.Second

// Client represents a connection to a BitTorrent peer.
type Client struct {
	Conn        net.Conn
	InfoHash    [20]byte
	PeerID      [20]byte
	r           *bufio.Reader // buffers inbound framing so reads coalesce syscalls
	writeMu     sync.Mutex    // protects concurrent writes to w (and reqBuf/pieceBuf)
	w           *bufio.Writer // buffers outbound messages; flushed explicitly
	reqBuf      [17]byte      // reusable scratch for WriteRequest framing
	pieceHdrBuf [13]byte      // reusable scratch for SendPiece header framing
	readLenBuf  [4]byte       // reusable 4-byte length-prefix scratch for ReadMessage
	DisableDHT  bool          // Disable advertising DHT support in handshake

	// writeTimeout and writeDeadline implement the write timeout (see
	// peerWriteTimeout); writeDeadline is the deadline last set on Conn. Both are
	// guarded by writeMu.
	writeTimeout  time.Duration
	writeDeadline time.Time
}

// NewClient initializes a new peer wire client.
func NewClient(conn net.Conn, infoHash, peerID [20]byte) *Client {
	return &Client{
		Conn:     conn,
		InfoHash: infoHash,
		PeerID:   peerID,
		r:        bufio.NewReaderSize(conn, peerReadBufferSize),
		w:        bufio.NewWriterSize(conn, peerWriteBufferSize),

		writeTimeout: peerWriteTimeout,
	}
}

// SetWriteTimeout changes how long a write may make no progress before it fails
// and the connection is closed (peerWriteTimeout by default). Call it before the
// connection is in use.
func (c *Client) SetWriteTimeout(d time.Duration) {
	c.writeMu.Lock()
	c.writeTimeout = d
	c.writeDeadline = time.Time{}
	c.writeMu.Unlock()
}

// armWriteDeadlineLocked ensures a write deadline at least half a timeout away
// before a write that may reach the socket. Moving it only once half the window
// has passed costs one deadline update per connection every ~45 s instead of one
// per block. Caller holds writeMu.
func (c *Client) armWriteDeadlineLocked() {
	now := time.Now()
	if c.writeDeadline.Sub(now) >= c.writeTimeout/2 {
		return
	}
	c.writeDeadline = now.Add(c.writeTimeout)
	_ = c.Conn.SetWriteDeadline(c.writeDeadline)
}

// writeFailedLocked closes the connection after a failed write. bufio.Writer keeps
// the error, so the client cannot send again; closing it also ends the reader, so
// the connection is torn down even when the caller ignores the send error.
// Caller holds writeMu.
func (c *Client) writeFailedLocked(err error) error {
	if err != nil {
		_ = c.Conn.Close()
	}
	return err
}

// Handshake performs the BitTorrent protocol handshake.
// It writes our handshake, then reads and parses the peer's handshake response.
// The caller bounds the exchange with a deadline on Conn.
func (c *Client) Handshake() (*Handshake, error) {
	reqHandshake := &Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: c.InfoHash,
		PeerID:   c.PeerID,
	}
	reqHandshake.Reserved[5] = 0x10 // Support extension protocol (BEP 10)
	if !c.DisableDHT {
		reqHandshake.Reserved[7] |= 0x01 // Support DHT (BEP 5)
	}
	EnableFastExtension(&reqHandshake.Reserved)

	// The handshake is bounded by the deadline the caller sets for the whole
	// exchange and clears afterwards, so forget any armed write deadline: the
	// first write after the handshake arms a fresh one.
	c.writeMu.Lock()
	c.writeDeadline = time.Time{}
	_, err := c.w.Write(reqHandshake.Serialize())
	if err == nil {
		err = c.w.Flush()
	}
	c.writeMu.Unlock()
	if err != nil {
		return nil, err
	}

	// Read the response through the buffered reader: if the peer pipelines its
	// first messages in the same segment as the handshake, those bytes stay
	// buffered for the message loop rather than being lost.
	return ParseHandshake(c.r)
}

// SendMessage serializes and writes a message to the peer connection in a
// thread-safe manner, flushing immediately so control messages are not delayed.
func (c *Client) SendMessage(msg *Message) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.armWriteDeadlineLocked()
	if _, err := c.w.Write(msg.Serialize()); err != nil {
		return c.writeFailedLocked(err)
	}
	return c.writeFailedLocked(c.w.Flush())
}

// WriteRequest queues a block request into the write buffer without flushing.
// The request pump batches a burst of these and then calls Flush once, so a
// window fill becomes a single write syscall instead of one per request. The
// framing is built in a reused per-client scratch buffer (guarded by writeMu),
// so unlike the SendMessage path it allocates nothing per request.
// Safe for concurrent use with other senders.
func (c *Client) WriteRequest(index, begin, length uint32) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	buf := &c.reqBuf // 4-byte length prefix + 1-byte ID + 12-byte payload
	binary.BigEndian.PutUint32(buf[0:4], 13)
	buf[4] = byte(MsgRequest)
	binary.BigEndian.PutUint32(buf[5:9], index)
	binary.BigEndian.PutUint32(buf[9:13], begin)
	binary.BigEndian.PutUint32(buf[13:17], length)
	// Only a write that overflows the buffer reaches the socket; the rest of a
	// burst skips the deadline check.
	if c.w.Available() < len(buf) {
		c.armWriteDeadlineLocked()
	}
	_, err := c.w.Write(buf[:])
	return c.writeFailedLocked(err)
}

// Flush writes any buffered outbound messages (e.g. a batch of queued block
// requests) to the underlying connection.
func (c *Client) Flush() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// The request pump flushes after every message; an empty buffer writes
	// nothing, so it needs no deadline.
	if c.w.Buffered() > 0 {
		c.armWriteDeadlineLocked()
	}
	return c.writeFailedLocked(c.w.Flush())
}

// SendKeepAlive sends a keep-alive message (zero-length prefix).
func (c *Client) SendKeepAlive() error {
	return c.SendMessage(nil)
}

// SendChoke sends a choke message to the peer.
func (c *Client) SendChoke() error {
	return c.SendMessage(&Message{ID: MsgChoke})
}

// SendUnchoke sends an unchoke message to the peer.
func (c *Client) SendUnchoke() error {
	return c.SendMessage(&Message{ID: MsgUnchoke})
}

// SendInterested sends an interested message to the peer.
func (c *Client) SendInterested() error {
	return c.SendMessage(&Message{ID: MsgInterested})
}

// SendNotInterested sends a not-interested message to the peer.
func (c *Client) SendNotInterested() error {
	return c.SendMessage(&Message{ID: MsgNotInterested})
}

// SendHave sends a have message notifying the peer that we downloaded a piece.
func (c *Client) SendHave(index uint32) error {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, index)
	return c.SendMessage(&Message{ID: MsgHave, Payload: payload})
}

// SendBitfield sends our bitfield representation of possessed pieces to the peer.
func (c *Client) SendBitfield(bitfield []byte) error {
	return c.SendMessage(&Message{ID: MsgBitfield, Payload: bitfield})
}

// SendHaveAll tells a fast-extension peer that we have every piece.
func (c *Client) SendHaveAll() error {
	return c.SendMessage(&Message{ID: MsgHaveAll})
}

// SendHaveNone tells a fast-extension peer that we have no pieces.
func (c *Client) SendHaveNone() error {
	return c.SendMessage(&Message{ID: MsgHaveNone})
}

// SendPiece sends a piece block message to the peer. Like WriteRequest it frames
// the fixed header (4-byte length prefix + id + index + begin) into a reused
// per-client scratch buffer under writeMu, then streams the caller's block
// straight into the bufio writer. This avoids the payload copy and the
// Message.Serialize copy the SendMessage path would incur, so serving a block
// allocates nothing here — the dominant cost on the seed hot path.
func (c *Client) SendPiece(index, begin uint32, block []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	buf := &c.pieceHdrBuf // 4-byte length prefix + 1-byte ID + 8-byte index/begin
	length := uint32(9 + len(block))
	binary.BigEndian.PutUint32(buf[0:4], length)
	buf[4] = byte(MsgPiece)
	binary.BigEndian.PutUint32(buf[5:9], index)
	binary.BigEndian.PutUint32(buf[9:13], begin)
	c.armWriteDeadlineLocked()
	if _, err := c.w.Write(buf[:]); err != nil {
		return c.writeFailedLocked(err)
	}
	if _, err := c.w.Write(block); err != nil {
		return c.writeFailedLocked(err)
	}
	return c.writeFailedLocked(c.w.Flush())
}

// SendPort sends a PORT message (id 9, BEP 5) advertising our DHT UDP port so a
// DHT-capable peer can add us to its routing table.
func (c *Client) SendPort(port uint16) error {
	payload := make([]byte, 2)
	binary.BigEndian.PutUint16(payload, port)
	return c.SendMessage(&Message{ID: MsgPort, Payload: payload})
}

// SendCancel sends a cancel message to withdraw a request for a block.
func (c *Client) SendCancel(index, begin, length uint32) error {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	binary.BigEndian.PutUint32(payload[8:12], length)
	return c.SendMessage(&Message{ID: MsgCancel, Payload: payload})
}

// SendRejectRequest tells a fast-extension peer that a request will not be served.
func (c *Client) SendRejectRequest(index, begin, length uint32) error {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	binary.BigEndian.PutUint32(payload[8:12], length)
	return c.SendMessage(&Message{ID: MsgRejectRequest, Payload: payload})
}

// SendAllowedFast grants the peer permission to request a piece while choked.
func (c *Client) SendAllowedFast(index uint32) error {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, index)
	return c.SendMessage(&Message{ID: MsgAllowedFast, Payload: payload})
}

// ReadMessage reads a message from the peer connection through the buffered
// reader, so the length prefix and payload are typically served from a single
// underlying read.
func (c *Client) ReadMessage() (*Message, error) {
	// Single dedicated read goroutine per client, so the length-prefix scratch is
	// unshared. The payload is read into a pooled buffer that the caller returns
	// via Message.Release once it is done with the message.
	return readMessage(c.r, c.readLenBuf[:])
}

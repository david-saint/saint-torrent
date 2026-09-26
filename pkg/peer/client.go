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
	haveBuf     [9]byte       // reusable scratch for SendQueuedHaves framing
	readLenBuf  [4]byte       // reusable 4-byte length-prefix scratch for ReadMessage
	DisableDHT  bool          // Disable advertising DHT support in handshake

	// RemotePeerID is the peer's ID from its handshake: set by Handshake, or by
	// the caller for an incoming connection whose handshake it parsed itself.
	RemotePeerID [20]byte

	// writeTimeout and writeDeadline implement the write timeout (see
	// peerWriteTimeout); writeDeadline is the deadline last set on Conn. Both are
	// guarded by writeMu.
	writeTimeout  time.Duration
	writeDeadline time.Time

	// Haves queued by other goroutines for the goroutine that owns the connection
	// to send (see QueueHave). queueMu guards queuedHaves.
	queueMu     sync.Mutex
	queuedHaves []uint32
	notify      chan struct{} // capacity 1; see Notified
}

// maxReusedHaveQueue bounds the Have queue capacity a client keeps for reuse
// after a drain, so one large burst (a recheck) does not pin its array.
const maxReusedHaveQueue = 1024

// NewClient initializes a new peer wire client.
func NewClient(conn net.Conn, infoHash, peerID [20]byte) *Client {
	return &Client{
		Conn:     conn,
		InfoHash: infoHash,
		PeerID:   peerID,
		r:        bufio.NewReaderSize(conn, peerReadBufferSize),
		w:        bufio.NewWriterSize(conn, peerWriteBufferSize),

		writeTimeout: peerWriteTimeout,
		notify:       make(chan struct{}, 1),
	}
}

// SetWriteTimeout changes how long a write may make no progress before it fails
// and the connection is closed (peerWriteTimeout by default); zero or less turns
// the timeout off. Call it before the connection is in use.
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
	if c.writeTimeout <= 0 {
		return
	}
	// time.Until reads only the monotonic clock for a deadline that carries a
	// monotonic reading, which makes this per-block check about half the cost of
	// a time.Now.
	if time.Until(c.writeDeadline) >= c.writeTimeout/2 {
		return
	}
	c.writeDeadline = time.Now().Add(c.writeTimeout)
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
	hs, err := ParseHandshake(c.r)
	if err != nil {
		return nil, err
	}
	c.RemotePeerID = hs.PeerID
	return hs, nil
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

// QueueHave queues a Have for piece index and wakes the goroutine that owns the
// connection through Notified; that goroutine sends it with SendQueuedHaves. It
// never blocks and never writes: completing a piece costs one append per
// connected peer instead of a goroutine and a socket write, and a peer that
// reads slowly holds up nobody but its own connection.
func (c *Client) QueueHave(index uint32) {
	c.queueMu.Lock()
	c.queuedHaves = append(c.queuedHaves, index)
	c.queueMu.Unlock()
	c.Notify()
}

// Notify wakes the goroutine that owns the connection through Notified without
// queueing anything, for state it reads itself (such as whether we choke the
// peer). It never blocks.
func (c *Client) Notify() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// Notified receives after QueueHave or Notify. Wakeups coalesce: one receive
// covers everything queued before it.
func (c *Client) Notified() <-chan struct{} {
	return c.notify
}

// takeQueuedHaves removes and returns the queued Haves.
func (c *Client) takeQueuedHaves() []uint32 {
	c.queueMu.Lock()
	haves := c.queuedHaves
	c.queuedHaves = nil
	c.queueMu.Unlock()
	return haves
}

// recycleHaveQueue hands a drained queue back for reuse.
func (c *Client) recycleHaveQueue(haves []uint32) {
	if cap(haves) > maxReusedHaveQueue {
		return
	}
	c.queueMu.Lock()
	if c.queuedHaves == nil {
		c.queuedHaves = haves[:0]
	}
	c.queueMu.Unlock()
}

// SendQueuedHaves sends every queued Have with one flush.
func (c *Client) SendQueuedHaves() error {
	haves := c.takeQueuedHaves()
	if len(haves) == 0 {
		return nil
	}
	c.writeMu.Lock()
	buf := &c.haveBuf // 4-byte length prefix + 1-byte ID + 4-byte index
	binary.BigEndian.PutUint32(buf[0:4], 5)
	buf[4] = byte(MsgHave)
	var err error
	for _, index := range haves {
		binary.BigEndian.PutUint32(buf[5:9], index)
		// A long queue fills the buffer several times; each of those writes
		// reaches the socket.
		if c.w.Available() < len(buf) {
			c.armWriteDeadlineLocked()
		}
		if _, err = c.w.Write(buf[:]); err != nil {
			break
		}
	}
	if err == nil {
		c.armWriteDeadlineLocked()
		err = c.w.Flush()
	}
	err = c.writeFailedLocked(err)
	c.writeMu.Unlock()
	c.recycleHaveQueue(haves)
	return err
}

// DropQueuedHaves discards the queued Haves, for an owner that has not sent its
// bitfield yet: the bitfield it sends later covers them.
func (c *Client) DropQueuedHaves() {
	c.recycleHaveQueue(c.takeQueuedHaves())
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

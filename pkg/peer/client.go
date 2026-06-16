// Package peer implements the BitTorrent peer wire protocol.
package peer

import (
	"bufio"
	"encoding/binary"
	"net"
	"sync"
)

// peerReadBufferSize is the size of the per-connection read buffer.
// 64 KiB allows several inbound messages (e.g. piece blocks) to be drained in one syscall.
const peerReadBufferSize = 64 * 1024

// peerWriteBufferSize is the size of the per-connection write buffer.
// 8 KiB is sized to comfortably hold a full burst of pipelined block requests
// (up to 256 requests * 17 bytes = 4.25 KiB) in a single write syscall without overflowing.
const peerWriteBufferSize = 8 * 1024

// Client represents a connection to a BitTorrent peer.
type Client struct {
	Conn     net.Conn
	InfoHash [20]byte
	PeerID   [20]byte
	r        *bufio.Reader // buffers inbound framing so reads coalesce syscalls
	writeMu  sync.Mutex    // protects concurrent writes to w (and reqBuf)
	w        *bufio.Writer // buffers outbound messages; flushed explicitly
	reqBuf   [17]byte      // reusable scratch for WriteRequest framing
}

// NewClient initializes a new peer wire client.
func NewClient(conn net.Conn, infoHash, peerID [20]byte) *Client {
	return &Client{
		Conn:     conn,
		InfoHash: infoHash,
		PeerID:   peerID,
		r:        bufio.NewReaderSize(conn, peerReadBufferSize),
		w:        bufio.NewWriterSize(conn, peerWriteBufferSize),
	}
}

// Handshake performs the BitTorrent protocol handshake.
// It writes our handshake, then reads and parses the peer's handshake response.
func (c *Client) Handshake() (*Handshake, error) {
	reqHandshake := &Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: c.InfoHash,
		PeerID:   c.PeerID,
	}
	reqHandshake.Reserved[5] = 0x10 // Support extension protocol (BEP 10)

	c.writeMu.Lock()
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
	if _, err := c.w.Write(msg.Serialize()); err != nil {
		return err
	}
	return c.w.Flush()
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
	_, err := c.w.Write(buf[:])
	return err
}

// Flush writes any buffered outbound messages (e.g. a batch of queued block
// requests) to the underlying connection.
func (c *Client) Flush() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.w.Flush()
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

// SendPiece sends a piece block message to the peer.
func (c *Client) SendPiece(index, begin uint32, block []byte) error {
	payload := make([]byte, 8+len(block))
	binary.BigEndian.PutUint32(payload[0:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	copy(payload[8:], block)
	return c.SendMessage(&Message{ID: MsgPiece, Payload: payload})
}

// SendCancel sends a cancel message to withdraw a request for a block.
func (c *Client) SendCancel(index, begin, length uint32) error {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	binary.BigEndian.PutUint32(payload[8:12], length)
	return c.SendMessage(&Message{ID: MsgCancel, Payload: payload})
}

// ReadMessage reads a message from the peer connection through the buffered
// reader, so the length prefix and payload are typically served from a single
// underlying read.
func (c *Client) ReadMessage() (*Message, error) {
	return ParseMessage(c.r)
}
